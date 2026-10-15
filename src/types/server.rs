use std::collections::{HashMap, HashSet};
use std::sync::{Arc, OnceLock};

use arc_swap::ArcSwapOption;
use async_trait::async_trait;
use claviron::auth::UserProvider;
use claviron::auth::orbit::TokenAuthenticator;
use claviron::db::DbService;
use claviron::orbit::FleetEventRegistry;
use claviron::orbit::placement::{Scope, Scoped};
use claviron::runtime::events::ProcessEventBus;
use claviron::runtime::registry::{
    Bootable, Component, Error as RegistryError, Reloadable, Runnable,
};
use claviron::runtime::{AppState, Register, RunContext};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use tokio::sync::{RwLock, broadcast};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::config::{ChatConfig, ChatConfigProvider};
use crate::event::{ChatDeliveryEvent, ChatPresenceEvent};
use crate::models::messages::MessageStore;
use crate::models::receipts::ReceiptStore;
#[cfg(feature = "quic")]
use crate::models::users::by_identifier;
use crate::models::users::{ChatUser, by_local_id, by_subject, hydrate_users};
use crate::presence::PresenceRegistry;
use crate::svc::rate_limit::ChatRateLimiter;
use crate::types::messages::{
    HistoryMessage, MessageStatusType, OnlineUserInfo, ReadReceipt, RoomInfo,
    ServerMessage, UnreadPrivateDetail, UnreadRoomCount, UnreadSummary,
    UserInfo,
};
use crate::types::room::ChatRoom;
use crate::{AvatarUrlProvider, ChatClient, SessionAuthenticator};

/// Global svc server state.
///
/// Lifecycle: `Register` constructs an `empty()` instance with its
/// `ChatConfigProvider` and the process event bus in hand — only in-memory
/// state (clients, rooms) besides. `Bootable::boot()` derives the rate
/// limiter from the applied configuration and, when chat is enabled, takes
/// the identity, auth and database handles from the registry; nothing is
/// opened or spawned there. `Runnable::run`, scoped to workers, starts the
/// fanout and presence listeners and holds them until shutdown. Config file
/// I/O lives entirely in `ChatConfigProvider` — this struct never reads
/// `chat.yaml` directly.
#[derive(Clone)]
pub struct ChatServer {
    /// Connected clients (connection_id -> client)
    clients: Arc<RwLock<HashMap<Uuid, ChatClient>>>,

    /// User connections mapping (user_id -> set of connection_ids)
    user_connections: Arc<RwLock<HashMap<i32, HashSet<Uuid>>>>,

    /// Active rooms (room_name -> room)
    rooms: Arc<RwLock<HashMap<String, ChatRoom>>>,

    /// Message storage (PostgreSQL). Set once at `Bootable::boot()`.
    message_store: Arc<OnceLock<MessageStore>>,

    /// Message receipt storage (PostgreSQL). Set once at `Bootable::boot()`.
    receipt_store: Arc<OnceLock<ReceiptStore>>,

    /// Cached `ChatConfigProvider` — the single source of truth for
    /// `chat.yaml`. Set once at `Bootable::boot()`; reads thereafter
    /// are lock-free `ArcSwap` loads via the provider's `current()`.
    config_provider: Arc<OnceLock<Arc<ChatConfigProvider>>>,

    /// Rate limiter for spam prevention. Derived from the provider's
    /// snapshot at boot/reload — kept here because it's a runtime
    /// object (not a config slice). `None` until `Bootable::boot()`
    /// runs; `check_rate_limit` fails open in that window.
    rate_limiter: Arc<ArcSwapOption<ChatRateLimiter>>,

    /// Postgres pool. Set once at `Bootable::boot()` from `DbService`.
    db_pool: Arc<OnceLock<sqlx::PgPool>>,

    /// Process-local event bus used as the chat fanout ingress. The
    /// `claviron-orbit` bridge forwards these events across workers.
    events: Arc<OnceLock<ProcessEventBus>>,

    /// Application-owned identity lookup bound through the core auth contract.
    users: Arc<OnceLock<Arc<dyn UserProvider>>>,
    auth: Arc<OnceLock<Arc<dyn TokenAuthenticator>>>,
    session_auth: Arc<OnceLock<Arc<dyn SessionAuthenticator>>>,

    /// Optional application-owned avatar URL projection.
    avatars: Arc<OnceLock<Option<Arc<dyn AvatarUrlProvider>>>>,

    /// Fleet-replicated remote presence. Local transport handles remain in
    /// `clients`, `user_connections`, and `rooms` above.
    presence: PresenceRegistry,
}

impl ChatServer {
    /// Empty constructor — no I/O, no registry resolves, no fake config
    /// seeding. `Register` hands it the config provider and the event bus;
    /// `Bootable::boot()` fills the identity and database slots.
    pub fn empty() -> Self {
        Self {
            clients: Arc::default(),
            user_connections: Arc::default(),
            rooms: Arc::default(),
            message_store: Arc::default(),
            receipt_store: Arc::default(),
            config_provider: Arc::default(),
            rate_limiter: Arc::default(),
            db_pool: Arc::default(),
            events: Arc::default(),
            users: Arc::default(),
            auth: Arc::default(),
            session_auth: Arc::default(),
            avatars: Arc::default(),
            presence: PresenceRegistry::new(),
        }
    }

    /// Returns the postgres pool. Panics if called before `Bootable::boot()`.
    pub fn pool(&self) -> &sqlx::PgPool {
        self.db_pool
            .get()
            .expect("ChatServer::pool() called before Bootable::boot() ran")
    }

    fn users(&self) -> &dyn UserProvider {
        self.users
            .get()
            .expect("ChatServer::users() called before Bootable::boot() ran")
            .as_ref()
    }

    pub(crate) async fn authenticate_token(
        &self,
        token: &str,
        audience: &str,
    ) -> Result<crate::types::client::ChatAuthentication, crate::error::AuthError>
    {
        let auth =
            self.auth.get().ok_or("Orbit token authenticator not bound")?;
        let now = crate::types::client::auth_now()
            .ok_or("system clock unavailable")?;
        let authenticated = auth.authenticate(token, now).await?;
        if authenticated.session.principal().claims().audience.as_str()
            != audience
        {
            return Err("invalid chat audience".into());
        }
        let user =
            ChatUser::from_authenticated(authenticated.user, self.avatars())
                .await?;
        // User/avatar resolution may have yielded while another worker logged out.
        authenticated.session.check(
            crate::types::client::auth_now()
                .ok_or("system clock unavailable")?,
        )?;
        Ok(crate::types::client::ChatAuthentication {
            user,
            session: Some(authenticated.session),
        })
    }

    pub(crate) async fn authenticate_session(
        &self,
        vhost: &str,
        headers: &http::HeaderMap,
    ) -> Result<crate::types::client::ChatAuthentication, http::StatusCode>
    {
        let authenticator = self
            .session_auth
            .get()
            .ok_or(http::StatusCode::SERVICE_UNAVAILABLE)?;
        let authenticated =
            authenticator.authenticate_headers(vhost, headers).await?;
        if authenticated.session.principal().session_id().is_none() {
            return Err(http::StatusCode::UNAUTHORIZED);
        }
        let user = ChatUser::from_authenticated(
            authenticated.user,
            self.avatars(),
        )
        .await
        .map_err(|error| {
            tracing::error!(%error, "chat session identity hydration failed");
            http::StatusCode::SERVICE_UNAVAILABLE
        })?;
        let now = crate::types::client::auth_now()
            .ok_or(http::StatusCode::SERVICE_UNAVAILABLE)?;
        authenticated
            .session
            .check(now)
            .map_err(|_| http::StatusCode::UNAUTHORIZED)?;
        Ok(crate::types::client::ChatAuthentication {
            user,
            session: Some(authenticated.session),
        })
    }

    fn avatars(&self) -> Option<&dyn AvatarUrlProvider> {
        self.avatars.get().and_then(Option::as_ref).map(Arc::as_ref)
    }

    pub(crate) async fn user_by_subject(
        &self,
        subject: &str,
    ) -> Result<ChatUser, String> {
        by_subject(self.users(), self.avatars(), subject)
            .await?
            .ok_or_else(|| "user not found".to_string())
    }

    #[cfg(feature = "quic")]
    pub(crate) async fn user_by_identifier(
        &self,
        identifier: &str,
    ) -> Result<ChatUser, String> {
        by_identifier(self.users(), self.avatars(), identifier)
            .await?
            .ok_or_else(|| "user not found".to_string())
    }

    pub(crate) async fn user_id_by_uuid(
        &self,
        uuid: &Uuid,
    ) -> Result<i32, String> {
        self.user_by_subject(&uuid.to_string()).await.map(|user| user.id())
    }

    async fn hydrate_users(
        &self,
        ids: &[i32],
    ) -> Result<Vec<ChatUser>, String> {
        hydrate_users(self.users(), self.avatars(), ids).await
    }

    /// Cached `ChatConfigProvider` reference. Panics if called before
    /// `Bootable::boot()` (which is when the OnceLock is populated).
    fn config_provider(&self) -> &ChatConfigProvider {
        self.config_provider.get().expect(
            "ChatServer::config_provider() called before Bootable::boot() ran",
        )
    }

    /// Current chat config — reads through the registered
    /// `ChatConfigProvider` so reloads are observed atomically.
    pub fn config_snapshot(&self) -> Arc<ChatConfig> {
        self.config_provider().current()
    }

    /// Checks whether the given connection is allowed to send a message *right now*.
    ///
    /// This uses `ArcSwap::load()` to take a cheap, lock-free snapshot of the current
    /// [`ChatRateLimiter`] and performs the check through that snapshot.
    ///
    /// Important:
    /// - The returned guard is **not** stored anywhere; it lives only for the duration
    ///   of this function call.
    /// - If `reload()` swaps the limiter concurrently, this call will still use a
    ///   consistent snapshot (either the old or the new limiter), which is exactly
    ///   what we want.
    ///
    /// Returns:
    /// - `Ok(())` if the connection may proceed.
    /// - `Err(retry_after_ms)` if the connection is rate limited.
    pub fn check_rate_limit(
        &self,
        connection_id: Uuid,
    ) -> Result<(), u64> {
        // `None` only happens before `Bootable::boot()` runs — the
        // listener doesn't bind in that window, so a request reaching
        // here without a limiter is an invariant break, not an
        // operator state. Fail open with `Ok(())` rather than panic.
        match self.rate_limiter.load_full() {
            Some(limiter) => limiter.check(connection_id),
            None => Ok(()),
        }
    }

    fn emit_delivery(
        &self,
        event: ChatDeliveryEvent,
    ) {
        match self.events.get() {
            Some(bus) => bus.emit(event),
            None => warn!(
                "chat delivery event dropped before ChatServer boot completed"
            ),
        }
    }

    fn emit_presence(
        &self,
        event: ChatPresenceEvent,
    ) {
        match self.events.get() {
            Some(bus) => bus.emit(event),
            None => warn!(
                "chat presence event dropped before ChatServer boot completed"
            ),
        }
    }

    fn start_delivery_listener(
        &self,
        state: AppState,
    ) {
        let mut rx = state.events().subscribe::<ChatDeliveryEvent>();
        let server = self.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = state.on_shutdown() => break,
                    event = rx.recv() => match event {
                        Ok(event) => server.fanout_delivery_event(event).await,
                        Err(broadcast::error::RecvError::Lagged(skipped)) => {
                            warn!(skipped, "chat delivery listener lagged");
                        }
                        Err(broadcast::error::RecvError::Closed) => break
                    }
                }
            }
        });
    }

    fn start_presence_listener(
        &self,
        state: AppState,
    ) {
        let mut rx = state.events().subscribe::<ChatPresenceEvent>();
        let presence = self.presence.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = state.on_shutdown() => break,
                    event = rx.recv() => match event {
                        Ok(event) => presence.apply(event).await,
                        Err(broadcast::error::RecvError::Lagged(skipped)) => {
                            warn!(skipped, "chat presence listener lagged");
                        }
                        Err(broadcast::error::RecvError::Closed) => break
                    }
                }
            }
        });
    }

    fn start_presence_refresh(
        &self,
        state: AppState,
    ) {
        let server = self.clone();
        tokio::spawn(async move {
            loop {
                let interval = server
                    .config_snapshot()
                    .presence
                    .sync_interval
                    .max(std::time::Duration::from_millis(100));
                let deadline = tokio::time::Instant::now() + interval;
                server.emit_all_presence(interval).await;
                tokio::select! {
                    _ = state.on_shutdown() => break,
                    _ = tokio::time::sleep_until(deadline) => {}
                }
            }
        });
    }

    async fn emit_all_presence(
        &self,
        interval: std::time::Duration,
    ) {
        let instance_id = self.presence.instance_id();
        let mut events = self
            .user_connections
            .read()
            .await
            .iter()
            .map(|(user_id, connections)| ChatPresenceEvent::User {
                instance_id,
                revision: self.presence.next_revision(),
                user_id: *user_id,
                connections: connections.len().min(u32::MAX as usize) as u32,
            })
            .collect::<Vec<_>>();

        let room_events = {
            let rooms = self.rooms.read().await;
            let mut room_users = HashMap::<(String, i32), (usize, i64)>::new();
            for (room_name, room) in rooms.iter() {
                for client in room.get_members() {
                    let record = room_users
                        .entry((room_name.clone(), client.user_id()))
                        .or_insert((0, room.created_at.unix_timestamp()));
                    record.0 += 1;
                }
            }
            room_users
                .into_iter()
                .map(|((room, user_id), (connections, created_at_unix))| {
                    ChatPresenceEvent::Room {
                        instance_id,
                        revision: self.presence.next_revision(),
                        user_id,
                        room,
                        connections: connections.min(u32::MAX as usize) as u32,
                        created_at_unix,
                    }
                })
                .collect::<Vec<_>>()
        };
        events.extend(room_events);

        let spacing = u32::try_from(events.len())
            .ok()
            .and_then(|count| interval.checked_div(count))
            .unwrap_or(interval)
            .max(std::time::Duration::from_millis(2));
        for event in events {
            self.emit_presence(event);
            tokio::time::sleep(spacing).await;
        }
    }

    async fn emit_user_presence(
        &self,
        user_id: i32,
    ) {
        let connections = self
            .user_connections
            .read()
            .await
            .get(&user_id)
            .map(HashSet::len)
            .unwrap_or(0);
        self.emit_presence(ChatPresenceEvent::User {
            instance_id: self.presence.instance_id(),
            revision: self.presence.next_revision(),
            user_id,
            connections: connections.min(u32::MAX as usize) as u32,
        });
    }

    async fn emit_room_presence(
        &self,
        user_id: i32,
        room_name: &str,
    ) {
        let (connections, created_at_unix) = {
            let rooms = self.rooms.read().await;
            match rooms.get(room_name) {
                Some(room) => (
                    room.get_members()
                        .iter()
                        .filter(|client| client.user_id() == user_id)
                        .count(),
                    room.created_at.unix_timestamp(),
                ),
                None => (0, OffsetDateTime::now_utc().unix_timestamp()),
            }
        };
        self.emit_presence(ChatPresenceEvent::Room {
            instance_id: self.presence.instance_id(),
            revision: self.presence.next_revision(),
            user_id,
            room: room_name.to_string(),
            connections: connections.min(u32::MAX as usize) as u32,
            created_at_unix,
        });
    }

    async fn fanout_delivery_event(
        &self,
        event: ChatDeliveryEvent,
    ) {
        match event {
            ChatDeliveryEvent::RoomMessage { uuid } => {
                self.fanout_room_message(uuid).await
            }
            ChatDeliveryEvent::DirectMessage { uuid, to_user_id } => {
                self.fanout_direct_message(uuid, to_user_id).await;
            }
            ChatDeliveryEvent::UserJoined { room, user_id } => {
                self.fanout_user_joined(room, user_id).await;
            }
            ChatDeliveryEvent::UserLeft { room, user_id } => {
                self.fanout_user_left(room, user_id).await;
            }
            ChatDeliveryEvent::UserOnline { user_id } => {
                self.fanout_user_online(user_id).await
            }
            ChatDeliveryEvent::UserOffline { user_id } => {
                self.fanout_user_offline(user_id).await
            }
            ChatDeliveryEvent::UserTyping { room, user_id } => {
                self.fanout_user_typing(room, user_id).await;
            }
            ChatDeliveryEvent::UserTypingDirect {
                from_user_id,
                to_user_id,
            } => {
                self.fanout_user_typing_direct(from_user_id, to_user_id).await;
            }
            ChatDeliveryEvent::MessageStatus {
                uuid,
                sender_user_id,
                status,
                room_message,
            } => {
                self.fanout_message_status(
                    uuid,
                    sender_user_id,
                    status,
                    room_message,
                )
                .await;
            }
        }
    }

    async fn hydrate_user(
        &self,
        user_id: i32,
    ) -> Option<ChatUser> {
        by_local_id(self.users(), self.avatars(), user_id).await.ok().flatten()
    }

    async fn fanout_room_message(
        &self,
        uuid: Uuid,
    ) {
        let Some(store) = self.message_store.get() else {
            warn!(%uuid, "chat room delivery skipped: message store unavailable");
            return;
        };
        let message = match store.get_room_message_by_uuid(&uuid).await {
            Ok(Some(message)) => message,
            Ok(None) => {
                warn!(%uuid, "chat room delivery skipped: message not found");
                return;
            }
            Err(err) => {
                warn!(%uuid, error = %err, "chat room delivery skipped: database read failed");
                return;
            }
        };
        let Some(sender) = self.hydrate_user(message.from_user_id).await else {
            warn!(%uuid, sender_user_id = message.from_user_id, "chat room delivery skipped: sender not found");
            return;
        };

        let msg = ServerMessage::RoomMessage {
            uuid,
            room: message.room.clone(),
            from_user_uuid: sender.uuid(),
            from_username: sender.username_arc().clone(),
            from_avatar_url: sender.avatar_arc().clone(),
            content: message.content,
            timestamp: message.created_at,
        };
        self.broadcast_to_room_local(&message.room, msg).await;
    }

    async fn fanout_direct_message(
        &self,
        uuid: Uuid,
        to_user_id: i32,
    ) {
        let Some(store) = self.message_store.get() else {
            warn!(%uuid, "chat direct delivery skipped: message store unavailable");
            return;
        };
        let message = match store.get_private_message_by_uuid(&uuid).await {
            Ok(Some(message)) => message,
            Ok(None) => {
                warn!(%uuid, "chat direct delivery skipped: message not found");
                return;
            }
            Err(err) => {
                warn!(%uuid, error = %err, "chat direct delivery skipped: database read failed");
                return;
            }
        };
        let Some(sender) = self.hydrate_user(message.from_user_id).await else {
            warn!(%uuid, sender_user_id = message.from_user_id, "chat direct delivery skipped: sender not found");
            return;
        };

        let msg = ServerMessage::DirectMessage {
            uuid,
            from_user_uuid: sender.uuid(),
            from_username: sender.username_arc().clone(),
            from_avatar_url: sender.avatar_arc().clone(),
            content: message.content,
            timestamp: message.created_at,
        };
        self.send_to_user_local(to_user_id, msg).await;
    }

    async fn fanout_user_joined(
        &self,
        room: String,
        user_id: i32,
    ) {
        let Some(user) = self.hydrate_user(user_id).await else {
            return;
        };
        self.broadcast_to_room_local(
            &room,
            ServerMessage::UserJoined {
                room: room.clone(),
                user_uuid: user.uuid(),
                username: user.username_arc().clone(),
            },
        )
        .await;
    }

    async fn fanout_user_left(
        &self,
        room: String,
        user_id: i32,
    ) {
        let Some(user) = self.hydrate_user(user_id).await else {
            return;
        };
        self.broadcast_to_room_local(
            &room,
            ServerMessage::UserLeft {
                room: room.clone(),
                user_uuid: user.uuid(),
                username: user.username_arc().clone(),
            },
        )
        .await;
    }

    async fn fanout_user_online(
        &self,
        user_id: i32,
    ) {
        let Some(user) = self.hydrate_user(user_id).await else {
            return;
        };
        self.broadcast_to_all_except_user(
            user_id,
            ServerMessage::UserOnline {
                user_uuid: user.uuid(),
                username: user.username_arc().clone(),
            },
        )
        .await;
    }

    async fn fanout_user_offline(
        &self,
        user_id: i32,
    ) {
        let Some(user) = self.hydrate_user(user_id).await else {
            return;
        };
        self.broadcast_to_all_except_user(
            user_id,
            ServerMessage::UserOffline {
                user_uuid: user.uuid(),
                username: user.username_arc().clone(),
            },
        )
        .await;
    }

    async fn fanout_user_typing(
        &self,
        room: String,
        user_id: i32,
    ) {
        let Some(user) = self.hydrate_user(user_id).await else {
            return;
        };
        self.try_broadcast_to_room_local(
            &room,
            ServerMessage::UserTyping {
                room: room.clone(),
                user_uuid: user.uuid(),
                username: user.username_arc().clone(),
            },
        )
        .await;
    }

    async fn fanout_user_typing_direct(
        &self,
        from_user_id: i32,
        to_user_id: i32,
    ) {
        let Some(from_user) = self.hydrate_user(from_user_id).await else {
            return;
        };
        let msg = ServerMessage::UserTypingDirect {
            from_user_uuid: from_user.uuid(),
            from_username: from_user.username_arc().clone(),
        };
        self.send_to_user_local(to_user_id, msg.clone()).await;
        self.send_to_user_local(from_user_id, msg).await;
    }

    async fn fanout_message_status(
        &self,
        uuid: Uuid,
        sender_user_id: i32,
        status: MessageStatusType,
        room_message: bool,
    ) {
        let msg = if room_message {
            let Some(receipt_store) = self.receipt_store.get() else {
                return;
            };
            let total_recipients =
                receipt_store.get_total_recipients(&uuid).await.unwrap_or(0)
                    as usize;
            if matches!(status, MessageStatusType::Read) {
                let reader_ids =
                    receipt_store.get_readers(&uuid).await.unwrap_or_default();
                let mut read_by = Vec::new();
                if !reader_ids.is_empty() {
                    let reader_users = self
                        .hydrate_users(&reader_ids)
                        .await
                        .unwrap_or_default();
                    for reader_user in reader_users {
                        read_by.push(ReadReceipt {
                            user_uuid: reader_user.uuid(),
                            username: reader_user.username().to_string(),
                        });
                    }
                }
                ServerMessage::message_status_with_receipts(
                    uuid,
                    status,
                    read_by,
                    total_recipients,
                )
            } else {
                ServerMessage::message_status_with_receipts(
                    uuid,
                    status,
                    vec![],
                    total_recipients,
                )
            }
        } else {
            ServerMessage::message_status(uuid, status)
        };
        self.send_to_user_local(sender_user_id, msg).await;
    }

    /// Register a new client connection (supports multi-device)
    pub async fn connect_client(
        &self,
        client: ChatClient,
    ) {
        let user_id = client.user_id();
        let username = client.user().username();
        let connection_id = client.connection_id();

        // Add to clients map
        {
            let mut clients = self.clients.write().await;
            clients.insert(connection_id, client.clone());
        }

        // Add to user_connections mapping
        {
            let mut user_connections = self.user_connections.write().await;
            user_connections
                .entry(user_id)
                .or_insert_with(HashSet::new)
                .insert(connection_id);
        }

        let total_connections = self.clients.read().await.len();
        let user_connection_count = self
            .user_connections
            .read()
            .await
            .get(&user_id)
            .map(|s| s.len())
            .unwrap_or(0);

        debug!(
            username = %username,
            user_connections = user_connection_count,
            total_connections = total_connections,
            "🔹 Client connected (multi-device)"
        );

        let remote_online = if user_connection_count == 1 {
            self.presence
                .has_remote_user(
                    user_id,
                    self.config_snapshot().presence.stale_after,
                )
                .await
        } else {
            false
        };
        self.emit_user_presence(user_id).await;

        // Announce only the fleet's first known connection.
        if user_connection_count == 1 && !remote_online {
            self.emit_delivery(ChatDeliveryEvent::UserOnline { user_id });

            info!(
                username = %username,
                user_uuid = %client.user_uuid(),
                "🟢 User online (first connection)"
            );
        } else {
            info!(
                username = %username,
                connection_count = user_connection_count,
                "🔗 Additional connection (user already online)"
            );
        }
    }

    /// Disconnect a client by connection_id (multi-device aware)
    pub async fn disconnect_client(
        &self,
        connection_id: Uuid,
    ) {
        // First, get client info before removing
        let (user_id, username, joined_rooms) = {
            let clients = self.clients.read().await;
            match clients.get(&connection_id) {
                Some(client) => {
                    let user_id = client.user_id();
                    let username = client.username_arc();
                    let rooms = client.get_rooms().await;
                    (user_id, username, rooms)
                }
                None => {
                    warn!("Attempted to disconnect unknown connection");
                    return;
                }
            }
        };

        // Remove from clients map
        {
            let mut clients = self.clients.write().await;
            clients.remove(&connection_id);
        }

        // Remove from user_connections mapping
        let user_remaining_connections = {
            let mut user_connections = self.user_connections.write().await;
            if let Some(conn_set) = user_connections.get_mut(&user_id) {
                conn_set.remove(&connection_id);
                let remaining = conn_set.len();

                // If user has no more connections, remove from mapping and rooms
                if remaining == 0 {
                    user_connections.remove(&user_id);
                }
                remaining
            } else {
                0
            }
        };

        // Remove this specific connection from rooms and send UserLeft if needed
        for room_name in &joined_rooms {
            let mut rooms = self.rooms.write().await;
            if let Some(room) = rooms.get_mut(room_name) {
                room.remove_member_by_connection(connection_id);

                // Check if user still has other connections in this room
                let user_still_in_room =
                    room.get_members().iter().any(|c| c.user_id() == user_id);

                drop(rooms); // Release lock before presence/fanout work
                self.emit_room_presence(user_id, room_name).await;
                let user_present_remotely = self
                    .presence
                    .has_remote_room_user(
                        room_name,
                        user_id,
                        self.config_snapshot().presence.stale_after,
                    )
                    .await;

                // If user has NO more connections in this room anywhere, send UserLeft.
                if !user_still_in_room && !user_present_remotely {
                    self.emit_delivery(ChatDeliveryEvent::UserLeft {
                        room: room_name.clone(),
                        user_id,
                    });

                    info!(
                        username = %username,
                        room = %room_name,
                        "👋 User left room (no more connections in this room)"
                    );
                } else {
                    info!(
                        username = %username,
                        room = %room_name,
                        "🔗 Connection removed but user still in room (other connections exist)"
                    );
                }

                // Re-acquire the local registry to remove an empty local room.
                let mut rooms = self.rooms.write().await;
                if let Some(room) = rooms.get(room_name)
                    && room.is_empty()
                {
                    rooms.remove(room_name);
                    info!(room = %room_name, "Room removed (empty)");
                }
            }
        }

        self.emit_user_presence(user_id).await;
        let user_present_remotely = if user_remaining_connections == 0 {
            self.presence
                .has_remote_user(
                    user_id,
                    self.config_snapshot().presence.stale_after,
                )
                .await
        } else {
            false
        };

        // Send global offline only after the fleet's last known connection closes.
        if user_remaining_connections == 0 && !user_present_remotely {
            self.emit_delivery(ChatDeliveryEvent::UserOffline { user_id });

            info!(
                username = %username,
                connection_id = %connection_id,
                "🔴 User went offline (all connections closed)"
            );
        } else {
            info!(
                username = %username,
                connection_id = %connection_id,
                remaining = user_remaining_connections,
                "Connection closed (user still has other connections)"
            );
        }
    }

    /// Get all connections for a user
    pub async fn get_user_clients(
        &self,
        user_id: i32,
    ) -> Vec<ChatClient> {
        let user_connections = self.user_connections.read().await;
        let conn_ids = match user_connections.get(&user_id) {
            Some(ids) => ids.clone(),
            None => return Vec::new(),
        };

        let clients = self.clients.read().await;
        conn_ids
            .iter()
            .filter_map(|conn_id| clients.get(conn_id).cloned())
            .collect()
    }

    /// Get any one client for a user (for backward compatibility)
    pub async fn get_client(
        &self,
        user_id: i32,
    ) -> Option<ChatClient> {
        self.get_user_clients(user_id).await.into_iter().next()
    }

    /// Join a room (by connection_id for multi-device support)
    pub async fn join_room(
        &self,
        connection_id: Uuid,
        room_name: &str,
    ) -> Result<(), String> {
        // Get client by connection_id
        let client = {
            let clients = self.clients.read().await;
            clients.get(&connection_id).cloned().ok_or("Client not found")?
        };

        // Check if already in room (this connection)
        if client.is_in_room(room_name).await {
            return Err("Already in room".to_string());
        }

        let user_id = client.user_id();
        let mut rooms = self.rooms.write().await;

        // Create room if it doesn't exist
        let room = rooms
            .entry(room_name.to_string())
            .or_insert_with(|| ChatRoom::new(room_name.to_string()));

        // Check if user already has OTHER connections in this room (multi-device)
        let user_already_in_room =
            room.get_members().iter().any(|c| c.user_id() == user_id);

        // Add this connection to room
        if !room.add_member(client.clone()) {
            return Err("Failed to join room".to_string());
        }

        // Mark client as in room
        client.join_room(room_name.to_string()).await;

        drop(rooms); // Release room lock before broadcasting

        let user_present_remotely = if !user_already_in_room {
            self.presence
                .has_remote_room_user(
                    room_name,
                    user_id,
                    self.config_snapshot().presence.stale_after,
                )
                .await
        } else {
            false
        };
        self.emit_room_presence(user_id, room_name).await;

        // Broadcast only for the fleet's first connection in the room.
        if !user_already_in_room && !user_present_remotely {
            self.emit_delivery(ChatDeliveryEvent::UserJoined {
                room: room_name.to_string(),
                user_id,
            });

            debug!(
                username = %client.username(),
                room = %room_name,
                "👋 User joined room (first connection)"
            );
        } else {
            // User already in room with other connections
            // Send UserJoined ONLY to this new connection (confirmation)
            let join_msg = ServerMessage::UserJoined {
                room: room_name.to_string(),
                user_uuid: client.user_uuid(),
                username: client.username_arc().clone(),
            };
            let _ = client.send(join_msg).await;

            debug!(
                username = %client.username(),
                room = %room_name,
                "🔗 Additional connection joined room (user already present, confirmation sent)"
            );
        }

        Ok(())
    }

    /// Leave a room (by connection_id for multi-device support)
    pub async fn leave_room(
        &self,
        connection_id: Uuid,
        room_name: &str,
    ) -> Result<(), String> {
        let client = {
            let clients = self.clients.read().await;
            clients.get(&connection_id).cloned().ok_or("Client not found")?
        };

        let user_id = client.user_id();
        let mut rooms = self.rooms.write().await;

        let room = rooms.get_mut(room_name).ok_or("Room not found")?;

        // Remove this connection from room
        room.remove_member_by_connection(connection_id)
            .ok_or("Connection not in room")?;

        // Check if user still has OTHER connections in this room (multi-device)
        let user_still_in_room =
            room.get_members().iter().any(|c| c.user_id() == user_id);

        // Remove empty rooms before releasing lock
        let is_empty = room.is_empty();
        if is_empty {
            rooms.remove(room_name);
            info!(room = %room_name, "Room removed (empty)");
        }

        drop(rooms); // Release room lock before broadcasting

        // Update client state
        client.leave_room(room_name).await;

        self.emit_room_presence(user_id, room_name).await;
        let user_present_remotely = if !user_still_in_room {
            self.presence
                .has_remote_room_user(
                    room_name,
                    user_id,
                    self.config_snapshot().presence.stale_after,
                )
                .await
        } else {
            false
        };

        // Broadcast only after the fleet's last connection leaves the room.
        if !user_still_in_room && !user_present_remotely {
            self.emit_delivery(ChatDeliveryEvent::UserLeft {
                room: room_name.to_string(),
                user_id,
            });

            debug!(
                username = %client.username(),
                room = %room_name,
                "👋 User left room (last connection)"
            );
        } else {
            // User still in room with other connections
            // Send UserLeft ONLY to this connection (confirmation)
            let leave_msg = ServerMessage::UserLeft {
                room: room_name.to_string(),
                user_uuid: client.user_uuid(),
                username: client.username_arc().clone(),
            };
            let _ = client.send(leave_msg).await;

            debug!(
                username = %client.username(),
                room = %room_name,
                "🔗 Connection left room (user still present, confirmation sent)"
            );
        }

        Ok(())
    }

    /// Send message to a room (multi-device: sends to ALL connections including sender's)
    pub async fn send_to_room(
        &self,
        user_id: i32,
        room_name: &str,
        uuid: Uuid,
        content: String,
    ) -> Result<(), String> {
        let client =
            self.get_client(user_id).await.ok_or("Client not found")?;

        if !client.is_in_room(room_name).await {
            return Err("Not in room".to_string());
        }

        // Save to database
        if let Some(store) = self.message_store.get() {
            if let Err(e) = store
                .save_room_message(&uuid, room_name, user_id, &content)
                .await
            {
                error!(error = %e, room = %room_name, "Failed to save room message to database");
                return Err("Failed to save room message".to_string());
            } else {
                debug!(room = %room_name, "Room message saved to database");
            }
        }

        let rooms = self.rooms.read().await;
        let room = rooms.get(room_name).ok_or("Room not found")?;

        debug!(
            room = %room_name,
            username = %client.username_arc(),
            member_count = room.member_count(),
            "📨 Broadcasting room message to ALL (client filters own UUID)"
        );

        self.emit_delivery(ChatDeliveryEvent::RoomMessage { uuid });

        Ok(())
    }

    /// Send direct message to another user (multi-device: sends to all recipient connections)
    pub async fn send_direct(
        &self,
        from_user_id: i32,
        to_user_id: i32,
        uuid: Uuid,
        content: String,
    ) -> Result<(), String> {
        let _from_client =
            self.get_client(from_user_id).await.ok_or("Sender not found")?;

        // Save to database as private message
        if let Some(store) = self.message_store.get() {
            if let Err(e) = store
                .save_private_message(&uuid, from_user_id, to_user_id, &content)
                .await
            {
                error!(error = %e, dm_to = to_user_id, "Failed to save private message to database");
                return Err("Failed to save private message".to_string());
            } else {
                debug!(dm_to = to_user_id, "Private message saved to database");
            }
        }

        self.emit_delivery(ChatDeliveryEvent::DirectMessage {
            uuid,
            to_user_id,
        });

        Ok(())
    }

    /// Send typing indicator to a room (multi-device: sends to ALL room members including sender's other tabs)
    pub async fn send_typing(
        &self,
        user_id: i32,
        room_name: &str,
    ) -> Result<(), String> {
        let client =
            self.get_client(user_id).await.ok_or("Client not found")?;

        if !client.is_in_room(room_name).await {
            return Err("Not in room".to_string());
        }

        self.emit_delivery(ChatDeliveryEvent::UserTyping {
            room: room_name.to_string(),
            user_id,
        });

        Ok(())
    }

    /// Send typing indicator to another user (multi-device: sends to recipient AND sender's other tabs)
    pub async fn send_typing_direct(
        &self,
        from_user_id: i32,
        to_user_id: i32,
    ) -> Result<(), String> {
        let _from_client =
            self.get_client(from_user_id).await.ok_or("Sender not found")?;

        self.emit_delivery(ChatDeliveryEvent::UserTypingDirect {
            from_user_id,
            to_user_id,
        });

        Ok(())
    }

    /// List all available rooms
    pub async fn list_rooms(&self) -> Vec<RoomInfo> {
        let mut summaries = {
            let rooms = self.rooms.read().await;
            rooms
                .values()
                .map(|room| {
                    (room.name.clone(), (room.member_count(), room.created_at))
                })
                .collect::<HashMap<_, _>>()
        };

        let remote_rooms = self
            .presence
            .remote_rooms(self.config_snapshot().presence.stale_after)
            .await;
        for (name, remote) in remote_rooms {
            let created_at =
                OffsetDateTime::from_unix_timestamp(remote.created_at_unix)
                    .unwrap_or(OffsetDateTime::UNIX_EPOCH);
            let summary = summaries.entry(name).or_insert((0, created_at));
            summary.0 += remote.member_count;
            summary.1 = summary.1.min(created_at);
        }

        let mut rooms = summaries
            .into_iter()
            .map(|(name, (member_count, created_at))| RoomInfo {
                name,
                crated_at: created_at
                    .format(&Rfc3339)
                    .unwrap_or_else(|_| String::from("invalid-date")),
                member_count,
            })
            .collect::<Vec<_>>();
        rooms.sort_by(|left, right| left.name.cmp(&right.name));
        rooms
    }

    /// List users in a room (deduplicated by user_id for multi-device support)
    pub async fn list_users(
        &self,
        room_name: &str,
    ) -> Result<Vec<UserInfo>, String> {
        let users = self.fleet_room_users(room_name).await?;
        let mut user_infos = users
            .iter()
            .map(|user| UserInfo::from((user, true)))
            .collect::<Vec<_>>();
        user_infos.sort_by(|left, right| left.username.cmp(&right.username));
        Ok(user_infos)
    }

    /// Get full User models for all users in a room
    pub async fn get_room_users(
        &self,
        room_name: &str,
    ) -> Result<Vec<ChatUser>, String> {
        self.fleet_room_users(room_name).await
    }

    async fn fleet_room_users(
        &self,
        room_name: &str,
    ) -> Result<Vec<ChatUser>, String> {
        let mut users_by_id = {
            let rooms = self.rooms.read().await;
            let mut users = HashMap::new();
            if let Some(room) = rooms.get(room_name) {
                for client in room.get_members() {
                    users
                        .entry(client.user_id())
                        .or_insert_with(|| client.user().clone());
                }
            }
            users
        };

        let remote_ids = self
            .presence
            .remote_room_user_ids(
                room_name,
                self.config_snapshot().presence.stale_after,
            )
            .await;
        if users_by_id.is_empty() && remote_ids.is_empty() {
            return Err("Room not found".to_string());
        }

        let missing_ids = remote_ids
            .into_iter()
            .filter(|user_id| !users_by_id.contains_key(user_id))
            .collect::<Vec<_>>();
        for user in self.hydrate_users(&missing_ids).await? {
            users_by_id.insert(user.id(), user);
        }

        Ok(users_by_id.into_values().collect())
    }

    /// Get the total number of connected clients
    pub async fn client_count(&self) -> usize {
        self.clients.read().await.len()
    }

    /// Get total number of active rooms
    pub async fn room_count(&self) -> usize {
        self.rooms.read().await.len()
    }

    /// Get lightweight unread summary for a user (used in Welcome message)
    /// Only returns counts, not detailed sender info
    pub async fn get_unread_summary(
        &self,
        user_id: i32,
    ) -> Result<UnreadSummary, String> {
        let message_store =
            self.message_store.get().ok_or("Message store not available")?;

        // Get total unread private message count
        let private_count = message_store
            .count_unread_private_messages(user_id)
            .await
            .map_err(|e| {
                format!("Failed to count unread private messages: {}", e)
            })?;

        // Get unread room message counts by room
        let room_summaries = message_store
            .get_unread_room_counts_by_room(user_id)
            .await
            .map_err(|e| {
                format!("Failed to fetch unread room messages: {}", e)
            })?;

        // Convert to lightweight API format
        let room_messages = room_summaries
            .into_iter()
            .map(|s| UnreadRoomCount { room: s.room, count: s.unread_count })
            .collect();

        Ok(UnreadSummary {
            private_messages_count: private_count,
            room_messages,
        })
    }

    /// Get detailed unread private messages (on-demand, when user opens DM view)
    pub async fn get_unread_private_details(
        &self,
        user_id: i32,
    ) -> Result<Vec<UnreadPrivateDetail>, String> {
        let message_store =
            self.message_store.get().ok_or("Message store not available")?;

        let summaries = message_store
            .get_unread_private_counts_by_sender(user_id)
            .await
            .map_err(|e| {
                format!("Failed to fetch unread private messages: {}", e)
            })?;

        let mut details = Vec::with_capacity(summaries.len());
        for summary in summaries {
            let Some(sender) = self.hydrate_user(summary.sender_user_id).await
            else {
                warn!(
                    sender_user_id = summary.sender_user_id,
                    "unread private sender could not be hydrated"
                );
                continue;
            };
            details.push(UnreadPrivateDetail {
                sender_uuid: sender.uuid(),
                sender_username: sender.username().to_owned(),
                sender_avatar: sender.avatar_arc().to_string(),
                count: summary.unread_count,
            });
        }
        Ok(details)
    }

    /// Get unread count for a specific room (on-demand, when user joins room)
    pub async fn get_unread_room_count(
        &self,
        user_id: i32,
        room: &str,
    ) -> Result<i64, String> {
        let message_store =
            self.message_store.get().ok_or("Message store not available")?;

        message_store
            .count_unread_room_messages_by_room(user_id, room)
            .await
            .map_err(|e| format!("Failed to count unread room messages: {}", e))
    }

    /// Get list of currently online users
    pub async fn get_online_users(
        &self
    ) -> Result<Vec<OnlineUserInfo>, String> {
        let mut users_by_id = {
            let clients = self.clients.read().await;
            let mut users = HashMap::<i32, ChatUser>::new();
            for client in clients.values() {
                users
                    .entry(client.user_id())
                    .or_insert_with(|| client.user().clone());
            }
            users
        };

        let remote_ids = self
            .presence
            .remote_user_ids(self.config_snapshot().presence.stale_after)
            .await;
        let missing_ids = remote_ids
            .into_iter()
            .filter(|user_id| !users_by_id.contains_key(user_id))
            .collect::<Vec<_>>();
        for user in self.hydrate_users(&missing_ids).await? {
            users_by_id.insert(user.id(), user);
        }

        let mut users = users_by_id
            .into_values()
            .map(|user| OnlineUserInfo {
                user_uuid: user.uuid(),
                username: user.username_arc().clone(),
            })
            .collect::<Vec<_>>();
        users.sort_by(|left, right| left.username.cmp(&right.username));

        Ok(users)
    }

    async fn broadcast_to_all_except_user(
        &self,
        user_id: i32,
        msg: ServerMessage,
    ) {
        let clients = self.clients.read().await;
        for client in clients.values() {
            if client.user_id() != user_id {
                let _ = client.send(msg.clone()).await;
            }
        }
    }

    async fn send_to_user_local(
        &self,
        user_id: i32,
        msg: ServerMessage,
    ) {
        let clients = self.get_user_clients(user_id).await;
        for client in clients {
            let _ = client.send(msg.clone()).await;
        }
    }

    async fn broadcast_to_room_local(
        &self,
        room_name: &str,
        msg: ServerMessage,
    ) {
        let rooms = self.rooms.read().await;
        if let Some(room) = rooms.get(room_name) {
            room.broadcast(msg).await;
        }
    }

    async fn try_broadcast_to_room_local(
        &self,
        room_name: &str,
        msg: ServerMessage,
    ) {
        let rooms = self.rooms.read().await;
        if let Some(room) = rooms.get(room_name) {
            room.try_broadcast(msg);
        }
    }

    /// Broadcast message to a specific room (no sender validation)
    pub async fn broadcast_to_room(
        &self,
        room_name: &str,
        msg: ServerMessage,
    ) -> Result<(), String> {
        let rooms = self.rooms.read().await;
        let room = rooms.get(room_name).ok_or("Room not found")?;
        room.broadcast(msg).await;
        Ok(())
    }

    /// Get room message history
    pub async fn get_room_history(
        &self,
        room_name: &str,
        limit: Option<i64>,
        since: Option<OffsetDateTime>, // RFC3339 timestamp
        before: Option<OffsetDateTime>, // RFC3339 timestamp
    ) -> Result<Vec<HistoryMessage>, String> {
        let store =
            self.message_store.get().ok_or("Message store not available")?;

        // Determine which query to use based on parameters
        let messages = if let Some(since) = since {
            // Sync mode: get all messages AFTER this timestamp
            store.get_room_messages_since(room_name, since).await
        } else if let Some(before) = before {
            // Pagination mode: get messages BEFORE this timestamp with limit
            let limit = limit.unwrap_or(50).min(100);
            store.get_room_messages_before(room_name, before, limit).await
        } else {
            // Default: get recent messages with limit
            let limit = limit.unwrap_or(50).min(100);
            store.get_recent_room_messages(room_name, limit).await
        }
        .map_err(|e| format!("Database error: {}", e))?;

        // Convert to HistoryMessage format (UUID-only, no user details)
        // For since queries, messages come in ASC order already
        // For before and recent queries, we reverse to get chronological order
        let needs_reverse = since.is_none();

        // Collect all unique sender ids for UUID lookup
        let ids: Vec<i32> = messages
            .iter()
            .map(|m| m.from_user_id)
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();

        // Hydrate users to get UUIDs only (cache-first, DB on miss)
        let mut uuid_by_id: HashMap<i32, Uuid> = HashMap::new();
        if !ids.is_empty() {
            let users = self.hydrate_users(&ids).await.unwrap_or_default();
            for u in users {
                uuid_by_id.insert(u.id(), u.uuid());
            }
        }

        // Build history entries with UUID-only sender references
        let mut result: Vec<HistoryMessage> = messages
            .into_iter()
            .filter_map(|msg| {
                // Get sender UUID, skip if user not found
                uuid_by_id.get(&msg.from_user_id).map(|&from_uuid| {
                    HistoryMessage {
                        uuid: msg.uuid,
                        from_uuid,
                        content: msg.content,
                        timestamp: msg.created_at,
                    }
                })
            })
            .collect();

        if needs_reverse {
            result.reverse();
        }

        Ok(result)
    }

    /// Get direct message history with another user
    pub async fn get_direct_history(
        &self,
        from_user_id: i32,
        with_user_id: i32,
        limit: Option<i64>,
        since: Option<OffsetDateTime>,  // timestamp
        before: Option<OffsetDateTime>, // timestamp
    ) -> Result<Vec<HistoryMessage>, String> {
        let store =
            self.message_store.get().ok_or("Message store not available")?;

        // Determine which query to use based on parameters
        let messages = if let Some(since) = since {
            store
                .get_private_messages_since(from_user_id, with_user_id, since)
                .await
        } else if let Some(before) = before {
            let limit = limit.unwrap_or(50).min(100);
            store
                .get_private_messages_before(
                    from_user_id,
                    with_user_id,
                    before,
                    limit,
                )
                .await
        } else {
            let limit = limit.unwrap_or(50).min(100);
            store
                .get_recent_private_messages(from_user_id, with_user_id, limit)
                .await
        }
        .map_err(|e| format!("Database error: {}", e))?;

        let needs_reverse = since.is_none();

        // Collect all unique sender ids for UUID lookup
        let ids: Vec<i32> = messages
            .iter()
            .map(|m| m.from_user_id)
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();

        // Hydrate users to get UUIDs only (cache-first, DB on miss)
        let mut uuid_by_id: HashMap<i32, Uuid> = HashMap::new();
        if !ids.is_empty() {
            let users = self.hydrate_users(&ids).await.unwrap_or_default();
            for u in users {
                uuid_by_id.insert(u.id(), u.uuid());
            }
        }

        // Build history entries with UUID-only sender references
        let mut result: Vec<HistoryMessage> = messages
            .into_iter()
            .filter_map(|msg| {
                // Get sender UUID, skip if user not found
                uuid_by_id.get(&msg.from_user_id).map(|&from_uuid| {
                    HistoryMessage {
                        uuid: msg.uuid,
                        from_uuid,
                        content: msg.content,
                        timestamp: msg.created_at,
                    }
                })
            })
            .collect();

        if needs_reverse {
            result.reverse();
        }

        Ok(result)
    }

    /// Mark message as delivered by a user
    pub async fn mark_delivered(
        &self,
        message_uuid: &Uuid,
        user_id: i32,
        room: Option<&str>,
    ) -> Result<(), String> {
        let message_store =
            self.message_store.get().ok_or("Message store not available")?;

        // Get reader user info for logging
        let reader_users =
            self.hydrate_users(&[user_id]).await.unwrap_or_default();
        let reader_user = reader_users.first();

        // If room is None, this is a private message
        if room.is_none() {
            if let Ok(Some(private_msg)) =
                message_store.get_private_message_by_uuid(message_uuid).await
            {
                // Mark private message as delivered
                let actually_updated = message_store
                    .mark_private_message_delivered(message_uuid)
                    .await
                    .map_err(|e| {
                        format!(
                            "Failed to mark private message delivered: {}",
                            e
                        )
                    })?;

                // Only send status and log if actually updated (wasn't delivered before)
                if actually_updated {
                    // Get sender info for logging
                    let sender_users = self
                        .hydrate_users(&[private_msg.from_user_id])
                        .await
                        .unwrap_or_default();
                    let sender_user = sender_users.first();

                    self.emit_delivery(ChatDeliveryEvent::MessageStatus {
                        uuid: *message_uuid,
                        sender_user_id: private_msg.from_user_id,
                        status: MessageStatusType::Delivered,
                        room_message: false,
                    });

                    debug!(
                        msg_uuid = %message_uuid,
                        reader = %reader_user.as_ref().map(|u| u.username()).unwrap_or("unknown"),
                        sender = %sender_user.as_ref().map(|u| u.username()).unwrap_or("unknown"),
                        msg_type = "private",
                        "📬 Private message marked delivered"
                    );
                }
                return Ok(());
            }
            return Err("Private message not found".to_string());
        }

        // If room is Some, this is a room message
        if let Ok(Some(room_msg)) =
            message_store.get_room_message_by_uuid(message_uuid).await
        {
            let receipt_store = self
                .receipt_store
                .get()
                .ok_or("Receipt store not available")?;

            // Mark room message as delivered for this user
            let actually_updated = receipt_store
                .mark_delivered(message_uuid, user_id)
                .await
                .map_err(|e| {
                    format!("Failed to mark room message delivered: {}", e)
                })?;

            // Only send status and log if actually updated (wasn't delivered before)
            if actually_updated {
                // Get sender info for logging
                let sender_users = self
                    .hydrate_users(&[room_msg.from_user_id])
                    .await
                    .unwrap_or_default();
                let sender_user = sender_users.first();

                let total_recipients = receipt_store
                    .get_total_recipients(message_uuid)
                    .await
                    .unwrap_or(0)
                    as usize;
                self.emit_delivery(ChatDeliveryEvent::MessageStatus {
                    uuid: *message_uuid,
                    sender_user_id: room_msg.from_user_id,
                    status: MessageStatusType::Delivered,
                    room_message: true,
                });

                debug!(
                    msg_uuid = %message_uuid,
                    reader = %reader_user.as_ref().map(|u| u.username()).unwrap_or("unknown"),
                    sender = %sender_user.as_ref().map(|u| u.username()).unwrap_or("unknown"),
                    room = %room.unwrap_or("none"),
                    msg_type = "room",
                    recipients = %total_recipients,
                    "📬 Room message marked delivered"
                );
            }
            return Ok(());
        }

        Err("Room message not found".to_string())
    }

    /// Mark message as read by a user
    pub async fn mark_read(
        &self,
        message_uuid: &Uuid,
        user_id: i32,
        room: Option<&str>,
    ) -> Result<(), String> {
        let message_store =
            self.message_store.get().ok_or("Message store not available")?;

        // Get reader user info for logging
        let reader_users =
            self.hydrate_users(&[user_id]).await.unwrap_or_default();
        let reader_user = reader_users.first();

        // If room is None, this is a private message
        if room.is_none() {
            if let Ok(Some(private_msg)) =
                message_store.get_private_message_by_uuid(message_uuid).await
            {
                // Mark private message as read
                let actually_updated = message_store
                    .mark_private_message_read(message_uuid)
                    .await
                    .map_err(|e| {
                        format!("Failed to mark private message read: {}", e)
                    })?;

                // Only send status and log if actually updated (wasn't read before)
                if actually_updated {
                    // Get sender info for logging
                    let sender_users = self
                        .hydrate_users(&[private_msg.from_user_id])
                        .await
                        .unwrap_or_default();
                    let sender_user = sender_users.first();

                    self.emit_delivery(ChatDeliveryEvent::MessageStatus {
                        uuid: *message_uuid,
                        sender_user_id: private_msg.from_user_id,
                        status: MessageStatusType::Read,
                        room_message: false,
                    });

                    debug!(
                        msg_uuid = %message_uuid,
                        reader = %reader_user.map(|u| u.username()).unwrap_or("unknown"),
                        sender = %sender_user.map(|u| u.username()).unwrap_or("unknown"),
                        msg_type = "private",
                        "✓✓ Private message marked read"
                    );
                }
                return Ok(());
            }
            return Err("Private message not found".to_string());
        }

        // If room is Some, this is a room message
        if let Ok(Some(room_msg)) =
            message_store.get_room_message_by_uuid(message_uuid).await
        {
            let receipt_store = self
                .receipt_store
                .get()
                .ok_or("Receipt store not available")?;

            // Mark room message as read for this user
            // Check if already read by checking if insert happened
            let actually_updated =
                receipt_store.mark_read(message_uuid, user_id).await.map_err(
                    |e| format!("Failed to mark room message read: {}", e),
                )?;

            // Only send status and log if actually updated (wasn't read before)
            if actually_updated {
                // Get readers and total recipients for room message
                let reader_ids = receipt_store
                    .get_readers(message_uuid)
                    .await
                    .unwrap_or_default();

                let total_recipients = receipt_store
                    .get_total_recipients(message_uuid)
                    .await
                    .unwrap_or(0)
                    as usize;

                // Convert reader user_ids to ReadReceipt structs with UUIDs
                let mut read_by = Vec::new();
                if !reader_ids.is_empty() {
                    // Hydrate all reader users in bulk
                    let reader_users = self
                        .hydrate_users(&reader_ids)
                        .await
                        .unwrap_or_default();

                    for reader_user in reader_users {
                        read_by.push(ReadReceipt {
                            user_uuid: reader_user.uuid(),
                            username: reader_user.username().to_string(),
                        });
                    }
                }

                // Get sender info for logging
                let sender_users = self
                    .hydrate_users(&[room_msg.from_user_id])
                    .await
                    .unwrap_or_default();
                let sender_user = sender_users.first();

                self.emit_delivery(ChatDeliveryEvent::MessageStatus {
                    uuid: *message_uuid,
                    sender_user_id: room_msg.from_user_id,
                    status: MessageStatusType::Read,
                    room_message: true,
                });

                debug!(
                    msg_uuid = %message_uuid,
                    reader = %reader_user.map(|u| u.username()).unwrap_or("unknown"),
                    sender = %sender_user.map(|u| u.username()).unwrap_or("unknown"),
                    room = %room.unwrap_or("none"),
                    msg_type = "room",
                    read_count = %read_by.len(),
                    total_recipients = %total_recipients,
                    "✓✓ Room message marked read"
                );
            }
            return Ok(());
        }

        Err("Room message not found".to_string())
    }
}

/// Registers the server, after `ChatConfigProvider`, with that provider
/// and the process event bus in hand.
impl Register<AppState> for ChatServer {
    fn register(state: &AppState) -> claviron::runtime::registry::Result<()> {
        let cfg_provider = state
            .registry_ref()
            .resolve::<ChatConfigProvider>()
            .ok_or_else(|| {
                RegistryError::msg(
                    "ChatServer needs ChatConfigProvider: register it first",
                )
            })?;
        let server = ChatServer::empty();
        let _ = server.config_provider.set(cfg_provider);
        let _ = server.events.set(state.events().bus().clone());
        state.registry_ref().register(Arc::new(server))?;
        Ok(())
    }
}

#[async_trait]
impl Component<AppState> for ChatServer {
    // Disabled chat does not require DB; an available fleet bridge is bound at boot.
    type Deps = (Option<Arc<DbService>>, Option<Arc<FleetEventRegistry>>);

    fn name(&self) -> &'static str {
        "ChatServer"
    }

    fn as_bootable(&self) -> Option<&dyn Bootable<AppState, Self::Deps>> {
        Some(self)
    }

    fn as_reloadable(&self) -> Option<&dyn Reloadable<AppState>> {
        Some(self)
    }

    fn as_runnable(
        self: Arc<Self>
    ) -> Option<Arc<dyn Runnable<AppState, Self::Deps>>> {
        Some(self)
    }

    fn as_scoped(&self) -> Option<&dyn Scoped> {
        Some(self)
    }
}

/// Fanout and presence belong where connections are: workers, or a
/// standalone process. The master holds no clients.
impl Scoped for ChatServer {
    fn scope(&self) -> Scope {
        Scope::Worker
    }
}

/// The owned work: the delivery and presence listeners and the presence
/// refresh, started when the process runs and ended with it. `enabled: false`
/// runs nothing.
#[async_trait]
impl Runnable<AppState, <Self as Component<AppState>>::Deps> for ChatServer {
    async fn run(
        self: Arc<Self>,
        _deps: <Self as Component<AppState>>::Deps,
        state: AppState,
        context: RunContext,
    ) -> claviron::runtime::registry::Result<()> {
        if !self.config_snapshot().enabled {
            return Ok(());
        }
        self.start_delivery_listener(state.clone());
        self.start_presence_listener(state.clone());
        self.start_presence_refresh(state);
        context.cancelled().await;
        Ok(())
    }
}

#[async_trait]
impl Reloadable<AppState> for ChatServer {
    async fn reload(
        &self,
        _state: &AppState,
    ) -> claviron::runtime::registry::Result<()> {
        // ChatConfigProvider owns the file read and runs at EARLY,
        // so by the time we get here its snapshot is already fresh.
        let cfg = self.config_provider().current();

        let new_rate_limiter = ChatRateLimiter::new(
            cfg.rate_limit.window,
            cfg.rate_limit.max_messages,
        );
        self.rate_limiter.store(Some(Arc::new(new_rate_limiter)));
        Ok(())
    }
}

#[async_trait]
impl Bootable<AppState, <Self as Component<AppState>>::Deps> for ChatServer {
    /// Derive the rate limiter from the applied configuration and — if chat
    /// is enabled — take the identity, auth and database handles from the
    /// registry. No file I/O, nothing opened, nothing spawned.
    async fn boot(
        &self,
        (db, events): <Self as Component<AppState>>::Deps,
        state: &AppState,
    ) -> claviron::runtime::registry::Result<()> {
        let cfg = self.config_snapshot();
        let new_rate_limiter = ChatRateLimiter::new(
            cfg.rate_limit.window,
            cfg.rate_limit.max_messages,
        );
        self.rate_limiter.store(Some(Arc::new(new_rate_limiter)));

        if !cfg.enabled {
            return Ok(());
        }

        // Identity/auth are bound DI capabilities, not lifecycle components.
        // Requiring them does not imply a component boot-order edge.
        let users = state
            .registry_ref()
            .resolve_dyn::<dyn UserProvider>()
            .ok_or_else(|| {
                RegistryError::msg("ChatServer boot: UserProvider not bound")
            })?;
        let auth = state.registry_ref().resolve_dyn::<dyn TokenAuthenticator>();
        if cfg.auth.guard_enabled(crate::config::ChatAuthGuard::Orbit)
            && auth.is_none()
        {
            return Err(RegistryError::msg(
                "ChatServer boot: Orbit TokenAuthenticator not bound",
            ));
        }
        if let Some(auth) = auth {
            let _ = self.auth.set(auth);
        }
        let session_auth =
            state.registry_ref().resolve_dyn::<dyn SessionAuthenticator>();
        if cfg.auth.guard_enabled(crate::config::ChatAuthGuard::Session)
            && session_auth.is_none()
        {
            return Err(RegistryError::msg(
                "ChatServer boot: SessionAuthenticator not bound",
            ));
        }
        if let Some(auth) = session_auth {
            let _ = self.session_auth.set(auth);
        }
        let avatars =
            state.registry_ref().resolve_dyn::<dyn AvatarUrlProvider>();
        let _ = self.users.set(users);
        let _ = self.avatars.set(avatars);

        let db = db.ok_or_else(|| {
            RegistryError::msg("ChatServer boot: DbService not registered")
        })?;
        let pool = db.claviron_pool().ok_or_else(|| {
            RegistryError::msg("ChatServer boot: claviron postgres pool unavailable from DbService")
        })?;

        let _ = self.db_pool.set(pool.clone());
        let _ = self.message_store.set(MessageStore::new(pool.clone()));
        let _ = self.receipt_store.set(ReceiptStore::new(pool));

        if let Some(registry) = events {
            registry.register_bridge::<ChatDeliveryEvent>(state)?;
            registry.register_bridge::<ChatPresenceEvent>(state)?;
        } else {
            warn!(
                "ChatServer booted without FleetEventRegistry; chat fanout and presence are worker-local"
            );
        }

        info!("📦 ChatServer booted");
        Ok(())
    }
}
