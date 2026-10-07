use std::collections::HashSet;
use std::sync::Arc;

use claviron::auth::orbit::VerifiedSession;
use mpsc::Sender;
use tokio::sync::{RwLock, mpsc};
use tracing::warn;
use uuid::Uuid;

use super::messages::ServerMessage;
use crate::models::users::ChatUser;

pub(crate) struct ChatAuthentication {
    pub user: ChatUser,
    pub session: Option<VerifiedSession>,
}

/// Represents a connected svc client
#[derive(Clone)]
pub struct ChatClient {
    /// Unique connection ID (server-generated, per WebSocket connection)
    pub connection_id: Uuid,

    /// Authenticated user information
    pub user: ChatUser,

    session: Option<VerifiedSession>,

    /// Channel to send messages to this client
    pub tx: Sender<ServerMessage>,

    /// Rooms this client has joined
    pub rooms: Arc<RwLock<HashSet<String>>>,
}

impl ChatClient {
    /// Create a new svc client with auto-generated connection ID
    pub fn new(
        user: ChatUser,
        tx: Sender<ServerMessage>,
    ) -> Self {
        Self {
            connection_id: Uuid::new_v4(),
            user,
            session: None,
            tx,
            rooms: Arc::new(RwLock::new(HashSet::new())),
        }
    }

    pub(crate) fn with_session(
        mut self,
        session: Option<VerifiedSession>,
    ) -> Self {
        self.session = session;
        self
    }

    pub(crate) fn auth_valid(&self) -> bool {
        self.session.as_ref().is_none_or(|session| {
            auth_now().is_some_and(|now| session.check(now).is_ok())
        })
    }

    pub fn user(&self) -> &ChatUser {
        &self.user
    }

    /// Get connection ID
    pub fn connection_id(&self) -> Uuid {
        self.connection_id
    }

    /// Get user ID
    pub fn user_id(&self) -> i32 {
        self.user.id()
    }

    pub fn user_uuid(&self) -> Uuid {
        self.user.uuid()
    }

    /// Get username
    pub fn username_arc(&self) -> Arc<str> {
        self.user.username_arc().clone()
    }

    pub fn username(&self) -> &str {
        self.user.username()
    }

    pub fn name_arc(&self) -> Arc<str> {
        self.user.name_arc().clone()
    }

    pub fn avatar_arc(&self) -> Arc<str> {
        self.user.avatar_arc().clone()
    }

    /// Send a message to this client (non-blocking)
    pub async fn send(
        &self,
        msg: ServerMessage,
    ) -> bool {
        match self.tx.send(msg).await {
            Ok(_) => true,
            Err(e) => {
                warn!(username = self.username(), error = %e,
                  "Failed to send message to client");
                false
            }
        }
    }

    /// Try to send a message (returns immediately if channel is full)
    pub fn try_send(
        &self,
        msg: ServerMessage,
    ) -> bool {
        match self.tx.try_send(msg) {
            Ok(_) => true,
            Err(e) => {
                warn!(
                    user_id = self.user_id(),
                    username = self.username(),
                    error = %e,
                    "Failed to send message to client (non-blocking)"
                );
                false
            }
        }
    }

    /// Check if client is in a specific room
    pub async fn is_in_room(
        &self,
        room: &str,
    ) -> bool {
        self.rooms.read().await.contains(room)
    }

    /// Add client to a room
    pub async fn join_room(
        &self,
        room: String,
    ) {
        self.rooms.write().await.insert(room);
    }

    /// Remove client from a room
    pub async fn leave_room(
        &self,
        room: &str,
    ) -> bool {
        self.rooms.write().await.remove(room)
    }

    /// Get list of rooms this client is in
    pub async fn get_rooms(&self) -> Vec<String> {
        self.rooms.read().await.iter().cloned().collect()
    }

    /// Leave all rooms
    pub async fn leave_all_rooms(&self) -> Vec<String> {
        let mut rooms = self.rooms.write().await;
        let room_list = rooms.iter().cloned().collect();
        rooms.clear();
        room_list
    }
}

pub(crate) fn auth_now() -> Option<u64> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .map(|now| now.as_secs())
}
