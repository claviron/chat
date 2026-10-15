use std::convert::Infallible;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Buf, Bytes, BytesMut};
use claviron::edge::{
    AuthContext, H2ConnectAccept, H2ConnectHandler, H2ConnectReject,
    H2ConnectResult,
};
use claviron::http::{RouteBody, RouteResponse, boxed_body};
use claviron::runtime::AppState;
use claviron::runtime::registry::{
    Error as RegistryError, Result as RegistryResult,
};
use futures_util::stream;
use http::{Request, Response, StatusCode};
use http_body::Frame;
use http_body_util::{BodyExt, StreamBody};
use tokio::sync::mpsc;
use tracing::{debug, error, trace, warn};

use super::auth::{extract_session_user, extract_user_from_authorization};
use super::framing::{
    FRAME_PREFIX_LEN, decode_client_message, encode_server_message,
};
use crate::config::ChatConfigProvider;
use crate::models::users::ChatUser;
use crate::svc::WELCOME_MESSAGE;
#[cfg(feature = "metrics")]
use crate::svc::metrics::CHAT_METRICS;
use crate::types::messages::{ClientMessage, ServerMessage, UnreadSummary};
use crate::{ChatClient, ChatServer, handle_client_message};

pub(crate) struct ChatH2ConnectHandler {
    config_provider: Arc<ChatConfigProvider>,
}

impl ChatH2ConnectHandler {
    pub(crate) fn new(config_provider: Arc<ChatConfigProvider>) -> Self {
        Self { config_provider }
    }
}

#[async_trait]
impl H2ConnectHandler for ChatH2ConnectHandler {
    fn name(&self) -> &'static str {
        "chat_h2_connect"
    }

    async fn accept(
        &self,
        state: &AppState,
        ctx: &AuthContext,
    ) -> H2ConnectResult {
        let cfg = self.config_provider.current();
        if cfg.auth.guard_enabled(crate::config::ChatAuthGuard::Session) {
            let user = extract_session_user(
                state,
                ctx.host.to_cow_str().as_ref(),
                &ctx.headers,
                &cfg,
            )
            .await
            .map_err(H2ConnectReject::new)?;
            return Ok(H2ConnectAccept::authenticated(user));
        }
        let authorization = ctx
            .headers
            .get(http::header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .ok_or_else(H2ConnectReject::unauthorized)?;
        let user =
            extract_user_from_authorization(authorization, state, &cfg.auth)
                .await
                .map_err(|_| H2ConnectReject::unauthorized())?;
        Ok(H2ConnectAccept::authenticated(user))
    }

    async fn handle(
        &self,
        state: AppState,
        ctx: AuthContext,
        authenticated: Option<claviron::edge::AnyAuthenticated>,
        req: Request<RouteBody>,
    ) -> RegistryResult<RouteResponse> {
        drop(ctx); // Do not retain credential headers for the connection lifetime.
        let user = authenticated
            .ok_or_else(|| RegistryError::msg("chat H2 CONNECT auth missing"))?
            .take::<crate::types::client::ChatAuthentication>()?;
        handle_h2_chat(req, state, user).await
    }
}

async fn handle_h2_chat(
    req: Request<RouteBody>,
    state: AppState,
    authenticated: crate::types::client::ChatAuthentication,
) -> RegistryResult<RouteResponse> {
    let crate::types::client::ChatAuthentication {
        user,
        session: auth_session,
    } = authenticated;
    let server = state
        .registry_ref()
        .resolve::<ChatServer>()
        .ok_or_else(|| RegistryError::msg("ChatServer not registered"))?;
    let chat_config = server.config_snapshot();
    let (client_tx, client_rx) =
        mpsc::channel::<ServerMessage>(chat_config.max_message_size);
    let (body_tx, body_rx) =
        mpsc::channel::<Bytes>(chat_config.max_message_size);

    let client = ChatClient::new(user.clone(), client_tx.clone())
        .with_session(auth_session);
    if !client.auth_valid() {
        return Err(RegistryError::msg("chat session expired or revoked"));
    }
    let connection_id = client.connection_id();
    server.connect_client(client.clone()).await;

    #[cfg(feature = "metrics")]
    CHAT_METRICS.connect();

    let unread_summary = server.get_unread_summary(user.id()).await.unwrap_or_else(|e| {
        warn!(username = user.username(), error = %e, "failed to fetch unread counts");
        UnreadSummary { private_messages_count: 0, room_messages: vec![] }
    });
    let _ = client_tx
        .send(ServerMessage::welcome(
            connection_id,
            user.uuid(),
            user.username_arc().clone(),
            user.name_arc().clone(),
            WELCOME_MESSAGE,
            unread_summary,
            chat_config.history.clone(),
        ))
        .await;

    let max_frame = chat_config.max_message_size;
    let body = req.into_body();
    tokio::spawn(async move {
        let mut session = H2ChatSession {
            state,
            server,
            client,
            user,
            inbound: BytesMut::new(),
            body,
            client_rx,
            body_tx,
            max_frame,
        };
        session.run().await;
    });

    Response::builder()
        .status(StatusCode::OK)
        .header(http::header::CONTENT_TYPE, "application/x-msgpack")
        .header("x-claviron-chat-framing", "u32be-msgpack")
        .body(body_from_receiver(body_rx))
        .map_err(|err| RegistryError::msg(err.to_string()))
}

struct H2ChatSession {
    state: AppState,
    server: Arc<ChatServer>,
    client: ChatClient,
    user: ChatUser,
    inbound: BytesMut,
    body: RouteBody,
    client_rx: mpsc::Receiver<ServerMessage>,
    body_tx: mpsc::Sender<Bytes>,
    max_frame: usize,
}

impl H2ChatSession {
    async fn run(&mut self) {
        debug!(
            username = %self.user.username(),
            connection_id = %self.client.connection_id(),
            "starting H2 CONNECT chat session"
        );

        loop {
            tokio::select! {
                _ = self.state.on_shutdown() => break,
                Some(msg) = self.client_rx.recv() => {
                    if !self.client.auth_valid() { break; }
                    if send_to_body(&self.body_tx, msg, self.max_frame).await.is_err() {
                        break;
                    }
                }
                result = read_next_client_message(&mut self.body, &mut self.inbound, self.max_frame) => {
                    match result {
                        Ok(Some(msg)) => {
                            if !self.client.auth_valid() { break; }
                            #[cfg(feature = "metrics")]
                            CHAT_METRICS.message_received();
                            handle_client_message(self.server.as_ref(), &self.client, msg).await;
                        }
                        Ok(None) => break,
                        Err(err) => {
                            warn!(username = self.user.username(), error = %err, "H2 chat input failed");
                            break;
                        }
                    }
                }
            }
        }

        #[cfg(feature = "metrics")]
        CHAT_METRICS.disconnect();

        self.server.disconnect_client(self.client.connection_id()).await;
        debug!(
            username = %self.user.username(),
            connection_id = %self.client.connection_id(),
            "H2 CONNECT chat session closed"
        );
    }
}

async fn send_to_body(
    body_tx: &mpsc::Sender<Bytes>,
    msg: ServerMessage,
    max_frame: usize,
) -> Result<(), ()> {
    match encode_server_message(&msg, max_frame) {
        Ok(bytes) => {
            trace!(size = bytes.len(), "sending H2 chat frame");
            body_tx.send(bytes).await.map_err(|_| ())?;
            #[cfg(feature = "metrics")]
            CHAT_METRICS.message_sent();
            Ok(())
        }
        Err(err) => {
            error!(error = %err, "failed to encode H2 chat frame");
            Err(())
        }
    }
}

async fn read_next_client_message(
    body: &mut RouteBody,
    inbound: &mut BytesMut,
    max_frame: usize,
) -> Result<Option<ClientMessage>, String> {
    loop {
        if inbound.len() >= FRAME_PREFIX_LEN {
            let len = u32::from_be_bytes(
                inbound[..FRAME_PREFIX_LEN]
                    .try_into()
                    .expect("slice length checked"),
            ) as usize;
            if len > max_frame {
                return Err(format!("frame too large: {len} > {max_frame}"));
            }
            if inbound.len() >= FRAME_PREFIX_LEN + len {
                inbound.advance(FRAME_PREFIX_LEN);
                let payload = inbound.split_to(len);
                return decode_client_message(&payload).map(Some);
            }
        }

        match body.frame().await {
            Some(Ok(frame)) => {
                if let Some(data) = frame.data_ref() {
                    inbound.extend_from_slice(data);
                }
            }
            Some(Err(err)) => return Err(err.to_string()),
            None => return Ok(None),
        }
    }
}

fn body_from_receiver(rx: mpsc::Receiver<Bytes>) -> RouteBody {
    let s = stream::unfold(rx, |mut rx| async move {
        rx.recv()
            .await
            .map(|chunk| (Ok::<_, Infallible>(Frame::data(chunk)), rx))
    });
    boxed_body(StreamBody::new(s))
}
