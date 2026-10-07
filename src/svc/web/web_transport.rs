use async_trait::async_trait;
use bytes::BytesMut;
use claviron::edge::{
    AnyAuthenticated, AuthContext, H3WebTransportSession, WebTransportAccept,
    WebTransportHandler, WebTransportReject, WebTransportResult,
};
use claviron::runtime::AppState;
use claviron::runtime::registry::{
    Error as RegistryError, Result as RegistryResult,
};
use h3_webtransport::server::AcceptedBi;
use tokio::sync::mpsc;
use tracing::{debug, error, warn};

use super::auth::extract_user_from_authorization;
use super::framing::{read_client_message, write_server_message};
use crate::config::ChatConfigProvider;
use crate::svc::WELCOME_MESSAGE;
#[cfg(feature = "metrics")]
use crate::svc::metrics::CHAT_METRICS;
use crate::types::messages::{ServerMessage, UnreadSummary};
use crate::{ChatClient, ChatServer, handle_client_message};

pub(crate) struct ChatWebTransportHandler {
    config_provider: std::sync::Arc<ChatConfigProvider>,
}

impl ChatWebTransportHandler {
    pub(crate) fn new(
        config_provider: std::sync::Arc<ChatConfigProvider>
    ) -> Self {
        Self { config_provider }
    }
}

#[async_trait]
impl WebTransportHandler for ChatWebTransportHandler {
    fn name(&self) -> &'static str {
        "chat_web_transport"
    }

    async fn accept(
        &self,
        state: &AppState,
        ctx: &AuthContext,
    ) -> WebTransportResult {
        let authorization = ctx
            .headers
            .get(http::header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .ok_or_else(WebTransportReject::unauthorized)?;
        let cfg = self.config_provider.current();
        let user =
            extract_user_from_authorization(authorization, state, &cfg.auth)
                .await
                .map_err(|_| WebTransportReject::unauthorized())?;
        Ok(WebTransportAccept::authenticated(user))
    }

    async fn handle(
        &self,
        state: AppState,
        ctx: AuthContext,
        authenticated: Option<AnyAuthenticated>,
        session: H3WebTransportSession,
    ) -> RegistryResult<()> {
        drop(ctx); // Do not retain credential headers for the connection lifetime.
        let user = authenticated
            .ok_or_else(|| {
                RegistryError::msg("chat WebTransport auth missing")
            })?
            .take::<crate::types::client::ChatAuthentication>()?;
        handle_web_transport_chat(state, user, session).await
    }
}

async fn handle_web_transport_chat(
    state: AppState,
    authenticated: crate::types::client::ChatAuthentication,
    session: H3WebTransportSession,
) -> RegistryResult<()> {
    let crate::types::client::ChatAuthentication {
        user,
        session: auth_session,
    } = authenticated;
    let server = state
        .registry_ref()
        .resolve::<ChatServer>()
        .ok_or_else(|| RegistryError::msg("ChatServer not registered"))?;
    let chat_config = server.config_snapshot();
    let max_frame = chat_config.max_message_size;

    let stream = match accept_chat_stream(&session).await? {
        Some(stream) => stream,
        None => return Ok(()),
    };
    let (mut reader, mut writer) = tokio::io::split(stream);
    let (client_tx, mut client_rx) = mpsc::channel::<ServerMessage>(max_frame);

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
        ))
        .await;

    let mut inbound = BytesMut::new();
    debug!(
        username = %user.username(),
        connection_id = %connection_id,
        session_id = ?session.session_id(),
        "starting WebTransport chat session"
    );

    loop {
        tokio::select! {
            _ = state.on_shutdown() => break,
            Some(msg) = client_rx.recv() => {
                if !client.auth_valid() { break; }
                if let Err(err) = write_server_message(&mut writer, msg, max_frame).await {
                    error!(username = user.username(), error = %err, "failed to write WebTransport chat frame");
                    break;
                }
                #[cfg(feature = "metrics")]
                CHAT_METRICS.message_sent();
            }
            result = read_client_message(&mut reader, &mut inbound, max_frame) => {
                match result {
                    Ok(Some(msg)) => {
                        if !client.auth_valid() { break; }
                        #[cfg(feature = "metrics")]
                        CHAT_METRICS.message_received();
                        handle_client_message(server.as_ref(), &client, msg).await;
                    }
                    Ok(None) => break,
                    Err(err) => {
                        warn!(username = user.username(), error = %err, "WebTransport chat input failed");
                        break;
                    }
                }
            }
        }
    }

    #[cfg(feature = "metrics")]
    CHAT_METRICS.disconnect();

    server.disconnect_client(connection_id).await;
    debug!(
        username = %user.username(),
        connection_id = %connection_id,
        "WebTransport chat session closed"
    );
    Ok(())
}

async fn accept_chat_stream(
    session: &H3WebTransportSession
) -> RegistryResult<
    Option<
        h3_webtransport::stream::BidiStream<
            h3_quinn::BidiStream<bytes::Bytes>,
            bytes::Bytes,
        >,
    >,
> {
    loop {
        let accepted = session.accept_bi().await.map_err(|err| {
            RegistryError::msg(format!(
                "WebTransport chat stream accept failed: {err}"
            ))
        })?;
        let Some(accepted) = accepted else {
            return Ok(None);
        };
        let resolved = accepted.resolve().await.map_err(|err| {
            RegistryError::msg(format!(
                "WebTransport chat stream resolve failed: {err}"
            ))
        })?;
        match resolved {
            AcceptedBi::BidiStream(session_id, stream)
                if session_id == session.session_id() =>
            {
                return Ok(Some(stream));
            }
            AcceptedBi::BidiStream(session_id, _) => {
                warn!(
                    expected = ?session.session_id(),
                    actual = ?session_id,
                    "ignoring WebTransport stream for another session"
                );
            }
            AcceptedBi::Request(_, _) => {
                warn!("ignoring HTTP/3 request on WebTransport chat session");
            }
        }
    }
}
