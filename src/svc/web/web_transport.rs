use async_trait::async_trait;
use std::sync::Arc;

use claviron::edge::{
    AnyAuthenticated, AuthContext, H3WebTransportBiStream,
    H3WebTransportSession, WebTransportAccept, WebTransportHandler,
    WebTransportReject, WebTransportResult,
};
use claviron::runtime::AppState;
use claviron::runtime::registry::{
    Error as RegistryError, Result as RegistryResult,
};
use tokio::sync::mpsc;
use tracing::{debug, warn};

use super::auth::{extract_session_user, extract_user_from_authorization};
use crate::config::{ChatConfig, ChatConfigProvider};
use crate::svc::WELCOME_MESSAGE;
#[cfg(feature = "metrics")]
use crate::svc::metrics::CHAT_METRICS;
use crate::types::messages::{ServerMessage, UnreadSummary};
use crate::{ChatClient, ChatServer, handle_client_message};

mod io;
mod streams;

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
        let cfg = self.config_provider.current();
        let embed = cfg.embed.as_ref().ok_or_else(|| {
            WebTransportReject::new(http::StatusCode::SERVICE_UNAVAILABLE)
        })?;
        if embed.auth.guard_enabled(crate::config::ChatAuthGuard::Session) {
            let user = extract_session_user(
                state,
                ctx.host.to_cow_str().as_ref(),
                &ctx.headers,
                embed,
            )
            .await
            .map_err(WebTransportReject::new)?;
            return Ok(WebTransportAccept::authenticated(user));
        }
        let authorization = ctx
            .headers
            .get(http::header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .ok_or_else(WebTransportReject::unauthorized)?;
        let user =
            extract_user_from_authorization(authorization, state, &embed.auth)
                .await
                .map_err(|_| WebTransportReject::unauthorized())?;
        Ok(WebTransportAccept::authenticated(user))
    }

    async fn handle(
        &self,
        state: AppState,
        mut ctx: AuthContext,
        authenticated: Option<AnyAuthenticated>,
        session: H3WebTransportSession,
    ) -> RegistryResult<()> {
        // Retain native drain ownership, not credential headers or the URI.
        let admission = ctx.admission.take();
        let graceful = ctx.graceful.take();
        drop(ctx);
        let user = authenticated
            .ok_or_else(|| {
                RegistryError::msg("chat WebTransport auth missing")
            })?
            .take::<crate::types::client::ChatAuthentication>()?;
        let server = state
            .registry_ref()
            .resolve::<ChatServer>()
            .ok_or_else(|| RegistryError::msg("ChatServer not registered"))?;
        let config = server.config_snapshot();
        let shutdown = async {
            let listener = async {
                match &admission {
                    Some(permit) => permit.wait_graceful_shutdown().await,
                    None => std::future::pending().await,
                }
            };
            let host = async {
                match graceful {
                    Some(signal) => signal.await,
                    None => std::future::pending().await,
                }
            };
            tokio::select! {
                () = listener => {},
                () = host => {},
                () = state.on_shutdown() => {},
                closed = session.closed() => {
                    if let Err(error) = closed {
                        debug!(%error, "WebTransport chat CONNECT ended");
                    }
                },
            }
        };
        streams::run(
            || session.accept_bi(),
            session.incoming_capacity(),
            shutdown,
            |stream: H3WebTransportBiStream, ended| {
                serve_stream(
                    stream,
                    Arc::clone(&server),
                    Arc::clone(&config),
                    crate::types::client::ChatAuthentication {
                        user: user.user.clone(),
                        session: user.session.clone(),
                    },
                    streams::wait_end(ended),
                )
            },
        )
        .await;
        Ok(())
    }
}

async fn serve_stream<S>(
    stream: S,
    server: Arc<ChatServer>,
    chat_config: Arc<ChatConfig>,
    authenticated: crate::types::client::ChatAuthentication,
    shutdown: impl std::future::Future<Output = ()>,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let crate::types::client::ChatAuthentication {
        user,
        session: auth_session,
    } = authenticated;
    let max_frame = chat_config.max_message_size;
    // Preserve the existing outbound bound; input overlaps the handler by
    // one queued message and one pending message, independently of output.
    let (client_tx, client_rx) = mpsc::channel::<ServerMessage>(max_frame);
    let (events, mut happened) = mpsc::channel(1);

    let client = ChatClient::new(user.clone(), client_tx.clone())
        .with_session(auth_session);
    if !client.auth_valid() {
        return;
    }
    let connection_id = client.connection_id();
    let handling = async {
        server.connect_client(client.clone()).await;

        #[cfg(feature = "metrics")]
        CHAT_METRICS.connect();

        let unread_summary = server
            .get_unread_summary(user.id())
            .await
            .unwrap_or_else(|error| {
                warn!(username = user.username(), %error, "failed to fetch unread counts");
                UnreadSummary {
                    private_messages_count: 0,
                    room_messages: vec![],
                }
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

        debug!(
            username = %user.username(),
            connection_id = %connection_id,
            "starting WebTransport chat stream"
        );

        while let Some(msg) = happened.recv().await {
            if !client.auth_valid() {
                break;
            }
            #[cfg(feature = "metrics")]
            CHAT_METRICS.message_received();
            // Do not cancel accepted application work on listener/session drain.
            handle_client_message(server.as_ref(), &client, msg).await;
        }
        // Wake the driver if dispatch ends before peer input or shutdown.
        drop(happened);

        #[cfg(feature = "metrics")]
        CHAT_METRICS.disconnect();

        server.disconnect_client(connection_id).await;
        debug!(
            username = %user.username(),
            connection_id = %connection_id,
            "WebTransport chat stream closed"
        );
    };
    let driving =
        io::drive(stream, client_rx, events, max_frame, shutdown, || {
            client.auth_valid()
        });
    tokio::join!(handling, driving);
}
