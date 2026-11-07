//! The chat's WebSocket, on HTTP/1.1 and HTTP/2 alike: the offer says what is
//! being opened and carries it.

use std::sync::Arc;

use claviron::http::session::SessionSpawner;
use claviron::http::socket::{Carrier, Offer, SocketIo, SocketKind};
use claviron::http::{RouteResponse, ServiceResult, err_status};
use claviron::pipeline::{HostHandler, HostHandlerFuture, HostRequest};
use claviron::runtime::AppState;
use http::StatusCode;
use http::request::Parts;
use tracing::{debug, warn};

use super::session::handle_websocket;
use crate::config::{ChatAuthGuard, ChatConfigProvider, ChatEmbedConfig};
use crate::svc::web::auth::{
    extract_session_user, extract_user_from_protocol, websocket_guard_name,
};
use crate::svc::web::validator::OriginValidator;

/// One host's chat socket, prepared with the host.
pub(crate) struct ChatSockets {
    pub(crate) state: AppState,
    pub(crate) config_provider: Arc<ChatConfigProvider>,
    pub(crate) host: claviron::edge::Host,
    pub(crate) path: Arc<str>,
    /// The transport's `h1.upgrade`: what the connection's terms allow.
    pub(crate) h1_upgrade: bool,
}

impl HostHandler for ChatSockets {
    fn call(
        &self,
        request: HostRequest,
    ) -> HostHandlerFuture {
        let head = request.head();
        let offer = head
            .extensions
            .get::<Offer>()
            .filter(|offer| offer.info().kind == SocketKind::WebSocket)
            .filter(|_| head.uri.path() == &*self.path)
            .cloned();
        let Some(offer) = offer else {
            return Box::pin(async move { ServiceResult::Unhandled(request) });
        };
        if offer.info().carrier == Carrier::H1 && !self.h1_upgrade {
            return Box::pin(async {
                ServiceResult::Handled(err_status(StatusCode::FORBIDDEN))
            });
        }
        let (state, config_provider, host) = (
            self.state.clone(),
            Arc::clone(&self.config_provider),
            self.host.clone(),
        );
        Box::pin(async move {
            let (head, _) = request.into_request().into_parts();
            ServiceResult::Handled(
                open(state, config_provider, host, offer, head).await,
            )
        })
    }
}

async fn open(
    state: AppState,
    config_provider: Arc<ChatConfigProvider>,
    host: claviron::edge::Host,
    offer: Offer,
    head: Parts,
) -> RouteResponse {
    let (user, protocol) = match authenticate(
        &config_provider,
        &state,
        &host,
        &head.headers,
    )
    .await
    {
        Ok(accepted) => accepted,
        Err(status) => return err_status(status),
    };
    let listener = head.extensions.get::<SessionSpawner>().cloned();
    // Credential headers go no further than this; the session holds the user.
    drop(head);
    let Some(claim) = offer.claim() else {
        return err_status(StatusCode::BAD_REQUEST);
    };
    let mut proposal = claim.accept_with(move |io| async move {
        let SocketIo::Stream(upgraded) = io else {
            debug!("the chat has no use for this session's I/O");
            return;
        };
        let shutdown = async move {
            match listener {
                Some(listener) => listener.wait_graceful_shutdown().await,
                None => std::future::pending().await,
            }
        };
        if let Err(error) =
            handle_websocket(upgraded, state, user, shutdown).await
        {
            debug!(%error, "chat WebSocket ended with an error");
        }
    });
    if let Some(protocol) = protocol
        && let Ok(value) = http::HeaderValue::from_str(&protocol)
    {
        proposal
            .headers_mut()
            .insert(http::header::SEC_WEBSOCKET_PROTOCOL, value);
    }
    proposal
}

fn origin_validator(embed: &ChatEmbedConfig) -> Option<Arc<OriginValidator>> {
    if embed.allowed_origins.is_empty() {
        return None;
    }
    Some(Arc::new(OriginValidator::new(&embed.allowed_origins)))
}

async fn authenticate(
    config_provider: &ChatConfigProvider,
    state: &AppState,
    host: &claviron::edge::Host,
    headers: &http::HeaderMap,
) -> Result<
    (crate::types::client::ChatAuthentication, Option<String>),
    StatusCode,
> {
    let cfg = config_provider.current();
    let embed = cfg.embed.as_ref().ok_or(StatusCode::SERVICE_UNAVAILABLE)?;
    if embed.auth.guard_enabled(ChatAuthGuard::Session) {
        let user = extract_session_user(
            state,
            host.to_cow_str().as_ref(),
            headers,
            embed,
        )
        .await
        .map_err(|status| match status {
            StatusCode::FORBIDDEN | StatusCode::UNAUTHORIZED => status,
            _ => StatusCode::SERVICE_UNAVAILABLE,
        })?;
        return Ok((user, None));
    }
    validate_origin(headers, origin_validator(embed).as_deref())?;
    let protocol = extract_protocol_from_headers(headers)?;
    let requested_guard = websocket_guard_name(&protocol);
    extract_user_from_protocol(protocol, state, &embed.auth)
        .await
        .map(|(user, protocol)| (user, Some(protocol)))
        .map_err(|err| {
            warn!(requested_guard,
                enabled_guards = ?embed.auth.enabled_guards(),
                default_guard = ?embed.auth.default_guard,
                reason = %err, "WebSocket authentication rejected");
            StatusCode::UNAUTHORIZED
        })
}

fn validate_origin(
    headers: &http::HeaderMap,
    origin_validator: Option<&OriginValidator>,
) -> Result<(), StatusCode> {
    let Some(validator) = origin_validator else {
        return Ok(());
    };
    match headers.get("origin").and_then(|origin| origin.to_str().ok()) {
        Some(origin) if validator.is_allowed(origin) => Ok(()),
        _ => Err(StatusCode::FORBIDDEN),
    }
}

fn extract_protocol_from_headers(
    headers: &http::HeaderMap
) -> Result<String, StatusCode> {
    headers
        .get(http::header::SEC_WEBSOCKET_PROTOCOL)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
        .ok_or(StatusCode::BAD_REQUEST)
}
