use std::sync::Arc;

use async_trait::async_trait;
use claviron::edge::{
    H1UpgradeAccept, H1UpgradeAuthenticated, H1UpgradeContext,
    H1UpgradeHandler, H1UpgradeReject, H1UpgradeResult, is_h1_header_upgrade,
    websocket_response_headers,
};
use claviron::runtime::AppState;
use claviron::runtime::registry::Result as RegistryResult;
use http::Request;
use session::handle_websocket;
use tracing::warn;

use super::session;
use crate::config::ChatConfigProvider;
use crate::svc::web::auth::{extract_user_from_protocol, websocket_guard_name};
use crate::svc::web::validator::OriginValidator;

pub(crate) struct ChatWsUpgradeHandler {
    config_provider: Arc<ChatConfigProvider>,
}

impl ChatWsUpgradeHandler {
    pub(crate) fn new(config_provider: Arc<ChatConfigProvider>) -> Self {
        Self { config_provider }
    }

    fn origin_validator(&self) -> Option<Arc<OriginValidator>> {
        let cfg = self.config_provider.current();
        if cfg.allowed_origins.is_empty() {
            return None;
        }
        Some(Arc::new(OriginValidator::new(&cfg.allowed_origins)))
    }
}

#[async_trait]
impl H1UpgradeHandler for ChatWsUpgradeHandler {
    fn name(&self) -> &'static str {
        "chat_ws_upgrade"
    }

    fn is_upgrade(
        &self,
        req: &Request<axum::body::Body>,
    ) -> bool {
        is_h1_header_upgrade(req, "websocket")
    }

    async fn accept(
        &self,
        state: &AppState,
        ctx: &H1UpgradeContext,
    ) -> H1UpgradeResult {
        let cfg = self.config_provider.current();
        validate_origin(ctx, self.origin_validator().as_deref())?;
        let protocol = extract_protocol_from_headers(&ctx.headers)?;
        let requested_guard = websocket_guard_name(&protocol);
        let (user, ws_protocol) =
            extract_user_from_protocol(protocol, state, &cfg.auth)
                .await
                .map_err(|err| {
                    warn!(
                        requested_guard,
                        enabled_guards = ?cfg.auth.enabled_guards(),
                        default_guard = ?cfg.auth.default_guard,
                        reason = %err,
                        "WebSocket authentication rejected"
                    );
                    H1UpgradeReject::unauthorized(err.to_string())
                })?;
        let headers =
            websocket_response_headers(ctx, Some(ws_protocol.as_str()))
                .map_err(|err| H1UpgradeReject::bad_request(err.to_string()))?;
        Ok(H1UpgradeAccept::authenticated(headers, user))
    }

    async fn handle_upgraded(
        &self,
        state: AppState,
        ctx: H1UpgradeContext,
        authenticated: Option<H1UpgradeAuthenticated>,
        upgraded: hyper::upgrade::Upgraded,
    ) -> RegistryResult<()> {
        drop(ctx); // Do not retain credential headers for the connection lifetime.
        let user = authenticated
            .ok_or_else(|| {
                claviron::runtime::registry::Error::msg(
                    "chat websocket auth missing",
                )
            })?
            .take::<crate::types::client::ChatAuthentication>()?;
        handle_websocket(upgraded, state, user).await.map_err(|err| {
            claviron::runtime::registry::Error::recoverable(err.to_string())
        })
    }
}

fn validate_origin(
    ctx: &H1UpgradeContext,
    origin_validator: Option<&OriginValidator>,
) -> Result<(), H1UpgradeReject> {
    let Some(v) = origin_validator else {
        return Ok(());
    };
    let origin = ctx.headers.get("origin").and_then(|h| h.to_str().ok());
    match origin {
        Some(origin_value) if v.is_allowed(origin_value) => Ok(()),
        Some(_) => Err(H1UpgradeReject::forbidden("websocket origin rejected")),
        None => Err(H1UpgradeReject::forbidden("websocket origin missing")),
    }
}

fn extract_protocol_from_headers(
    headers: &http::HeaderMap
) -> Result<String, H1UpgradeReject> {
    let protocol = headers
        .get(http::header::SEC_WEBSOCKET_PROTOCOL)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| {
            H1UpgradeReject::bad_request("missing auth signatures")
        })?;
    Ok(protocol.to_string())
}
