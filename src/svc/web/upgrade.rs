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
use crate::config::{ChatAuthGuard, ChatConfigProvider};
use crate::svc::web::auth::{
    extract_session_user, extract_user_from_protocol, websocket_guard_name,
};
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

    async fn authenticate(
        &self,
        state: &AppState,
        host: &claviron::edge::Host,
        headers: &http::HeaderMap,
    ) -> Result<
        (crate::types::client::ChatAuthentication, Option<String>),
        H1UpgradeReject,
    > {
        let cfg = self.config_provider.current();
        if cfg.auth.guard_enabled(ChatAuthGuard::Session) {
            let user = extract_session_user(
                state,
                host.to_cow_str().as_ref(),
                headers,
                &cfg,
            )
            .await
            .map_err(|status| match status {
                http::StatusCode::FORBIDDEN => {
                    H1UpgradeReject::forbidden("chat origin rejected")
                }
                http::StatusCode::UNAUTHORIZED => {
                    H1UpgradeReject::unauthorized("chat session required")
                }
                _ => H1UpgradeReject::service_unavailable(
                    "chat session verification unavailable",
                ),
            })?;
            return Ok((user, None));
        }
        validate_origin(headers, self.origin_validator().as_deref())?;
        let protocol = extract_protocol_from_headers(headers)?;
        let requested_guard = websocket_guard_name(&protocol);
        extract_user_from_protocol(protocol, state, &cfg.auth)
            .await
            .map(|(user, protocol)| (user, Some(protocol)))
            .map_err(|err| {
                warn!(requested_guard,
                    enabled_guards = ?cfg.auth.enabled_guards(),
                    default_guard = ?cfg.auth.default_guard,
                    reason = %err, "WebSocket authentication rejected");
                H1UpgradeReject::unauthorized("chat credential rejected")
            })
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
        let (user, ws_protocol) =
            self.authenticate(state, &ctx.host, &ctx.headers).await?;
        let headers =
            websocket_response_headers(ctx, ws_protocol.as_deref())
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
        let sessions = ctx.sessions.clone();
        drop(ctx); // Do not retain credential headers for the connection lifetime.
        let user = authenticated
            .ok_or_else(|| {
                claviron::runtime::registry::Error::msg(
                    "chat websocket auth missing",
                )
            })?
            .take::<crate::types::client::ChatAuthentication>()?;
        handle_websocket(
            upgraded,
            state,
            user,
            sessions.wait_graceful_shutdown(),
        )
        .await
        .map_err(|err| {
            claviron::runtime::registry::Error::recoverable(err.to_string())
        })
    }
}

/// RFC 8441 shares the H1 authentication and MessagePack WebSocket loop.
#[async_trait]
impl claviron::edge::H2ConnectHandler for ChatWsUpgradeHandler {
    fn name(&self) -> &'static str {
        "chat_websocket_h2"
    }

    async fn accept(
        &self,
        state: &AppState,
        ctx: &claviron::edge::AuthContext,
    ) -> claviron::edge::H2ConnectResult {
        if ctx
            .headers
            .get(http::header::SEC_WEBSOCKET_VERSION)
            .and_then(|value| value.to_str().ok())
            != Some("13")
        {
            return Err(claviron::edge::H2ConnectReject::bad_request());
        }
        let accepted =
            self.authenticate(state, &ctx.host, &ctx.headers).await.map_err(
                |error| claviron::edge::H2ConnectReject::new(error.status()),
            )?;
        Ok(claviron::edge::H2ConnectAccept::authenticated(accepted))
    }

    async fn handle(
        &self,
        state: AppState,
        ctx: claviron::edge::AuthContext,
        authenticated: Option<claviron::edge::AnyAuthenticated>,
        request: Request<claviron::http::RouteBody>,
    ) -> RegistryResult<claviron::http::RouteResponse> {
        use claviron::http::session::SessionSpawner;
        use claviron::http::{empty_body, err_status};
        use http::StatusCode;
        if !request.extensions().get::<hyper::ext::Protocol>().is_some_and(
            |protocol| protocol.as_str().eq_ignore_ascii_case("websocket"),
        ) {
            return Ok(err_status(StatusCode::BAD_REQUEST));
        }
        let (user, protocol) = authenticated
            .ok_or_else(|| {
                claviron::registry::Error::msg("chat H2 WebSocket auth missing")
            })?
            .take::<(crate::types::client::ChatAuthentication, Option<String>)>(
            )?;
        let sessions =
            request.extensions().get::<SessionSpawner>().cloned().ok_or_else(
                || {
                    claviron::registry::Error::msg(
                        "chat H2 WebSocket session owner missing",
                    )
                },
            )?;
        drop(ctx);
        let owner = sessions.clone();
        let submitted = sessions.spawn(async move {
            let upgraded = tokio::select! {
                biased;
                _ = owner.wait_graceful_shutdown() => return,
                result = hyper::upgrade::on(request) => match result {
                    Ok(upgraded) => upgraded,
                    Err(error) => {
                        tracing::debug!(%error, "chat H2 WebSocket upgrade failed");
                        return;
                    }
                }
            };
            if let Err(error) = handle_websocket(upgraded, state, user, owner.wait_graceful_shutdown()).await {
                tracing::debug!(%error, "chat H2 WebSocket ended with an error");
            }
        });
        if submitted.is_err() {
            return Ok(err_status(StatusCode::SERVICE_UNAVAILABLE));
        }
        let mut response = http::Response::builder().status(StatusCode::OK);
        if let Some(protocol) = protocol {
            response =
                response.header(http::header::SEC_WEBSOCKET_PROTOCOL, protocol);
        }
        response
            .body(empty_body())
            .map_err(|error| claviron::registry::Error::msg(error.to_string()))
    }
}

fn validate_origin(
    headers: &http::HeaderMap,
    origin_validator: Option<&OriginValidator>,
) -> Result<(), H1UpgradeReject> {
    let Some(v) = origin_validator else {
        return Ok(());
    };
    let origin = headers.get("origin").and_then(|h| h.to_str().ok());
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
