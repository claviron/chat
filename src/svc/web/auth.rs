use claviron::cache::{CacheProvider, CacheValue};
use claviron::helpers::base64::base64_decode;
use claviron::runtime::AppState;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::ChatServer;
use crate::config::{
    ChatAuthConfig, ChatAuthGuard, OrbitChatAuthConfig, TicketChatAuthConfig,
};
use crate::error::AuthError;
use crate::models::users::ChatUser;
use crate::types::client::ChatAuthentication;

/// Same-origin session admission is exclusive: invalid cookies never fall
/// back to tickets or a separately configured token authority.
pub(crate) async fn extract_session_user(
    state: &AppState,
    vhost: &str,
    headers: &http::HeaderMap,
    config: &crate::config::ChatConfig,
) -> Result<ChatAuthentication, http::StatusCode> {
    let origin =
        headers.get(http::header::ORIGIN).and_then(|value| value.to_str().ok());
    let validator =
        super::validator::OriginValidator::new(&config.allowed_origins);
    if !origin.is_some_and(|origin| validator.is_allowed(origin)) {
        return Err(http::StatusCode::FORBIDDEN);
    }
    let chat = state
        .registry_ref()
        .resolve::<ChatServer>()
        .ok_or(http::StatusCode::SERVICE_UNAVAILABLE)?;
    chat.authenticate_session(vhost, headers).await
}

/// Extract authenticated user from the already-owned WebSocket subprotocol.
pub(crate) async fn extract_user_from_protocol(
    protocol_str: String,
    state: &AppState,
    auth: &ChatAuthConfig,
) -> Result<(ChatAuthentication, String), AuthError> {
    let guard = websocket_guard(&protocol_str)?;
    ensure_guard_enabled(auth, guard)?;
    match guard {
        ChatAuthGuard::Orbit => {
            let user = extract_orbit_user_from_protocol(
                &protocol_str,
                state,
                &auth.orbit,
            )
            .await?;
            Ok((user, protocol_str))
        }
        ChatAuthGuard::Ticket => {
            let user = extract_ticket_user_from_protocol(
                &protocol_str,
                state,
                &auth.ticket,
            )
            .await?;
            Ok((ChatAuthentication { user, session: None }, protocol_str))
        }
        ChatAuthGuard::Session => {
            Err("session auth requires request headers".into())
        }
    }
}

pub(crate) async fn extract_user_from_authorization(
    authorization: &str,
    state: &AppState,
    auth: &ChatAuthConfig,
) -> Result<ChatAuthentication, AuthError> {
    let (guard, credential) =
        authorization_guard(authorization, auth.default_guard)?;
    ensure_guard_enabled(auth, guard)?;
    match guard {
        ChatAuthGuard::Orbit => {
            extract_orbit_user_from_bearer(credential, state, &auth.orbit).await
        }
        ChatAuthGuard::Ticket => {
            extract_ticket_user_from_bearer(credential, state, &auth.ticket)
                .await
                .map(|user| ChatAuthentication { user, session: None })
        }
        ChatAuthGuard::Session => {
            Err("session auth requires request headers".into())
        }
    }
}

fn websocket_guard(protocol: &str) -> Result<ChatAuthGuard, AuthError> {
    if protocol.starts_with("bearer.") {
        Ok(ChatAuthGuard::Orbit)
    } else if protocol.starts_with("ticket.") {
        Ok(ChatAuthGuard::Ticket)
    } else {
        Err("unsupported websocket auth subprotocol".into())
    }
}

pub(crate) fn websocket_guard_name(protocol: &str) -> &'static str {
    if protocol.starts_with("bearer.") {
        "orbit"
    } else if protocol.starts_with("ticket.") {
        "ticket"
    } else {
        "unknown"
    }
}

fn authorization_guard(
    authorization: &str,
    default_guard: ChatAuthGuard,
) -> Result<(ChatAuthGuard, &str), AuthError> {
    let authorization = authorization.trim();
    if authorization.is_empty() {
        return Err("empty authorization credential".into());
    }
    let Some((scheme, credential)) = authorization.split_once(' ') else {
        return Ok((default_guard, authorization));
    };
    let credential = credential.trim();
    if credential.is_empty() {
        return Err("empty authorization credential".into());
    }
    if scheme.eq_ignore_ascii_case("ticket") {
        Ok((ChatAuthGuard::Ticket, credential))
    } else if scheme.eq_ignore_ascii_case("bearer") {
        Ok((default_guard, credential))
    } else {
        Err("unsupported authorization scheme".into())
    }
}

fn ensure_guard_enabled(
    auth: &ChatAuthConfig,
    guard: ChatAuthGuard,
) -> Result<(), AuthError> {
    if auth.guard_enabled(guard) {
        Ok(())
    } else {
        Err(format!("chat auth guard '{guard:?}' is disabled").into())
    }
}

async fn extract_orbit_user_from_protocol(
    protocol_str: &str,
    state: &AppState,
    cfg: &OrbitChatAuthConfig,
) -> Result<ChatAuthentication, AuthError> {
    if !cfg.websocket_subprotocol {
        return Err("orbit websocket subprotocol auth is disabled".into());
    }
    let encoded = protocol_str
        .strip_prefix("bearer.")
        .ok_or("subprotocol must start with 'bearer.'")?;
    validate_encoded_token(encoded, state, cfg).await
}

async fn extract_orbit_user_from_bearer(
    token: &str,
    state: &AppState,
    cfg: &OrbitChatAuthConfig,
) -> Result<ChatAuthentication, AuthError> {
    if !cfg.bearer_header {
        return Err("orbit bearer header auth is disabled".into());
    }
    let token = token
        .strip_prefix("Bearer ")
        .or_else(|| token.strip_prefix("bearer "))
        .unwrap_or(token);
    validate_orbit_and_get_user(token, state, &cfg.audience).await
}

async fn validate_encoded_token(
    encoded: &str,
    state: &AppState,
    cfg: &OrbitChatAuthConfig,
) -> Result<ChatAuthentication, AuthError> {
    match base64_decode(encoded) {
        Ok(token_bytes) => match String::from_utf8(token_bytes) {
            Ok(token) => {
                let user =
                    validate_orbit_and_get_user(&token, state, &cfg.audience)
                        .await?;
                Ok(user)
            }
            Err(e) => Err(format!(" UTF-8 decode failed {}", e).into()),
        },
        Err(e) => Err(format!("Base64 decode failed {}", e).into()),
    }
}

async fn extract_ticket_user_from_protocol(
    protocol_str: &str,
    state: &AppState,
    cfg: &TicketChatAuthConfig,
) -> Result<ChatUser, AuthError> {
    if !cfg.websocket_subprotocol {
        return Err("ticket websocket subprotocol auth is disabled".into());
    }
    let encoded = protocol_str
        .strip_prefix("ticket.")
        .ok_or("subprotocol must start with 'ticket.'")?;
    let ticket = decode_ticket_subprotocol(encoded)?;
    validate_ticket_and_get_user(&ticket, state, cfg).await
}

async fn extract_ticket_user_from_bearer(
    token: &str,
    state: &AppState,
    cfg: &TicketChatAuthConfig,
) -> Result<ChatUser, AuthError> {
    if !cfg.bearer_header {
        return Err("ticket bearer header auth is disabled".into());
    }
    let ticket = token
        .strip_prefix("Bearer ")
        .or_else(|| token.strip_prefix("bearer "))
        .or_else(|| token.strip_prefix("Ticket "))
        .or_else(|| token.strip_prefix("ticket "))
        .unwrap_or(token)
        .trim();
    validate_ticket_and_get_user(ticket, state, cfg).await
}

fn decode_ticket_subprotocol(encoded: &str) -> Result<String, AuthError> {
    let bytes = base64_decode(encoded)
        .map_err(|e| format!("Base64 decode failed {}", e))?;
    String::from_utf8(bytes)
        .map_err(|e| format!("UTF-8 decode failed {}", e).into())
}

async fn validate_ticket_and_get_user(
    ticket: &str,
    state: &AppState,
    cfg: &TicketChatAuthConfig,
) -> Result<ChatUser, AuthError> {
    if ticket.is_empty() {
        return Err("empty chat ticket".into());
    }
    let cache =
        state.registry_ref().resolve::<CacheProvider>().ok_or_else(|| {
            AuthError::Internal("cache provider not registered".to_string())
        })?;
    let store = cache.store(&cfg.cache_store).map_err(|_| {
        AuthError::Internal(format!(
            "chat ticket cache store '{}' is not registered",
            cfg.cache_store
        ))
    })?;
    let key = chat_ticket_key(cfg, ticket);
    let bytes = store
        .get(&key)
        .await
        .map_err(|err| {
            AuthError::Internal(format!("chat ticket cache read failed: {err}"))
        })?
        .ok_or("chat ticket not found or expired")?;
    if cfg.consume {
        store.forget(&key).await.map_err(|err| {
            AuthError::Internal(format!(
                "chat ticket cache consume failed: {err}"
            ))
        })?;
    }
    let payload = parse_ticket_payload(&bytes)?;
    if payload.v != cfg.version {
        return Err(
            format!("unsupported chat ticket version {}", payload.v).into()
        );
    }
    payload.into_chat_user()
}

fn chat_ticket_key(
    cfg: &TicketChatAuthConfig,
    ticket: &str,
) -> String {
    format!("{}{}", cfg.key_prefix, sha256_hex(ticket.as_bytes()))
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write;
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
}

fn parse_ticket_payload(bytes: &[u8]) -> Result<ChatTicketPayload, AuthError> {
    if let Ok(payload) = serde_json::from_slice(bytes) {
        return Ok(payload);
    }
    let value = CacheValue::decode(bytes)
        .map_err(|err| {
            AuthError::Internal(format!(
                "chat ticket payload decode failed: {err}"
            ))
        })?
        .to_json();
    serde_json::from_value(value).map_err(|err| {
        AuthError::Internal(format!(
            "chat ticket payload json shape is invalid: {err}"
        ))
    })
}

#[derive(Debug, Deserialize)]
struct ChatTicketPayload {
    v: u16,
    id: i32,
    uuid: Uuid,
    username: String,
    name: String,
    #[serde(default, alias = "avatar")]
    avatar_url: String,
}

impl ChatTicketPayload {
    fn into_chat_user(self) -> Result<ChatUser, AuthError> {
        if self.id <= 0 {
            return Err("chat ticket user id must be positive".into());
        }
        if self.username.trim().is_empty() {
            return Err("chat ticket username is empty".into());
        }
        let name = if self.name.trim().is_empty() {
            self.username.clone()
        } else {
            self.name
        };
        Ok(ChatUser::from_trusted_snapshot(
            self.id,
            self.uuid,
            self.username,
            name,
            self.avatar_url,
        ))
    }
}

/// Validate through the boot-bound capability; never bypass live session checks.
async fn validate_orbit_and_get_user(
    token: &str,
    state: &AppState,
    audience: &str,
) -> Result<ChatAuthentication, AuthError> {
    let chat = state
        .registry_ref()
        .resolve::<ChatServer>()
        .ok_or("chat server not registered")?;
    chat.authenticate_token(token, audience).await
}

#[cfg(test)]
#[path = "../../../tests/svc/web/auth.rs"]
mod tests;
