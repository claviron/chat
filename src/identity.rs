use std::sync::Arc;

use async_trait::async_trait;
use claviron::auth::Authenticated;
use thiserror::Error;

/// Application-owned session admission for same-origin chat transports.
/// Cookie names, guard selection and identity storage remain with the host.
#[async_trait]
pub trait SessionAuthenticator: Send + Sync + 'static {
    async fn authenticate_headers(
        &self,
        vhost: &str,
        headers: &http::HeaderMap,
    ) -> Result<claviron::auth::orbit::AuthenticatedSession, http::StatusCode>;
}

/// Failure returned by an application-owned avatar URL resolver.
#[derive(Debug, Error)]
#[error("avatar URL provider error: {0}")]
pub struct AvatarUrlError(String);

impl AvatarUrlError {
    pub fn msg(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

/// Optional presentation capability used while chat creates a user snapshot.
///
/// Implementations own storage, hash/MIME handling, CDN selection, and URL
/// policy. Chat receives only the final URL and falls back to a name-based
/// avatar when the capability is absent or returns `None`.
#[async_trait]
pub trait AvatarUrlProvider: Send + Sync + 'static {
    async fn avatar_url(
        &self,
        user: &dyn Authenticated,
    ) -> Result<Option<Arc<str>>, AvatarUrlError>;
}
