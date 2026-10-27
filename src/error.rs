use thiserror::Error;

/// Crate-local error type for the chat service's config / boot path.
///
/// Mirrors `claviron-pubsub` / `claviron-grpc` shape — each capability
/// crate owns its error surface so consumers see a consistent
/// "owned-error-per-crate" pattern. Domain-specific errors (`AuthError`) are kept separate below.
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("chat io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("chat config error: {0}")]
    Yaml(#[from] serde_yaml::Error),

    // #[error("chat msg decode error: {0}")]
    // MsgDecode(#[from] rmp_serde::decode::Error),
    #[error("chat msg encode error: {0}")]
    MsgEncode(#[from] rmp_serde::encode::Error),

    #[error("{0}")]
    Other(String),
}

impl Error {
    pub fn msg(msg: impl Into<String>) -> Self {
        Error::Other(msg.into())
    }
}

impl From<&str> for Error {
    fn from(s: &str) -> Self {
        Error::Other(s.to_string())
    }
}

impl From<String> for Error {
    fn from(s: String) -> Self {
        Error::Other(s)
    }
}

impl From<claviron::runtime::registry::Error> for Error {
    fn from(error: claviron::runtime::registry::Error) -> Self {
        Error::Other(error.to_string())
    }
}

#[derive(Error, Debug)]
pub enum AuthError {
    #[error("internal error: {0}")]
    Internal(String),
    #[error(transparent)]
    Orbit(#[from] claviron::auth::session::AuthError),
}

impl From<String> for AuthError {
    fn from(s: String) -> Self {
        Self::Internal(s)
    }
}

impl From<&str> for AuthError {
    fn from(s: &str) -> Self {
        Self::Internal(s.into())
    }
}
