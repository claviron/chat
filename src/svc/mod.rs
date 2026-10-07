#[cfg(feature = "metrics")]
pub mod metrics;
#[cfg(feature = "quic")]
mod quic;
pub mod rate_limit;
#[cfg(feature = "quic")]
pub(crate) mod shared;
mod web;

#[cfg(feature = "quic")]
pub use quic::ChatQuicService;
pub use web::ChatWebService;

pub const WELCOME_MESSAGE: &str =
    "Welcome, you are connected to Claviron svc server";
/// The minimum multiplier for idle_timeout vs ping_interval/keep_alive
/// idle_timeout must be GREATER than ping_interval (not equal)
/// Error if: idle_timeout <= ping_interval * MIN_IDLE_TIMEOUT_MULTIPLIER
#[cfg(feature = "quic")]
pub const MIN_IDLE_TIMEOUT_MULTIPLIER: u32 = 1;
/// Recommended multiplier for idle_timeout vs ping_interval/keep_alive
/// Allows missing 2 ping cycles before timeout
/// Warning if: idle_timeout < ping_interval * RECOMMENDED_IDLE_TIMEOUT_MULTIPLIER
#[cfg(feature = "quic")]
pub const RECOMMENDED_IDLE_TIMEOUT_MULTIPLIER: u32 = 3;
