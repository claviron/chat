//! Raw chat QUIC is not HTTP/3. Reuse transport knobs, not an HTTP listener.

use std::num::NonZeroUsize;
use std::time::Duration;

use claviron::helpers::de::{
    deserialize_duration, deserialize_optional_duration,
};
use claviron::http::config::QuicConfig;
use serde::Deserialize;

#[derive(Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ChatQuicConfig {
    #[serde(default)]
    pub max_connections: Option<NonZeroUsize>,
    #[serde(default, deserialize_with = "deserialize_optional_duration")]
    pub graceful_timeout: Option<Duration>,
    #[serde(
        default = "super::default_acquire_timeout",
        deserialize_with = "deserialize_duration"
    )]
    pub acquire_timeout: Duration,
    #[serde(
        default = "default_handshake_timeout",
        deserialize_with = "deserialize_duration"
    )]
    pub handshake_timeout: Duration,
    #[serde(default)]
    pub transport: QuicConfig,
}

fn default_handshake_timeout() -> Duration {
    Duration::from_secs(5)
}

impl ChatQuicConfig {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.acquire_timeout.is_zero() || self.handshake_timeout.is_zero() {
            return Err(
                "quic admission and handshake timeouts must be positive",
            );
        }
        if self.transport.max_concurrent_bidi_streams == 0
            || self.transport.max_concurrent_uni_streams == 0
        {
            return Err("quic stream limits must be positive");
        }
        if self.transport.keep_alive_interval.is_none_or(|keep_alive| {
            keep_alive.is_zero() || keep_alive >= self.transport.idle_timeout
        }) {
            return Err(
                "raw chat QUIC requires positive keep_alive_interval shorter than idle_timeout",
            );
        }
        if self.transport.enable_early_data {
            return Err(
                "raw chat QUIC client authentication does not permit early data",
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn separates_listener_limits_from_quic_and_preserves_indefinite_grace() {
        let config: ChatQuicConfig = serde_yaml::from_str("max_connections: 10\ngraceful_timeout: null\nhandshake_timeout: 2s\ntransport: {keep_alive_interval: 10s, idle_timeout: 60s}").unwrap();
        assert_eq!(config.max_connections.unwrap().get(), 10);
        assert_eq!(config.handshake_timeout, Duration::from_secs(2));
        assert_eq!(config.graceful_timeout, None);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn rejects_http_fields_flat_transport_keys_and_invalid_limits() {
        for yaml in [
            "max_request_body_size: 1024",
            "listen: '127.0.0.1:443'",
            "max_connections: 0",
            "keep_alive_interval: 10s",
            "transport: {bogus: true}",
        ] {
            assert!(
                serde_yaml::from_str::<ChatQuicConfig>(yaml).is_err(),
                "{yaml}"
            );
        }
        for yaml in [
            "transport: {keep_alive_interval: 0s}",
            "transport: {keep_alive_interval: 60s, idle_timeout: 60s}",
            "transport: {keep_alive_interval: 10s, enable_early_data: true}",
        ] {
            assert!(
                serde_yaml::from_str::<ChatQuicConfig>(yaml)
                    .unwrap()
                    .validate()
                    .is_err()
            );
        }
    }
}
