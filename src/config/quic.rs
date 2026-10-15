//! Raw chat QUIC is not HTTP/3. Reuse transport knobs, not an HTTP listener.

use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::time::Duration;

use claviron::edge::Host;
use claviron::helpers::de::{
    deserialize_duration, deserialize_optional_duration, one_or_many,
};
use claviron::http::config::QuicConfig;
use claviron::http::config::acl::AccessLimiter;
use claviron::orbit::placement::{Dispatch, DispatchPolicy, Scope};
use serde::Deserialize;

#[derive(Clone, Default, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ChatQuicConfig {
    /// No listener declaration means no raw QUIC socket or runnable admission.
    pub listener: Option<ChatQuicListenerConfig>,
}

#[derive(Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ChatQuicListenerConfig {
    pub listen: SocketAddr,
    /// Actual TLS hostnames for raw QUIC, not edge vhost keys.
    #[serde(deserialize_with = "one_or_many", alias = "host")]
    pub hosts: Vec<Host>,
    #[serde(default)]
    pub scope: Scope,
    #[serde(default)]
    pub dispatch: Dispatch,
    pub dispatch_policy: Option<DispatchPolicy>,
    pub acl: Option<AccessLimiter>,
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
        if let Some(listener) = &self.listener {
            listener.validate()?;
        }
        Ok(())
    }
}

impl ChatQuicListenerConfig {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.listen.port() == 0 {
            return Err("quic.listener.listen requires a nonzero UDP port");
        }
        if self.scope != Scope::Worker {
            return Err(
                "quic.listener.scope must be workers; chat fanout runs in workers",
            );
        }
        self.dispatch.validate_policy(self.dispatch_policy).map_err(|_| {
            "quic.listener dispatch one/pool requires a valid dispatch_policy"
        })?;
        if self.hosts.is_empty() {
            return Err("quic.listener.hosts requires concrete TLS hostnames");
        }
        for host in &self.hosts {
            let name = host.to_string();
            if host.is_ip() || name.contains(['*', ':']) {
                return Err(
                    "quic.listener.hosts requires DNS hostnames without wildcards or ports",
                );
            }
        }
        if self.acquire_timeout.is_zero() || self.handshake_timeout.is_zero() {
            return Err(
                "quic.listener admission and handshake timeouts must be positive",
            );
        }
        if self.transport.max_concurrent_bidi_streams == 0
            || self.transport.max_concurrent_uni_streams == 0
        {
            return Err("quic.listener stream limits must be positive");
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
#[path = "../../tests/quic_config.rs"]
mod tests;
