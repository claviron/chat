use std::sync::Arc;
use std::time::Duration;

use claviron::edge::Host;
use claviron::helpers::de::{deserialize_duration, one_or_many};
use serde::Deserialize;

use super::{ChatAuthConfig, ChatAuthGuard, join_route_path};

/// Opt-in Routable contribution. The embedding Edge owns the HTTP listener.
#[derive(Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChatEmbedConfig {
    #[serde(deserialize_with = "one_or_many", alias = "vhost")]
    pub vhosts: Vec<Host>,
    #[serde(default = "super::default_path")]
    pub path: String,
    #[serde(default = "super::default_websocket_path")]
    pub websocket_path: String,
    #[serde(default = "super::default_h2_connect_path")]
    pub h2_connect_path: String,
    #[serde(default = "super::default_web_transport_path")]
    pub web_transport_path: String,
    #[serde(default)]
    pub auth: ChatAuthConfig,
    /// Session auth requires an explicit allowlist on every web entrance.
    #[serde(default)]
    pub allowed_origins: Vec<String>,
    /// WebSocket liveness only; H2/WT connection policy belongs to Edge.
    #[serde(
        default = "super::default_ping_interval",
        deserialize_with = "deserialize_duration"
    )]
    pub ping_interval: Duration,
    #[serde(
        default = "super::default_idle_timeout",
        deserialize_with = "deserialize_duration"
    )]
    pub idle_timeout: Duration,
    #[serde(
        default,
        deserialize_with = "claviron::helpers::de::deserialize_optional_duration"
    )]
    pub graceful_timeout: Option<Duration>,
    #[serde(
        default = "super::default_acquire_timeout",
        deserialize_with = "deserialize_duration"
    )]
    pub acquire_timeout: Duration,
}

impl ChatEmbedConfig {
    pub(crate) fn websocket_route_path(&self) -> Arc<str> {
        join_route_path(&self.path, &self.websocket_path)
    }

    pub(crate) fn h2_connect_route_path(&self) -> Arc<str> {
        join_route_path(&self.path, &self.h2_connect_path)
    }

    pub(crate) fn web_transport_route_path(&self) -> Arc<str> {
        join_route_path(&self.path, &self.web_transport_path)
    }

    pub(crate) fn validate(&self) -> Result<(), &'static str> {
        if self.vhosts.is_empty() {
            return Err("embed.vhosts must not be empty");
        }
        if self.ping_interval.is_zero()
            || self.idle_timeout <= self.ping_interval
        {
            return Err(
                "embed requires positive ping_interval shorter than idle_timeout",
            );
        }
        if self.auth.guard_enabled(ChatAuthGuard::Session) {
            if self.auth.enabled_guards().len() != 1 {
                return Err(
                    "embed.auth session cannot fall back to other guards",
                );
            }
            if self.allowed_origins.is_empty() {
                return Err(
                    "embed.auth session requires embed.allowed_origins",
                );
            }
        }
        Ok(())
    }
}
