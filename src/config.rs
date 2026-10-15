use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use async_trait::async_trait;
use claviron::edge::Host;
use claviron::helpers::de::{
    deserialize_bind_address, deserialize_duration, one_or_many,
};
use claviron::runtime::congif::{
    self, ConfigFile, ConfigObserver, Configurable,
};
use claviron::runtime::registry::{Component, Reloadable, priority};
use claviron::runtime::{AppState, Register};
use claviron::tls::auth::verifier::PolicyConfig;
use serde::Deserialize;
use tracing::info;

use crate::svc::rate_limit::ChatRateLimitConfig;

mod quic;
use quic::ChatQuicConfig;

/// What the chat configuration file is called in the config directory.
pub const CONFIG_FILE: &str = "chat.yaml";

/// Single owner of `chat.yaml`, read by Congif: prepared before boot and in
/// the reload batch, applied here through `configure`, and followed through
/// `ConfigObserver` when the source is watched. Every chat runtime
/// (`ChatServer`, `ChatWebService`, `ChatQuicService`) is handed this at
/// registration and reads the snapshot via `current()` — nobody else does
/// file I/O, and all of them see the same atomic swap.
pub struct ChatConfigProvider {
    config_path: PathBuf,
    snapshot: ArcSwap<ChatConfig>,
}

impl ChatConfigProvider {
    pub fn new(state: &AppState) -> Self {
        Self::empty(state.config_dir().join(CONFIG_FILE))
    }

    pub fn empty(config_path: PathBuf) -> Self {
        Self {
            config_path,
            snapshot: ArcSwap::from_pointee(ChatConfig::default()),
        }
    }

    /// Current chat config snapshot. Always returns — Congif applies
    /// `chat.yaml` (or the defaults, without the file) before any boot.
    pub fn current(&self) -> Arc<ChatConfig> {
        self.snapshot.load_full()
    }

    fn apply(
        &self,
        config: &ChatConfig,
    ) {
        self.snapshot.store(Arc::new(config.clone()));
        info!("📦 ChatConfigProvider applied {}", self.config_path.display());
    }
}

impl Register<AppState> for ChatConfigProvider {
    fn register(state: &AppState) -> claviron::runtime::registry::Result<()> {
        state.bind_config_environment::<Self>()?;
        state.registry_ref().register(Arc::new(Self::new(state)))?;
        Ok(())
    }
}

#[async_trait]
impl Component<AppState> for ChatConfigProvider {
    type Deps = ();

    fn name(&self) -> &'static str {
        "ChatConfigProvider"
    }

    fn as_configurable(&self) -> Option<&dyn congif::ConfigRegistration> {
        Some(self)
    }

    fn as_reloadable(&self) -> Option<&dyn Reloadable<AppState>> {
        Some(self)
    }
}

#[async_trait]
impl Configurable for ChatConfigProvider {
    type Config = ChatConfig;

    fn source(&self) -> ConfigFile {
        ConfigFile::yaml(&self.config_path)
    }

    /// Without the file chat is off; `enabled: true` is how it is opted in.
    fn absent_source(&self) -> Option<ChatConfig> {
        Some(ChatConfig::default())
    }

    fn validate_config(
        &self,
        config: &ChatConfig,
    ) -> congif::Result<()> {
        config.validate().map_err(congif::Error::invalid)
    }

    async fn configure(
        &self,
        config: &ChatConfig,
    ) -> congif::Result<()> {
        self.apply(config);
        Ok(())
    }

    fn as_config_observer(
        &self
    ) -> Option<&dyn ConfigObserver<Config = Self::Config>> {
        Some(self)
    }
}

#[async_trait]
impl ConfigObserver for ChatConfigProvider {
    async fn config_changed(
        &self,
        config: &ChatConfig,
    ) -> congif::Result<()> {
        self.apply(config);
        Ok(())
    }
}

#[async_trait]
impl Reloadable<AppState> for ChatConfigProvider {
    fn priority(&self) -> Option<u8> {
        Some(priority::EARLY)
    }

    async fn reload(
        &self,
        state: &AppState,
    ) -> claviron::runtime::registry::Result<()> {
        let config = state.config_reader().snapshot::<Self>()?;
        self.apply(&config);
        Ok(())
    }
}

/// Chat service configuration
#[derive(Clone, PartialEq, Deserialize)]
pub struct ChatConfig {
    /// Enable chat runtime support (default: false).
    #[serde(default)]
    pub enabled: bool,

    /// Raw QUIC demo/prototype listener bind address.
    #[serde(
        default = "default_bind_address",
        deserialize_with = "deserialize_bind_address"
    )]
    pub address: SocketAddr,

    /// Chat route base path. Transport endpoints are configured below and
    /// mounted under this path.
    #[serde(default = "default_path")]
    pub path: String,

    /// WebSocket endpoint mounted under `path` (default route: `/chat/ws`).
    #[serde(default = "default_websocket_path")]
    pub websocket_path: String,

    /// HTTP/2 CONNECT endpoint mounted under `path` (default route: `/chat/h2`).
    #[serde(default = "default_h2_connect_path")]
    pub h2_connect_path: String,

    /// WebTransport endpoint mounted under `path` (default route: `/chat/wt`).
    #[serde(default = "default_web_transport_path")]
    pub web_transport_path: String,

    /// Chat authentication guard configuration.
    #[serde(default)]
    pub auth: ChatAuthConfig,

    /// Resolved vhost keys that should receive chat bindings.
    #[serde(default, deserialize_with = "one_or_many", alias = "vhost")]
    vhosts: Vec<Host>,

    #[serde(default)]
    /// Certificate verifier for non-websocket clients
    ca_path: Option<PathBuf>,

    /// Custom Policy for tls auth
    /// Verifies Org/Unit/Client list
    policy: Option<PolicyConfig>,

    /// Raw QUIC demo/prototype listener transport settings.
    quic: Option<ChatQuicConfig>,

    /// WebSocket application ping interval (default: 10s). H2 CONNECT
    /// liveness is owned by the web/HTTP2 transport service, not chat.
    #[serde(
        default = "default_ping_interval",
        deserialize_with = "deserialize_duration"
    )]
    pub ping_interval: Duration,

    /// WebSocket idle timeout (default: 60s). H2 CONNECT does not use
    /// this to disconnect quiet readers.
    #[serde(
        default = "default_idle_timeout",
        deserialize_with = "deserialize_duration"
    )]
    pub idle_timeout: Duration,

    /// Maximum message size in bytes (default: 64KB)
    #[serde(default = "default_max_message_size")]
    pub max_message_size: usize,

    /// Maximum concurrent connections (default: 1000)
    #[serde(default = "default_max_connections")]
    pub max_connections: usize,

    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_duration"
    )]
    pub graceful_timeout: Option<Duration>,
    #[serde(
        default = "default_acquire_timeout",
        deserialize_with = "deserialize_duration"
    )]
    pub acquire_timeout: Duration,

    #[serde(default)]
    /// Allowed origins for WebSocket connections (supports regex)
    /// Empty = allow all origins (insecure, dev only!)
    /// Exact match: "https://app.example.com"
    /// Regex: "^https?://localhost:\\d+$" (any localhost port)
    /// Example: ["https://app.example.com", "^https?://localhost:\\d+$"]
    pub allowed_origins: Vec<String>,

    /// Rate limiting configuration for svc messages
    /// Prevents spam by limiting messages per connection per time window
    #[serde(default)]
    pub rate_limit: ChatRateLimitConfig,

    /// Worker presence replication and stale-record policy.
    #[serde(default)]
    pub presence: ChatPresenceConfig,

    /// Metrics reporting configuration
    #[serde(default)]
    pub metrics: Option<MetricsConfig>,
}

impl ChatConfig {
    pub(crate) fn quic_cfg(&self) -> Option<&ChatQuicConfig> {
        self.quic.as_ref()
    }

    pub fn policy_cfg(&self) -> Option<&PolicyConfig> {
        self.policy.as_ref()
    }

    pub fn ca_path(&self) -> Option<&std::path::Path> {
        self.ca_path.as_deref()
    }

    pub fn vhosts(&self) -> &[Host] {
        self.vhosts.as_slice()
    }

    pub(crate) fn metrics_interval(&self) -> Option<Duration> {
        self.metrics.as_ref().map(|m| m.interval)
    }

    pub(crate) fn websocket_route_path(&self) -> Arc<str> {
        join_route_path(&self.path, &self.websocket_path)
    }

    pub(crate) fn h2_connect_route_path(&self) -> Arc<str> {
        join_route_path(&self.path, &self.h2_connect_path)
    }

    pub(crate) fn web_transport_route_path(&self) -> Arc<str> {
        join_route_path(&self.path, &self.web_transport_path)
    }

    fn validate(&self) -> Result<(), &'static str> {
        if self.auth.guard_enabled(ChatAuthGuard::Session) {
            if self.auth.enabled_guards().len() != 1 {
                return Err(
                    "session chat auth cannot fall back to other guards",
                );
            }
            if self.allowed_origins.is_empty() {
                return Err("session chat auth requires allowed_origins");
            }
        }
        if let Some(quic) = &self.quic {
            quic.validate()?;
        }
        if self.presence.sync_interval.is_zero() {
            return Err("presence.sync_interval must be greater than zero");
        }
        let Some(minimum_stale_after) =
            self.presence.sync_interval.checked_mul(2)
        else {
            return Err("presence.sync_interval is too large");
        };
        if self.presence.stale_after < minimum_stale_after {
            return Err(
                "presence.stale_after must be at least twice presence.sync_interval",
            );
        }
        Ok(())
    }
}

impl Default for ChatConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            address: default_bind_address(),
            path: default_path(),
            websocket_path: default_websocket_path(),
            h2_connect_path: default_h2_connect_path(),
            web_transport_path: default_web_transport_path(),
            auth: ChatAuthConfig::default(),
            vhosts: Vec::new(),
            ca_path: None,
            policy: None,
            quic: None,
            ping_interval: default_ping_interval(),
            idle_timeout: default_idle_timeout(),
            max_message_size: default_max_message_size(),
            max_connections: default_max_connections(),
            graceful_timeout: None,
            acquire_timeout: default_acquire_timeout(),
            allowed_origins: Vec::new(),
            rate_limit: ChatRateLimitConfig::default(),
            presence: ChatPresenceConfig::default(),
            metrics: None,
        }
    }
}

fn default_bind_address() -> SocketAddr {
    "0.0.0.0:8443".parse().unwrap()
}

fn default_path() -> String {
    "/chat".to_string()
}

fn default_websocket_path() -> String {
    "/ws".to_string()
}

fn default_h2_connect_path() -> String {
    "/h2".to_string()
}

fn default_web_transport_path() -> String {
    "/wt".to_string()
}

fn join_route_path(
    base: &str,
    endpoint: &str,
) -> Arc<str> {
    let base = base.trim_matches('/');
    let endpoint = endpoint.trim_matches('/');
    match (base.is_empty(), endpoint.is_empty()) {
        (true, true) => Arc::from("/"),
        (true, false) => Arc::from(format!("/{endpoint}")),
        (false, true) => Arc::from(format!("/{base}")),
        (false, false) => Arc::from(format!("/{base}/{endpoint}")),
    }
}

fn default_ping_interval() -> Duration {
    Duration::from_secs(10)
}

fn default_idle_timeout() -> Duration {
    Duration::from_secs(60)
}

fn default_max_message_size() -> usize {
    64 * 1024 // 64KB
}

fn default_max_connections() -> usize {
    1_000
}

fn default_acquire_timeout() -> Duration {
    Duration::from_millis(100)
}

/// Orbit-backed worker presence refresh policy.
#[derive(Clone, PartialEq, Deserialize)]
pub struct ChatPresenceConfig {
    /// How often a worker republishes its current user and room counters.
    #[serde(
        default = "default_presence_sync_interval",
        deserialize_with = "deserialize_duration"
    )]
    pub sync_interval: Duration,

    /// Maximum age accepted for a remote worker's presence record.
    #[serde(
        default = "default_presence_stale_after",
        deserialize_with = "deserialize_duration"
    )]
    pub stale_after: Duration,
}

impl Default for ChatPresenceConfig {
    fn default() -> Self {
        Self {
            sync_interval: default_presence_sync_interval(),
            stale_after: default_presence_stale_after(),
        }
    }
}

fn default_presence_sync_interval() -> Duration {
    Duration::from_secs(5)
}

fn default_presence_stale_after() -> Duration {
    Duration::from_secs(15)
}

#[derive(Clone, PartialEq, Deserialize)]
pub struct ChatAuthConfig {
    /// Auth guards accepted by chat transports. An empty list preserves
    /// the legacy single-guard behavior and enables only `default_guard`.
    #[serde(default)]
    pub guards: Vec<ChatAuthGuard>,

    /// Guard used when the credential carrier is ambiguous. `guard` is
    /// retained as a deserialize alias for existing `chat.yaml` files.
    #[serde(default, rename = "default", alias = "guard")]
    pub default_guard: ChatAuthGuard,

    /// Orbit token guard settings.
    #[serde(default)]
    pub orbit: OrbitChatAuthConfig,

    /// Short-lived cache ticket guard settings.
    #[serde(default)]
    pub ticket: TicketChatAuthConfig,
}

impl Default for ChatAuthConfig {
    fn default() -> Self {
        Self {
            guards: vec![ChatAuthGuard::Orbit],
            default_guard: ChatAuthGuard::Orbit,
            orbit: OrbitChatAuthConfig::default(),
            ticket: TicketChatAuthConfig::default(),
        }
    }
}

impl ChatAuthConfig {
    pub(crate) fn guard_enabled(
        &self,
        guard: ChatAuthGuard,
    ) -> bool {
        if self.guards.is_empty() {
            self.default_guard == guard
        } else {
            self.guards.contains(&guard)
        }
    }

    pub(crate) fn enabled_guards(&self) -> Vec<ChatAuthGuard> {
        if self.guards.is_empty() {
            vec![self.default_guard]
        } else {
            self.guards.clone()
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ChatAuthGuard {
    #[default]
    Orbit,
    Ticket,
    Session,
}

#[derive(Clone, PartialEq, Deserialize)]
pub struct OrbitChatAuthConfig {
    /// Orbit token audience used by chat. Kept configurable so host apps can
    /// move away from starter defaults without code changes.
    #[serde(default = "default_orbit_audience")]
    pub audience: String,

    /// Allow WebSocket auth through `Sec-WebSocket-Protocol:
    /// bearer.<base64url-unpadded-token>`.
    #[serde(default = "default_true")]
    pub websocket_subprotocol: bool,

    /// Allow H2 CONNECT / WebTransport auth through `Authorization:
    /// Bearer <token>`.
    #[serde(default = "default_true")]
    pub bearer_header: bool,
}

impl Default for OrbitChatAuthConfig {
    fn default() -> Self {
        Self {
            audience: default_orbit_audience(),
            websocket_subprotocol: true,
            bearer_header: true,
        }
    }
}

fn default_orbit_audience() -> String {
    "urn:chat:svc".to_string()
}

#[derive(Clone, PartialEq, Deserialize)]
pub struct TicketChatAuthConfig {
    /// Cache store used for chat auth tickets. In production this is
    /// typically a Redis-backed `chat` store shared with the host app.
    #[serde(default = "default_ticket_cache_store")]
    pub cache_store: String,

    /// Store-local key prefix. The configured cache store may add its
    /// own driver prefix before this value.
    #[serde(default = "default_ticket_key_prefix")]
    pub key_prefix: String,

    /// Ticket payload version. Unknown versions are rejected so payload
    /// shape changes are explicit.
    #[serde(default = "default_ticket_version")]
    pub version: u16,

    /// Consume the ticket after a successful lookup. CacheStore exposes
    /// get/delete separately, so this is best-effort rather than an
    /// atomic pop.
    #[serde(default = "default_true")]
    pub consume: bool,

    /// Allow WebSocket auth through `Sec-WebSocket-Protocol:
    /// ticket.<base64url-unpadded-ticket>`.
    #[serde(default = "default_true")]
    pub websocket_subprotocol: bool,

    /// Allow H2 CONNECT / WebTransport auth through `Authorization:
    /// Ticket <ticket>`. `Bearer <ticket>` remains available when ticket
    /// is configured as the default guard for legacy clients.
    #[serde(default = "default_true")]
    pub bearer_header: bool,
}

impl Default for TicketChatAuthConfig {
    fn default() -> Self {
        Self {
            cache_store: default_ticket_cache_store(),
            key_prefix: default_ticket_key_prefix(),
            version: default_ticket_version(),
            consume: true,
            websocket_subprotocol: true,
            bearer_header: true,
        }
    }
}

fn default_ticket_cache_store() -> String {
    "chat".to_string()
}

fn default_ticket_key_prefix() -> String {
    "tickets:".to_string()
}

fn default_ticket_version() -> u16 {
    1
}

fn default_true() -> bool {
    true
}

/// Configuration for periodical metrics collection/export.
#[derive(Clone, PartialEq, Deserialize)]
pub struct MetricsConfig {
    /// Period between metrics collections/exports
    #[serde(
        default = "default_metrics_interval",
        deserialize_with = "deserialize_duration"
    )]
    pub(crate) interval: Duration,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self { interval: default_metrics_interval() }
    }
}

fn default_metrics_interval() -> Duration {
    Duration::from_secs(300)
}

#[cfg(test)]
#[path = "../tests/config.rs"]
mod tests;
