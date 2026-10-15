use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use async_trait::async_trait;
use claviron::helpers::de::deserialize_duration;
use claviron::runtime::congif::{
    self, ConfigFile, ConfigObserver, Configurable,
};
use claviron::runtime::registry::{Component, Reloadable, priority};
use claviron::runtime::{AppState, Register};
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::svc::rate_limit::ChatRateLimitConfig;

mod quic;
use quic::ChatQuicConfig;
mod embed;
pub use embed::ChatEmbedConfig;

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

    /// No file means no ingress declarations, so chat remains inactive.
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
#[serde(deny_unknown_fields)]
pub struct ChatConfig {
    /// Optional Routable integration into the embedding server's web listener.
    pub embed: Option<ChatEmbedConfig>,

    /// Raw QUIC is opt-in through its own optional listener, never HTTP/3.
    quic: Option<ChatQuicConfig>,

    /// Maximum message size in bytes (default: 64KB)
    #[serde(default = "default_max_message_size")]
    pub max_message_size: usize,

    /// Shared metrics warning threshold; listener admission is transport-owned.
    #[serde(default = "default_max_connections")]
    pub max_connections: usize,

    /// Rate limiting configuration for svc messages
    /// Prevents spam by limiting messages per connection per time window
    #[serde(default)]
    pub rate_limit: ChatRateLimitConfig,

    /// Worker presence replication and stale-record policy.
    #[serde(default)]
    pub presence: ChatPresenceConfig,

    /// Bounded history delivery and browser working-set policy.
    #[serde(default)]
    pub history: ChatHistoryConfig,

    /// Metrics reporting configuration
    #[serde(default)]
    pub metrics: Option<MetricsConfig>,
}

impl ChatConfig {
    pub(crate) fn quic_cfg(&self) -> Option<&ChatQuicConfig> {
        self.quic.as_ref()
    }

    pub(crate) fn has_ingress(&self) -> bool {
        self.embed.is_some()
            || self.quic.as_ref().is_some_and(|quic| quic.listener.is_some())
    }

    pub(crate) fn metrics_interval(&self) -> Option<Duration> {
        self.metrics.as_ref().map(|m| m.interval)
    }

    fn validate(&self) -> Result<(), &'static str> {
        self.history.validate()?;
        if let Some(embed) = &self.embed {
            embed.validate()?;
        }
        if let Some(quic) = &self.quic {
            quic.validate()?;
            if quic.listener.is_some() && !cfg!(feature = "quic") {
                return Err("quic.listener requires the chat quic feature");
            }
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
            embed: None,
            quic: None,
            max_message_size: default_max_message_size(),
            max_connections: default_max_connections(),
            rate_limit: ChatRateLimitConfig::default(),
            presence: ChatPresenceConfig::default(),
            history: ChatHistoryConfig::default(),
            metrics: None,
        }
    }
}

/// Published in Welcome so clients use the server's declared history policy.
/// These limits never delete persistent messages.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ChatHistoryConfig {
    pub initial_limit: u32,
    pub page_limit: u32,
    pub max_limit: u32,
    pub max_messages_per_conversation: u32,
    pub max_conversations: u32,
}

impl Default for ChatHistoryConfig {
    fn default() -> Self {
        Self {
            initial_limit: 50,
            page_limit: 50,
            max_limit: 100,
            max_messages_per_conversation: 500,
            max_conversations: 50,
        }
    }
}

impl ChatHistoryConfig {
    fn validate(&self) -> Result<(), &'static str> {
        if self.initial_limit == 0
            || self.page_limit == 0
            || self.max_limit == 0
        {
            return Err("history limits must be greater than zero");
        }
        if self.initial_limit > self.max_limit
            || self.page_limit > self.max_limit
        {
            return Err(
                "history initial_limit and page_limit must not exceed max_limit",
            );
        }
        if self.max_messages_per_conversation < self.initial_limit
            || self.max_messages_per_conversation < self.page_limit
        {
            return Err(
                "history max_messages_per_conversation must hold a history page",
            );
        }
        if self.max_conversations < 2 {
            return Err(
                "history max_conversations must hold a room and a private conversation",
            );
        }
        Ok(())
    }

    pub(crate) fn request_limit(
        &self,
        requested: Option<i64>,
    ) -> Result<i64, &'static str> {
        let requested = requested.unwrap_or(i64::from(self.initial_limit));
        if requested <= 0 {
            return Err("history limit must be greater than zero");
        }
        Ok(requested.min(i64::from(self.max_limit)))
    }
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
