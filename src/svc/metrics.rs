use std::sync::atomic::{AtomicU64, Ordering};

use tokio::time;
use tracing::{debug, warn};

use crate::config::ChatConfig;

/// Global svc metrics instance
pub static CHAT_METRICS: ChatMetrics = ChatMetrics::new();

/// Chat service metrics (WebSocket + QUIC)
pub struct ChatMetrics {
    /// Current active connections (WebSocket + QUIC)
    active_connections: AtomicU64,
    /// Total connections accepted since startup
    total_connections: AtomicU64,
    /// Total messages sent to clients
    messages_sent: AtomicU64,
    /// Total messages received from clients
    messages_received: AtomicU64,
}

impl ChatMetrics {
    pub const fn new() -> Self {
        Self {
            active_connections: AtomicU64::new(0),
            total_connections: AtomicU64::new(0),
            messages_sent: AtomicU64::new(0),
            messages_received: AtomicU64::new(0),
        }
    }

    /// Increment active connections counter
    pub fn connect(&self) {
        self.active_connections.fetch_add(1, Ordering::Relaxed);
        self.total_connections.fetch_add(1, Ordering::Relaxed);
    }

    /// Decrement active connections counter
    pub fn disconnect(&self) {
        self.active_connections.fetch_sub(1, Ordering::Relaxed);
    }

    /// Increment messages sent counter
    pub fn message_sent(&self) {
        self.messages_sent.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment messages received counter
    pub fn message_received(&self) {
        self.messages_received.fetch_add(1, Ordering::Relaxed);
    }

    /// Get current active connections
    pub fn active(&self) -> u64 {
        self.active_connections.load(Ordering::Relaxed)
    }

    /// Get total connections since startup
    pub fn total(&self) -> u64 {
        self.total_connections.load(Ordering::Relaxed)
    }

    /// Get total messages sent
    pub fn sent(&self) -> u64 {
        self.messages_sent.load(Ordering::Relaxed)
    }

    /// Get total messages received
    pub fn received(&self) -> u64 {
        self.messages_received.load(Ordering::Relaxed)
    }
}

impl Default for ChatMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Start periodic svc metrics reporter
pub fn spawn_chat_metrics_reporter(cfg: &ChatConfig) {
    let interval = match cfg.metrics_interval() {
        Some(interval) => interval,
        None => {
            warn!("💬 Chat-Metrics are not enabled");
            return;
        }
    };

    let max_connections = cfg.max_connections;

    tokio::spawn(async move {
        let mut ticker = time::interval(interval);
        ticker.set_missed_tick_behavior(time::MissedTickBehavior::Skip);

        // Store previous counters for rate calculation
        let mut prev_sent = 0u64;
        let mut prev_received = 0u64;

        loop {
            ticker.tick().await;

            let active = CHAT_METRICS.active();
            let total = CHAT_METRICS.total();
            let sent = CHAT_METRICS.sent();
            let received = CHAT_METRICS.received();

            // Calculate message rates (per interval)
            let sent_rate = sent.saturating_sub(prev_sent);
            let received_rate = received.saturating_sub(prev_received);
            prev_sent = sent;
            prev_received = received;

            let interval_secs = interval.as_secs();
            let sent_per_sec = sent_rate / interval_secs.max(1);
            let received_per_sec = received_rate / interval_secs.max(1);

            debug!(
                "💬 Chat Metrics: active={}, total={}, sent={} ({}/s), received={} ({}/s)",
                active, total, sent, sent_per_sec, received, received_per_sec
            );

            // Warn if approaching max connections
            let threshold = (max_connections as f64 * 0.8) as u64; // 80% threshold
            if active > threshold {
                warn!(
                    "⚠️  High number of active svc connections: {}/{} - approaching limit",
                    active, max_connections
                );
            }
        }
    });
}
