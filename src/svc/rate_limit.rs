use std::sync::Arc;
use std::time::{Duration, Instant};

use claviron::helpers::de::deserialize_duration;
use moka::sync::Cache;
use serde::Deserialize;
use uuid::Uuid;

/// Per-connection fixed-window counter with automatic TTL-based eviction
/// Prevents spam by limiting messages per connection in a time window
#[derive(Clone)]
pub struct ChatRateLimiter {
    window: Duration,
    max: u64,
    // key = ConnectionId (Uuid); value = (window_start, count)
    // Using moka Cache with automatic eviction after 2x window duration
    store: Arc<Cache<Uuid, (Instant, u64)>>,
}

impl ChatRateLimiter {
    pub fn new(
        window: Duration,
        max: u64,
    ) -> Self {
        // Configure cache with TTL = 2x the rate limit window
        // This ensures old entries are automatically evicted
        let ttl = window.mul_f32(2.0);

        let cache = Cache::builder()
            .time_to_idle(ttl) // Evict after 2x window of inactivity
            .max_capacity(10_000) // Hard limit: 10k connections max
            .build();

        Self { window, max, store: Arc::new(cache) }
    }

    /// Check if connection is allowed to send message
    /// Returns Ok(()) if allowed, Err(retry_after_secs) if rate limited
    pub fn check(
        &self,
        connection_id: Uuid,
    ) -> Result<(), u64> {
        let now = Instant::now();

        // Use get_with to atomically get or insert, then update if needed
        let (window_start, count) =
            self.store.get_with(connection_id, || (now, 0));

        // Check if we need to reset the window
        let (window_start, count) =
            if now.duration_since(window_start) >= self.window {
                // Window expired, reset counter
                self.store.insert(connection_id, (now, 1));
                (now, 1)
            } else {
                // Window still active, increment counter
                let new_count = count + 1;
                self.store.insert(connection_id, (window_start, new_count));
                (window_start, new_count)
            };

        if count > self.max {
            // Calculate when the window resets
            let reset_after =
                (window_start + self.window).saturating_duration_since(now);
            return Err(reset_after.as_secs());
        }

        Ok(())
    }

    /// Get current stats for a connection (for debugging/monitoring)
    pub fn stats(
        &self,
        connection_id: Uuid,
    ) -> Option<(u64, u64)> {
        let now = Instant::now();
        self.store.get(&connection_id).and_then(|(start, count)| {
            if now.duration_since(start) < self.window {
                Some((count, self.max.saturating_sub(count)))
            } else {
                None
            }
        })
    }

    /// Clear rate limit for a specific connection (useful for testing)
    pub fn reset(
        &self,
        connection_id: Uuid,
    ) {
        self.store.invalidate(&connection_id);
    }

    /// Get cache size (for monitoring)
    pub fn cache_size(&self) -> u64 {
        self.store.entry_count()
    }
}

#[derive(Clone, PartialEq, Deserialize)]
pub struct ChatRateLimitConfig {
    /// Time window for rate limiting (e.g., "1m", "30s")
    #[serde(deserialize_with = "deserialize_duration")]
    pub window: Duration,

    /// Maximum messages allowed per window
    pub max_messages: u64,
}

impl Default for ChatRateLimitConfig {
    fn default() -> Self {
        Self {
            window: Duration::from_secs(60), // 1 minute
            max_messages: 30,                // 30 messages per minute
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rate_limiter_allows_under_limit() {
        let limiter = ChatRateLimiter::new(Duration::from_secs(60), 5);
        let conn_id = Uuid::new_v4();

        // Should allow first 5 messages
        for _ in 0..5 {
            assert!(limiter.check(conn_id).is_ok());
        }
    }

    #[test]
    fn test_rate_limiter_blocks_over_limit() {
        let limiter = ChatRateLimiter::new(Duration::from_secs(60), 3);
        let conn_id = Uuid::new_v4();

        // First 3 should pass
        for _ in 0..3 {
            assert!(limiter.check(conn_id).is_ok());
        }

        // 4th should be blocked
        assert!(limiter.check(conn_id).is_err());
    }

    #[test]
    fn test_rate_limiter_window_reset() {
        let limiter = ChatRateLimiter::new(Duration::from_millis(100), 2);
        let conn_id = Uuid::new_v4();

        // Use up the limit
        assert!(limiter.check(conn_id).is_ok());
        assert!(limiter.check(conn_id).is_ok());
        assert!(limiter.check(conn_id).is_err());

        // Wait for window to reset
        std::thread::sleep(Duration::from_millis(150));

        // Should be allowed again
        assert!(limiter.check(conn_id).is_ok());
    }

    #[test]
    fn test_rate_limiter_stats() {
        let limiter = ChatRateLimiter::new(Duration::from_secs(60), 10);
        let conn_id = Uuid::new_v4();

        // No stats initially
        assert!(limiter.stats(conn_id).is_none());

        // After 3 messages
        for _ in 0..3 {
            limiter.check(conn_id).ok();
        }

        let (used, remaining) = limiter.stats(conn_id).unwrap();
        assert_eq!(used, 3);
        assert_eq!(remaining, 7);
    }
}
