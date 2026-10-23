//! Message admission keyed by authenticated identity, independent of HTTP routes.
use std::num::{NonZeroU64, NonZeroUsize};
use std::time::Duration;

use claviron::orbit::quota::{
    Decision, Error as QuotaError, Key, Quota, QuotaBacking, QuotaConfig,
    QuotaStore, QuotaTable,
};
use serde::Deserialize;

use crate::ChatClient;

/// The identity whose connections share one budget, never a transport socket ID.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ChatRateLimitKey {
    #[default]
    User,
    Session,
}

impl ChatRateLimitKey {
    fn namespace(self) -> &'static [u8] {
        match self {
            Self::User => b"chat:user",
            Self::Session => b"chat:session",
        }
    }
}

/// Chat owns the identity policy; Claviron owns prepared quota configuration.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct ChatRateLimitConfig {
    pub key: ChatRateLimitKey,
    pub quota: QuotaConfig,
}

impl Default for ChatRateLimitConfig {
    fn default() -> Self {
        Self { key: ChatRateLimitKey::User, quota: default_quota() }
    }
}

/// Retains the previous 30 messages/minute rate and 10,000-key capacity.
/// Bounds are configuration, not hidden hot-path retry/eviction policy.
fn default_quota() -> QuotaConfig {
    QuotaConfig {
        units: NonZeroU64::new(30).unwrap(),
        period: Duration::from_secs(60),
        burst: NonZeroU64::new(30).unwrap(),
        table: QuotaTable {
            slots: NonZeroUsize::new(10_000).unwrap(),
            shards: NonZeroUsize::new(16).unwrap(),
            key_bytes: NonZeroUsize::new(32).unwrap(),
            probes: NonZeroUsize::new(8).unwrap(),
        },
        cas_attempts: NonZeroUsize::new(64).unwrap(),
        store: QuotaBacking::LocalGuard {},
    }
}

impl ChatRateLimitConfig {
    /// Pure validation: no SHM open, clock read, or admission side effect.
    pub fn validate(&self) -> Result<(), &'static str> {
        if !matches!(
            self.quota.store,
            QuotaBacking::LocalGuard {} | QuotaBacking::FleetGuard { .. }
        ) {
            return Err("chat rate_limit supports local_guard or fleet_guard");
        }
        // Both authenticated UUIDs and Orbit SessionId are exactly 16 bytes.
        if self.quota.table.key_bytes.get() < self.key.namespace().len() + 16 {
            return Err("chat rate_limit key_bytes cannot fit its identity");
        }
        self.quota.validate().map_err(|_| "invalid chat rate_limit quota")
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ChatRateLimitError {
    #[error("Rate limit exceeded. Try again in {retry_after_seconds} seconds")]
    Limited { retry_after_seconds: u64 },
    #[error("Chat rate limit requires a verified session")]
    MissingSession,
    #[error("Chat rate limiter is unavailable")]
    Unavailable,
    #[error("Chat rate limiter is temporarily unavailable")]
    Store(#[source] QuotaError),
}

/// One prepared store per Chat service, shared by all its local connections.
/// FleetGuard independently opens a dedicated quota resource in each process.
#[derive(Clone)]
pub struct ChatRateLimiter {
    key: ChatRateLimitKey,
    store: Quota,
}

impl ChatRateLimiter {
    pub fn new(
        config: &ChatRateLimitConfig
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        config.validate()?;
        Ok(Self { key: config.key, store: Quota::new(config.quota.clone())? })
    }

    pub(crate) fn matches(
        &self,
        config: &ChatRateLimitConfig,
    ) -> bool {
        self.key == config.key && self.store.config() == &config.quota
    }

    /// Exactly one charge, no extra store read, allocation, retry or refund.
    pub fn check(
        &self,
        client: &ChatClient,
    ) -> Result<(), ChatRateLimitError> {
        let subject = match self.key {
            ChatRateLimitKey::User => *client.user_uuid().as_bytes(),
            ChatRateLimitKey::Session => client
                .quota_session_id()
                .ok_or(ChatRateLimitError::MissingSession)?,
        };
        let key = Key::new(self.key.namespace(), &subject)
            .map_err(ChatRateLimitError::Store)?;
        match self.store.try_consume(key, NonZeroU64::MIN) {
            Ok(Decision::Allowed) => Ok(()),
            Ok(Decision::Limited { retry_after }) => {
                Err(ChatRateLimitError::Limited {
                    retry_after_seconds:
                        claviron::http::rate_limit::retry_after_seconds(
                            retry_after,
                        ),
                })
            }
            Err(error) => Err(ChatRateLimitError::Store(error)),
        }
    }
}

#[cfg(test)]
#[path = "../../tests/svc/rate_limit.rs"]
mod tests;
