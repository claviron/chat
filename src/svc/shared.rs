use std::sync::Arc;
use std::time::Duration;

use claviron::edge::Host;
use claviron::runtime::AppState;
use claviron::runtime::registry::Error;
use claviron::tls::CertResolver;
use claviron::tls::auth::derive_resolver_for_hosts;
use tracing::{error, warn};

pub(crate) fn derive_chat_resolver(
    state: &AppState,
    hosts: &[Host],
) -> claviron::runtime::registry::Result<Arc<CertResolver>> {
    let resolver =
        state.registry_ref().resolve::<CertResolver>().ok_or_else(|| {
            Error::msg("Chat service requires a CustomCertResolver")
        })?;

    let cert_names = hosts.iter().map(String::from).collect::<Vec<_>>();
    Ok(derive_resolver_for_hosts(&cert_names, &resolver)?)
}

use crate::svc::{
    MIN_IDLE_TIMEOUT_MULTIPLIER, RECOMMENDED_IDLE_TIMEOUT_MULTIPLIER,
};
pub(crate) fn validate_idle_timeout(
    mode: &'static str,
    interval_name: &'static str,
    interval: Duration,
    idle_timeout: Duration,
) -> claviron::runtime::registry::Result<()> {
    let min_allowed = interval * MIN_IDLE_TIMEOUT_MULTIPLIER;
    let recommended = interval * RECOMMENDED_IDLE_TIMEOUT_MULTIPLIER;

    if idle_timeout <= min_allowed {
        error!(
            mode,
            interval_name,
            ?interval,
            ?idle_timeout,
            ?min_allowed,
            "❌ FATAL: idle_timeout must be greater than interval"
        );
        return Err(Error::msg(format!(
            "{mode} config error: idle_timeout ({idle_timeout:?}) must be greater than {interval_name} ({interval:?})"
        )));
    }

    if idle_timeout < recommended {
        warn!(
            mode,
            interval_name,
            ?interval,
            ?idle_timeout,
            ?recommended,
            "⚠️ idle_timeout is below recommended threshold ({}x interval); connection stability may be affected",
            RECOMMENDED_IDLE_TIMEOUT_MULTIPLIER
        );
    }

    Ok(())
}
