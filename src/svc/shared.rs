use std::time::Duration;

use claviron::edge::{Host, VhostResolver};
use claviron::runtime::AppState;
use claviron::runtime::registry::Error;
use claviron::tls::CertResolver;
use claviron::tls::auth::verifier::PolicyConfig;
use claviron::tls::auth::{
    ClientAuthPolicy, create_auth_config, derive_resolver_for_hosts,
};
use tracing::{error, warn};

/// Raw QUIC borrows server certificates and the existing hostname mTLS policy.
/// A single Quinn endpoint has one client verifier, so all selected server
/// names must have the same trust policy; never merge different CA authorities.
pub(crate) fn prepare_chat_tls(
    state: &AppState,
    hosts: &[Host],
) -> claviron::runtime::registry::Result<rustls::ServerConfig> {
    let resolver =
        state.registry_ref().resolve::<CertResolver>().ok_or_else(|| {
            Error::msg("Chat service requires a CustomCertResolver")
        })?;

    let vhosts =
        state.registry_ref().resolve::<VhostResolver>().ok_or_else(|| {
            Error::msg(
                "Raw chat QUIC requires the prepared TLS hostname policy",
            )
        })?;
    let snapshot = vhosts.snapshot();
    let mut selected: Option<ClientAuthPolicy> = None;
    for hostname in hosts {
        let (_, host) = snapshot.resolve_exact(hostname).ok_or_else(|| {
            Error::msg(format!("Raw chat QUIC TLS hostname '{hostname}' has no configured TLS policy"))
        })?;
        if !resolver.contains(&hostname.to_string()) {
            return Err(Error::msg(format!(
                "Raw chat QUIC TLS hostname '{hostname}' has no loaded certificate"
            )));
        }
        let policy = host.tls_policy();
        let auth = &policy.client_auth;
        validate_client_auth(auth)?;
        if let Some(previous) = &selected {
            if !same_client_auth(previous, auth) {
                return Err(Error::msg(
                    "Raw chat QUIC listener hosts must share the same client-auth CA and subject policy",
                ));
            }
        } else {
            selected = Some(auth.clone());
        }
    }
    let auth = selected.ok_or_else(|| {
        Error::msg("Raw chat QUIC listener requires TLS hostnames")
    })?;
    let subject = match (&auth.organization, &auth.unit) {
        (Some(organization), Some(unit)) => {
            Some(PolicyConfig::new(organization.clone(), unit.clone()))
        }
        _ => None,
    };
    let cert_names = hosts.iter().map(String::from).collect::<Vec<_>>();
    let scoped = derive_resolver_for_hosts(&cert_names, &resolver)?;
    let ca = auth.ca_bundle.as_deref().ok_or_else(|| {
        Error::msg("Raw chat QUIC TLS policy has no client-auth CA")
    })?;
    Ok(create_auth_config(
        ca,
        scoped,
        vec![super::quic::ALPN.to_vec()],
        subject.as_ref(),
    )?)
}

fn validate_client_auth(
    auth: &ClientAuthPolicy
) -> claviron::runtime::registry::Result<()> {
    if !auth.is_required() {
        return Err(Error::msg(
            "Raw chat QUIC requires tls.client_auth.mode=required on its TLS hostnames",
        ));
    }
    if auth.ca_bundle.as_ref().is_none_or(|path| path.as_os_str().is_empty()) {
        return Err(Error::msg(
            "Raw chat QUIC TLS hostname has no configured client-auth CA",
        ));
    }
    if auth.organization.is_some() != auth.unit.is_some() {
        return Err(Error::msg(
            "Raw chat QUIC TLS hostname must configure organization and unit together",
        ));
    }
    Ok(())
}

fn same_client_auth(
    left: &ClientAuthPolicy,
    right: &ClientAuthPolicy,
) -> bool {
    left.mode == right.mode
        && left.ca_bundle == right.ca_bundle
        && left.organization == right.organization
        && left.unit == right.unit
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

#[cfg(test)]
#[path = "../../tests/quic_tls.rs"]
mod tests;
