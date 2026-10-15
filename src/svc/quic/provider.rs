use std::sync::Arc;

use async_trait::async_trait;
use claviron::edge::HttpConfigProvider;
use claviron::http::listener::ListenerTransport;
use claviron::orbit::placement::{
    Dispatch, DispatchPolicy, Dispatched, Scope, Scoped,
};
use claviron::runtime::registry::{
    Component, Error as RegistryError, Runnable,
};
use claviron::runtime::{AppState, Register, RunContext, ServiceStartPolicy};

use crate::config::ChatConfigProvider;
use crate::svc::quic::server::start_quic_service;

/// Optional raw QUIC listener. Placement and socket ownership are independent
/// from the web transports; each execution owns its endpoint and drain.
pub struct ChatQuicService {
    state: AppState,
    config: Arc<ChatConfigProvider>,
}

impl Register<AppState> for ChatQuicService {
    fn register(state: &AppState) -> claviron::runtime::registry::Result<()> {
        let config =
            state.registry_ref().resolve::<ChatConfigProvider>().ok_or_else(
                || RegistryError::msg("ChatConfigProvider is not registered"),
            )?;
        let service = Self { state: state.clone(), config };
        state.registry_ref().register(Arc::new(service))?;
        Ok(())
    }
}

#[async_trait]
impl Component<AppState> for ChatQuicService {
    type Deps = (Arc<ChatConfigProvider>,);

    fn name(&self) -> &'static str {
        "ChatQuicService"
    }

    // Config reload belongs to ChatConfigProvider. A new runnable generation
    // captures the prepared listener; reload never silently rebinds a socket.

    fn as_runnable(
        self: Arc<Self>
    ) -> Option<std::sync::Arc<dyn Runnable<AppState, Self::Deps>>> {
        Some(self)
    }

    fn as_scoped(&self) -> Option<&dyn Scoped> {
        Some(self)
    }

    fn as_dispatched(&self) -> Option<&dyn Dispatched> {
        Some(self)
    }

    fn validate(
        &self,
        state: &AppState,
    ) -> Result<(), RegistryError> {
        let config = self.config.current();
        if let Some(listener) =
            config.quic_cfg().and_then(|quic| quic.listener.as_ref())
        {
            validate_bindings(listener.listen, state)?;
        }
        Ok(())
    }
}

impl Scoped for ChatQuicService {
    fn scope(&self) -> Scope {
        self.config
            .current()
            .quic_cfg()
            .and_then(|quic| quic.listener.as_ref())
            .map(|listener| listener.scope)
            .unwrap_or_default()
    }
}

impl Dispatched for ChatQuicService {
    fn dispatch(&self) -> Dispatch {
        self.config
            .current()
            .quic_cfg()
            .and_then(|quic| quic.listener.as_ref())
            .map(|listener| listener.dispatch)
            .unwrap_or_default()
    }

    fn dispatch_policy(&self) -> Option<DispatchPolicy> {
        self.config
            .current()
            .quic_cfg()
            .and_then(|quic| quic.listener.as_ref())
            .and_then(|listener| listener.dispatch_policy)
    }
}

#[async_trait]
impl Runnable<AppState, <Self as Component<AppState>>::Deps>
    for ChatQuicService
{
    fn start_policy(
        &self,
        _state: &AppState,
    ) -> ServiceStartPolicy {
        let config = self.config.current();
        if config.quic_cfg().is_some_and(|quic| quic.listener.is_some()) {
            ServiceStartPolicy::Automatic
        } else {
            ServiceStartPolicy::Unavailable
        }
    }

    async fn run(
        self: Arc<Self>,
        (provider,): <Self as Component<AppState>>::Deps,
        _state: AppState,
        context: RunContext,
    ) -> Result<(), RegistryError> {
        let state = self.state.clone();
        let cfg = provider.current();
        let Some(listener) =
            cfg.quic_cfg().and_then(|quic| quic.listener.as_ref())
        else {
            return Ok(());
        };
        validate_bindings(listener.listen, &state)?;
        start_quic_service(state, cfg.as_ref().clone(), context)
            .await
            .map_err(|err| RegistryError::recoverable(err.to_string()))
    }
}

fn validate_bindings(
    reserved: std::net::SocketAddr,
    state: &AppState,
) -> Result<(), RegistryError> {
    let Some(http) = state.registry_ref().resolve::<HttpConfigProvider>()
    else {
        return Ok(());
    };
    let snapshot = http.snapshot();
    for listener in snapshot
        .listeners
        .values()
        .filter(|listener| listener.transport() == ListenerTransport::Quic)
    {
        for address in &listener.listen {
            if binds_overlap(reserved, *address) {
                return Err(RegistryError::msg(format!(
                    "Raw chat QUIC bind {reserved} conflicts with edge QUIC listener '{}' at {address}; reuseport must not mix raw QUIC and HTTP/3",
                    listener.id
                )));
            }
        }
    }
    Ok(())
}

fn binds_overlap(
    left: std::net::SocketAddr,
    right: std::net::SocketAddr,
) -> bool {
    use std::net::IpAddr;
    if left.port() == 0 || right.port() == 0 || left.port() != right.port() {
        return false;
    }
    match (left.ip(), right.ip()) {
        (IpAddr::V4(a), IpAddr::V4(b)) => {
            a == b || a.is_unspecified() || b.is_unspecified()
        }
        (IpAddr::V6(a), IpAddr::V6(b)) => {
            a == b || a.is_unspecified() || b.is_unspecified()
        }
        (IpAddr::V6(v6), IpAddr::V4(v4)) | (IpAddr::V4(v4), IpAddr::V6(v6)) => {
            v6.is_unspecified()
                || v6
                    .to_ipv4_mapped()
                    .is_some_and(|mapped| mapped == v4 || v4.is_unspecified())
        }
    }
}

#[cfg(test)]
#[path = "../../../tests/quic_listener.rs"]
mod tests;
