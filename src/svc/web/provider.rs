use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
#[cfg(feature = "webtransport")]
use claviron::edge::WebTransportRegistry;
use claviron::edge::{
    H1UpgradeRoute, H2ConnectRegistry, Host, HostConfig, Routable,
};
use claviron::runtime::registry::{Error as RegistryError, Provider};
use claviron::runtime::{AppState, Register};
use tracing::warn;

use crate::config::ChatConfigProvider;
use crate::svc::web::h2::ChatH2ConnectHandler;
use crate::svc::web::upgrade::ChatWsUpgradeHandler;
#[cfg(feature = "webtransport")]
use crate::svc::web::web_transport::ChatWebTransportHandler;

/// Web (HTTP/1.1 + HTTP/2) chat service.
///
/// Registered with its `ChatConfigProvider` in hand; `boot` contributes the
/// H2 CONNECT and WebTransport handlers to the edge registries for the hosts
/// the applied configuration names, before the host routers are compiled.
/// WebSocket traffic is contributed as a host-scoped H1 upgrade binding, so
/// the normal vhost router owns the HTTP/1 upgrade boundary.
pub struct ChatWebService {
    config_provider: OnceLock<Arc<ChatConfigProvider>>,
}

impl ChatWebService {
    pub fn empty(_state: AppState) -> Self {
        Self { config_provider: OnceLock::new() }
    }

    fn config_provider(
        &self
    ) -> claviron::runtime::registry::Result<&Arc<ChatConfigProvider>> {
        self.config_provider.get().ok_or_else(|| {
            RegistryError::msg("ChatWebService: ChatConfigProvider not loaded")
        })
    }
}

/// Registers the service as a routable contribution, after
/// `ChatConfigProvider`.
impl Register<AppState> for ChatWebService {
    fn register(state: &AppState) -> claviron::runtime::registry::Result<()> {
        let cfg_provider = state
            .registry_ref()
            .resolve::<ChatConfigProvider>()
            .ok_or_else(|| {
                RegistryError::msg(
                    "ChatWebService needs ChatConfigProvider: register it first",
                )
            })?;
        let service = ChatWebService::empty(state.clone());
        let _ = service.config_provider.set(cfg_provider);
        claviron::edge::register_routable(state, Arc::new(service))
    }
}

#[async_trait]
impl Provider<AppState> for ChatWebService {
    fn name(&self) -> &'static str {
        "ChatWebService"
    }

    /// Contributes handlers to the edge registries from the applied
    /// configuration; nothing is bound or opened here.
    async fn boot(
        &self,
        state: &AppState,
    ) -> claviron::runtime::registry::Result<()> {
        let cfg_provider = self.config_provider()?.clone();
        let cfg = cfg_provider.current();
        if cfg.enabled
            && let Some(registry) =
                state.registry_ref().resolve::<H2ConnectRegistry>()
        {
            let path = cfg.h2_connect_route_path();
            for host in cfg.hosts() {
                if let Err(err) = registry.register_handler_for_host(
                    &host.to_string(),
                    path.clone(),
                    Arc::new(ChatH2ConnectHandler::new(cfg_provider.clone())),
                ) {
                    warn!(
                        "failed to register chat H2 CONNECT binding for host {} at {}: {}",
                        host, path, err
                    );
                }
            }
        }
        #[cfg(feature = "webtransport")]
        if cfg.enabled
            && let Some(registry) =
                state.registry_ref().resolve::<WebTransportRegistry>()
        {
            let path = cfg.web_transport_route_path();
            for host in cfg.hosts() {
                if let Err(err) = registry.register_handler_for_host(
                    &host.to_string(),
                    path.clone(),
                    Arc::new(ChatWebTransportHandler::new(
                        cfg_provider.clone(),
                    )),
                ) {
                    warn!(
                        "failed to register chat WebTransport binding for host {} at {}: {}",
                        host, path, err
                    );
                }
            }
        }
        Ok(())
    }

    // No `as_reloadable` — `ChatConfigProvider` owns config reload and
    // the listener reads the live snapshot via `current()` on every
    // request, so there's no per-service state to refresh.
}

impl Routable for ChatWebService {
    fn match_host(
        &self,
        key: &Host,
        host: &HostConfig,
    ) -> bool {
        let Some(cfg_provider) = self.config_provider.get() else {
            return false;
        };
        let cfg = cfg_provider.current();
        if !cfg.enabled {
            return false;
        }

        cfg.hosts().iter().any(|configured| {
            configured == key
                || host.aliases().iter().any(|alias| alias == configured)
        })
    }

    fn h1_upgrades(
        &self,
        _key: &Host,
        _host: Arc<HostConfig>,
    ) -> claviron::runtime::registry::Result<Vec<H1UpgradeRoute>> {
        let cfg_provider = self.config_provider()?.clone();
        let cfg = cfg_provider.current();
        let path = cfg.websocket_route_path();
        Ok(vec![H1UpgradeRoute::handler(
            path,
            Arc::new(ChatWsUpgradeHandler::new(cfg_provider)),
        )?])
    }
}
