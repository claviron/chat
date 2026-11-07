use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
#[cfg(feature = "webtransport")]
use claviron::edge::WebTransportRegistry;
use claviron::edge::{Host, HostConfig, Routable};
use claviron::pipeline::HostHandler;
use claviron::runtime::registry::{
    Bootable, Component, Error as RegistryError,
};
use claviron::runtime::{AppState, Register};
#[cfg(feature = "webtransport")]
use tracing::warn;

use crate::config::ChatConfigProvider;
use crate::svc::web::upgrade::ChatSockets;
#[cfg(feature = "webtransport")]
use crate::svc::web::web_transport::ChatWebTransportHandler;

/// Web (HTTP/1.1 + HTTP/2) chat service.
///
/// Registered with its `ChatConfigProvider` in hand. A WebSocket is a host's
/// application contribution, on either HTTP version; `boot` contributes the
/// WebTransport handler for the vhost keys the applied configuration names,
/// before the host routers are compiled.
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
impl Component<AppState> for ChatWebService {
    #[cfg(not(feature = "webtransport"))]
    type Deps = (Arc<ChatConfigProvider>,);
    #[cfg(feature = "webtransport")]
    type Deps = (Arc<ChatConfigProvider>, Option<Arc<WebTransportRegistry>>);

    fn name(&self) -> &'static str {
        "ChatWebService"
    }

    fn as_bootable(&self) -> Option<&dyn Bootable<AppState, Self::Deps>> {
        Some(self)
    }

    // No `as_reloadable` — `ChatConfigProvider` owns config reload and
    // the listener reads the live snapshot via `current()` on every
    // request, so there's no per-service state to refresh.
}

impl Routable for ChatWebService {
    fn match_host(
        &self,
        key: &Host,
        _host: &HostConfig,
    ) -> bool {
        let Some(cfg_provider) = self.config_provider.get() else {
            return false;
        };
        let cfg = cfg_provider.current();
        cfg.embed.as_ref().is_some_and(|embed| embed.vhosts.contains(key))
    }

    fn application(
        self: Arc<Self>,
        state: &AppState,
        key: &Host,
        host: Arc<HostConfig>,
    ) -> claviron::runtime::registry::Result<Option<Arc<dyn HostHandler>>> {
        let config_provider = self.config_provider()?.clone();
        let cfg = config_provider.current();
        let Some(embed) = &cfg.embed else {
            return Ok(None);
        };
        Ok(Some(Arc::new(ChatSockets {
            state: state.clone(),
            path: embed.websocket_route_path(),
            config_provider: Arc::clone(&config_provider),
            host: key.clone(),
            h1_upgrade: host.transport.h1.upgrade,
        })))
    }
}

#[async_trait]
impl Bootable<AppState, <Self as Component<AppState>>::Deps>
    for ChatWebService
{
    /// Contributes handlers to the edge registries from the applied
    /// configuration; nothing is bound or opened here.
    async fn boot(
        &self,
        deps: <Self as Component<AppState>>::Deps,
        _state: &AppState,
    ) -> claviron::runtime::registry::Result<()> {
        #[cfg(not(feature = "webtransport"))]
        let _ = deps;
        #[cfg(feature = "webtransport")]
        let (cfg_provider, webtransport) = deps;
        #[cfg(feature = "webtransport")]
        let cfg = cfg_provider.current();
        #[cfg(feature = "webtransport")]
        let Some(embed) = &cfg.embed else {
            return Ok(());
        };
        #[cfg(feature = "webtransport")]
        if let Some(registry) = webtransport {
            let path = embed.web_transport_route_path();
            for host in &embed.vhosts {
                if let Err(err) = registry.register_handler_for_host(
                    &host.to_string(),
                    path.clone(),
                    Arc::new(ChatWebTransportHandler::new(
                        cfg_provider.clone(),
                    )),
                ) {
                    warn!(
                        "failed to register chat WebTransport binding for vhost {} at {}: {}",
                        host, path, err
                    );
                }
            }
        }
        Ok(())
    }
}
