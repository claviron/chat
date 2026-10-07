use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use claviron::orbit::placement::{Scope, Scoped};
use claviron::runtime::registry::{Error as RegistryError, Provider, Runnable};
use claviron::runtime::{AppState, Register, RunContext};

use crate::config::ChatConfigProvider;
use crate::svc::quic::server::start_quic_service;

/// The raw QUIC prototype listener. Registered in every process with its
/// `ChatConfigProvider` in hand; `Scoped` keeps it running in workers (or a
/// standalone process), and `run` honours `enabled` before binding.
pub struct ChatQuicService {
    state: AppState,
    config_provider: OnceLock<Arc<ChatConfigProvider>>,
}

impl ChatQuicService {
    pub fn empty(state: AppState) -> Self {
        Self { state, config_provider: OnceLock::new() }
    }

    fn config_provider(&self) -> &ChatConfigProvider {
        self.config_provider
            .get()
            .expect("ChatQuicService is registered with its ChatConfigProvider")
    }
}

impl Register<AppState> for ChatQuicService {
    fn register(state: &AppState) -> claviron::runtime::registry::Result<()> {
        let cfg_provider = state
            .registry_ref()
            .resolve::<ChatConfigProvider>()
            .ok_or_else(|| {
                RegistryError::msg(
                    "ChatQuicService needs ChatConfigProvider: register it first",
                )
            })?;
        let service = ChatQuicService::empty(state.clone());
        let _ = service.config_provider.set(cfg_provider);
        state.registry_ref().insert(Arc::new(service))?;
        Ok(())
    }
}

#[async_trait]
impl Provider<AppState> for ChatQuicService {
    fn name(&self) -> &'static str {
        "ChatQuicService"
    }

    // No `as_reloadable` — config reload is owned by `ChatConfigProvider`,
    // and the listener reads the live snapshot.

    fn as_runnable(
        self: Arc<Self>
    ) -> Option<std::sync::Arc<dyn Runnable<AppState>>> {
        Some(self)
    }

    fn as_scoped(&self) -> Option<&dyn Scoped> {
        Some(self)
    }
}

impl Scoped for ChatQuicService {
    fn scope(&self) -> Scope {
        Scope::Worker
    }
}

#[async_trait]
impl Runnable<AppState> for ChatQuicService {
    async fn run(
        self: Arc<Self>,
        _state: AppState,
        context: RunContext,
    ) -> Result<(), RegistryError> {
        let state = self.state.clone();
        let provider = self.config_provider();
        let cfg = provider.current();
        if !cfg.enabled {
            return Ok(());
        }
        start_quic_service(state, cfg.as_ref().clone(), context)
            .await
            .map_err(|err| RegistryError::recoverable(err.to_string()))
    }
}
