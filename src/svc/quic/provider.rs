use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use claviron::runtime::registry::{Error as RegistryError, Provider, Runnable};
use claviron::runtime::{AppState, RunContext};

use crate::config::ChatConfigProvider;
use crate::svc::quic::server::start_quic_service;

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
            .expect("ChatQuicService::config_provider() called before Provider::boot() ran")
    }
}

#[async_trait]
impl Provider<AppState> for ChatQuicService {
    fn name(&self) -> &'static str {
        "ChatQuicService"
    }

    async fn boot(
        &self,
        state: &AppState,
    ) -> claviron::runtime::registry::Result<()> {
        let cfg_provider = state
            .registry_ref()
            .resolve::<ChatConfigProvider>()
            .ok_or_else(|| {
                RegistryError::msg(
                    "ChatQuicService boot: ChatConfigProvider not registered",
                )
            })?;
        let _ = self.config_provider.set(cfg_provider);
        Ok(())
    }

    // No `as_reloadable` — same rationale as ChatWebService: config
    // reload is owned by `ChatConfigProvider`.

    fn as_runnable(
        self: Arc<Self>
    ) -> Option<std::sync::Arc<dyn Runnable<AppState>>> {
        Some(self)
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
