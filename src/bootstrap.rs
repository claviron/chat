use std::sync::Arc;

use claviron::runtime::AppState;

use crate::ChatServer;
use crate::config::{ChatConfigProvider, chat_config_path};
#[cfg(feature = "quic")]
use crate::svc::ChatQuicService;
use crate::svc::ChatWebService;

/// Register chat providers.
///
/// Per the `Provider` lifecycle convention: `register()` is pure type
/// insertion — no config reads, no environment checks, no registry
/// resolves. `ChatConfigProvider` owns `chat.yaml` (loaded at its own
/// `boot()`, priority EARLY). The runtime providers (`ChatServer`,
/// `ChatWebService`, `ChatQuicService`) resolve `ChatConfigProvider`
/// in their boot to read the snapshot — none of them touch the
/// filesystem directly. `Runnable::run()` honours `enabled` before
/// spawning long-lived loops.
pub fn register(state: &AppState) -> claviron::runtime::registry::Result<()> {
    let registry = state.registry_ref();
    let cfg_path = chat_config_path(state);

    registry.insert(Arc::new(ChatConfigProvider::empty(cfg_path)))?;
    registry.insert(Arc::new(ChatServer::empty()))?;
    claviron::edge::register_routable(
        state,
        Arc::new(ChatWebService::empty(state.clone())),
    )?;

    #[cfg(feature = "quic")]
    if !state.is_master() {
        registry.insert(Arc::new(ChatQuicService::empty(state.clone())))?;
    }

    Ok(())
}
