use claviron::runtime::{AppState, Register};

use crate::ChatServer;
use crate::config::ChatConfigProvider;
#[cfg(feature = "quic")]
use crate::svc::ChatQuicService;
use crate::svc::ChatWebService;

/// Register chat, in every process.
///
/// `ChatConfigProvider` owns `chat.yaml` through Congif; the runtimes
/// (`ChatServer`, `ChatWebService`, `ChatQuicService`) are handed it at
/// registration and read its snapshot — none of them touch the filesystem.
/// What runs where is declared through `Scoped`: the fanout and the QUIC
/// listener run in workers, and every runnable honours `enabled` before it
/// does anything.
pub fn register(state: &AppState) -> claviron::runtime::registry::Result<()> {
    ChatConfigProvider::register(state)?;
    ChatServer::register(state)?;
    ChatWebService::register(state)?;
    #[cfg(feature = "quic")]
    ChatQuicService::register(state)?;
    Ok(())
}
