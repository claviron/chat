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
/// listener run in workers. Ingress declarations opt in to serving; missing
/// embed/QUIC listener configuration starts no serving work.
pub fn register(state: &AppState) -> claviron::runtime::registry::Result<()> {
    ChatConfigProvider::register(state)?;
    ChatServer::register(state)?;
    ChatWebService::register(state)?;
    #[cfg(feature = "quic")]
    ChatQuicService::register(state)?;
    Ok(())
}
