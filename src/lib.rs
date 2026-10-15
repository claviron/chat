mod bootstrap;
#[allow(unused)]
pub mod config;
mod error;
mod event;
pub(crate) mod handler;
mod identity;
#[allow(unused)]
mod models;
mod presence;
pub mod svc;
pub mod types;

pub use bootstrap::register;
pub use error::{Error, Result};
pub(crate) use handler::handle_client_message;
pub use identity::{
    AvatarUrlError, AvatarUrlProvider, SessionAuthenticator, UserDirectory,
};
pub use types::client::ChatClient;
pub use types::server::ChatServer;
