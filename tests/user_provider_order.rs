use std::sync::Arc;

use claviron::auth::UserProvider;
use claviron::runtime::AppState;
use claviron::runtime::registry::{Component, ComponentOrder, Registry};

use crate::{
    AvatarUrlProvider, ChatServer, SessionAuthenticator, UserDirectory,
};

struct IdentityPreparation;

impl Component<AppState> for IdentityPreparation {
    type Deps = ();
    fn name(&self) -> &'static str {
        "test identity preparation"
    }
    fn order(&self) -> ComponentOrder {
        ComponentOrder::new().before::<Arc<dyn UserProvider>>()
    }
}

struct AdmissionPreparation;

impl Component<AppState> for AdmissionPreparation {
    type Deps = ();
    fn name(&self) -> &'static str {
        "test admission preparation"
    }
    fn order(&self) -> ComponentOrder {
        ComponentOrder::new()
            .before::<Arc<dyn SessionAuthenticator>>()
            .before::<Arc<dyn AvatarUrlProvider>>()
    }
}

struct DirectoryPreparation;

impl Component<AppState> for DirectoryPreparation {
    type Deps = ();
    fn name(&self) -> &'static str {
        "test directory preparation"
    }
    fn order(&self) -> ComponentOrder {
        ComponentOrder::new().before::<Arc<dyn UserDirectory>>()
    }
}

#[test]
fn chat_waits_for_identity_and_admission_preparation() {
    // Build only the dependency plan: no AppState, service boot or database.
    let registry = Registry::<AppState>::new();
    registry.register(Arc::new(ChatServer::empty())).unwrap();
    registry.register(Arc::new(AdmissionPreparation)).unwrap();
    registry.register(Arc::new(IdentityPreparation)).unwrap();
    registry.register(Arc::new(DirectoryPreparation)).unwrap();
    let names = registry.lifecycle_names().unwrap();
    let chat = names.iter().position(|name| *name == "ChatServer").unwrap();
    for preparation in [
        "test identity preparation",
        "test admission preparation",
        "test directory preparation",
    ] {
        assert!(
            names.iter().position(|name| *name == preparation).unwrap() < chat
        );
    }
}
