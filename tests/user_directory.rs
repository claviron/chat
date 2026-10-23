use super::*;
use claviron::auth::{AnyAuthenticated, UserProviderError};

struct NoLookup;

#[async_trait]
impl UserProvider for NoLookup {
    async fn find_by_subject(
        &self,
        _: &str,
    ) -> Result<Option<AnyAuthenticated>, UserProviderError> {
        panic!("directory fixtures must not resolve subjects")
    }

    async fn find_by_local_id(
        &self,
        _: i64,
    ) -> Result<Option<AnyAuthenticated>, UserProviderError> {
        panic!("local room members must use their existing user snapshots")
    }
}

struct Contacts(Vec<AnyAuthenticated>);

#[async_trait]
impl UserDirectory for Contacts {
    async fn list_users(
        &self
    ) -> Result<Vec<AnyAuthenticated>, UserProviderError> {
        Ok(self.0.clone())
    }
}

struct Unavailable;

#[async_trait]
impl UserDirectory for Unavailable {
    async fn list_users(
        &self
    ) -> Result<Vec<AnyAuthenticated>, UserProviderError> {
        Err(UserProviderError::msg("directory unavailable"))
    }
}

fn server() -> ChatServer {
    let server = ChatServer::empty();
    assert!(server.users.set(Arc::new(NoLookup)).is_ok());
    assert!(
        server
            .config_provider
            .set(Arc::new(ChatConfigProvider::empty(
                "chat.example.yaml".into()
            )))
            .is_ok()
    );
    server
}

fn user(
    id: i32,
    username: &str,
) -> ChatUser {
    ChatUser::from_trusted_snapshot(id, Uuid::new_v4(), username, username, "")
}

async fn local_client(
    server: &ChatServer,
    user: ChatUser,
) -> ChatClient {
    let (sender, _) = tokio::sync::mpsc::channel(1);
    let client = ChatClient::new(user, sender);
    server.clients.write().await.insert(client.connection_id(), client.clone());
    client
}

#[tokio::test]
async fn directory_includes_offline_contacts_and_uses_fleet_presence() {
    let server = server();
    let alice = user(1, "alice");
    let bob = user(2, "bob");
    let carol = user(3, "carol");
    assert!(
        server
            .directory
            .set(Some(Arc::new(Contacts(vec![
                Arc::new(carol.clone()),
                Arc::new(bob.clone()),
                Arc::new(alice.clone()),
                Arc::new(alice.clone()),
            ]))))
            .is_ok()
    );
    // Presence is global to chat, not membership of the requested room.
    local_client(&server, alice.clone()).await;
    let remote = Uuid::new_v4();
    server
        .presence
        .apply(ChatPresenceEvent::User {
            instance_id: remote,
            revision: 1,
            user_id: bob.id(),
            connections: 2,
        })
        .await;
    let users = server.list_users("general").await.unwrap();
    assert_eq!(users.len(), 3);
    assert_eq!(
        users.iter().map(|user| user.username.as_ref()).collect::<Vec<_>>(),
        ["alice", "bob", "carol"]
    );
    assert_eq!(
        users.iter().map(|user| user.online).collect::<Vec<_>>(),
        [true, true, false]
    );
    assert_eq!(users[2].uuid, carol.uuid());
    assert!(server.rooms.read().await.is_empty());

    server.clients.write().await.clear();
    server
        .presence
        .apply(ChatPresenceEvent::User {
            instance_id: remote,
            revision: 2,
            user_id: bob.id(),
            connections: 0,
        })
        .await;
    let users = server.list_users("general").await.unwrap();
    assert_eq!(users.len(), 3);
    assert!(users.iter().all(|user| !user.online));
}

#[tokio::test]
async fn absent_directory_keeps_connected_room_members_only() {
    let server = server();
    let alice = user(11, "alice");
    let bob = user(12, "bob");
    let first = local_client(&server, alice.clone()).await;
    let second = local_client(&server, alice.clone()).await;
    local_client(&server, bob).await;
    let mut room = ChatRoom::new("general".into());
    room.add_member(first);
    room.add_member(second);
    server.rooms.write().await.insert("general".into(), room);
    let users = server.list_users("general").await.unwrap();
    assert_eq!(users.len(), 1);
    assert_eq!(users[0].uuid, alice.uuid());
    assert!(users[0].online);
    assert!(server.list_users("missing").await.is_err());
}

#[tokio::test]
async fn empty_directory_is_not_replaced_with_connected_users() {
    let server = server();
    assert!(server.directory.set(Some(Arc::new(Contacts(Vec::new())))).is_ok());
    local_client(&server, user(21, "alice")).await;
    assert!(server.list_users("general").await.unwrap().is_empty());
}

#[tokio::test]
async fn directory_failure_is_not_silently_replaced_with_another_list() {
    let server = server();
    assert!(server.directory.set(Some(Arc::new(Unavailable))).is_ok());
    assert!(
        server
            .list_users("general")
            .await
            .unwrap_err()
            .contains("directory unavailable")
    );
}
