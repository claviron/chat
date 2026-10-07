use std::borrow::Cow;
use std::sync::Arc;

use claviron::auth::{AnyAuthenticated, Authenticated, UserProvider};
use moka::sync::Cache;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use tracing::warn;
use uuid::Uuid;

use crate::AvatarUrlProvider;

const DEFAULT_CHAT_USER_SUBJECT: &str = "39493052-9a18-478c-8930-529a18478caa";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ChatUser(Arc<CachedUser>);

impl ChatUser {
    pub fn id(&self) -> i32 {
        self.0.user_id
    }

    pub fn uuid(&self) -> Uuid {
        self.0.uuid
    }

    pub fn username(&self) -> &str {
        &self.0.username
    }

    pub fn username_arc(&self) -> &Arc<str> {
        &self.0.username
    }

    pub fn name_arc(&self) -> &Arc<str> {
        &self.0.name
    }

    pub fn avatar_arc(&self) -> &Arc<str> {
        &self.0.avatar_url
    }

    pub(crate) fn from_trusted_snapshot(
        user_id: i32,
        uuid: Uuid,
        username: impl Into<Arc<str>>,
        name: impl Into<Arc<str>>,
        avatar_url: impl Into<Arc<str>>,
    ) -> Self {
        upsert_cache(Arc::new(CachedUser {
            user_id,
            uuid,
            username: username.into(),
            name: name.into(),
            avatar_url: avatar_url.into(),
        }))
    }

    pub(crate) async fn from_authenticated(
        user: AnyAuthenticated,
        avatars: Option<&dyn AvatarUrlProvider>,
    ) -> Result<Self, String> {
        let local_id = user.auth_local_id().ok_or_else(|| {
            "chat requires an application-local numeric user id".to_string()
        })?;
        let user_id = i32::try_from(local_id).map_err(|_| {
            "chat user local id is outside the i32 range".to_string()
        })?;
        if user_id <= 0 {
            return Err("chat user local id must be positive".to_string());
        }

        let subject = user.auth_subject();
        let uuid = Uuid::parse_str(&subject).map_err(|_| {
            "chat currently requires a UUID auth subject".to_string()
        })?;
        let username = user.auth_identifier().to_owned();
        let name = user.auth_display_name().to_owned();
        let avatar_url = match avatars {
            Some(provider) => match provider.avatar_url(user.as_ref()).await {
                Ok(Some(url)) if !url.is_empty() => url,
                Ok(_) => Arc::from(format_avatar_from_name(&name)),
                Err(error) => {
                    warn!(%error, subject = %subject, "avatar URL provider failed; using fallback");
                    Arc::from(format_avatar_from_name(&name))
                }
            },
            None => Arc::from(format_avatar_from_name(&name)),
        };

        Ok(Self::from_trusted_snapshot(
            user_id, uuid, username, name, avatar_url,
        ))
    }
}

impl Authenticated for ChatUser {
    fn auth_subject(&self) -> Cow<'_, str> {
        Cow::Owned(self.uuid().to_string())
    }

    fn auth_identifier(&self) -> &str {
        self.username()
    }

    fn auth_display_name(&self) -> &str {
        &self.0.name
    }

    fn auth_local_id(&self) -> Option<i64> {
        Some(i64::from(self.id()))
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CachedUser {
    #[serde(skip_serializing)]
    user_id: i32,
    uuid: Uuid,
    username: Arc<str>,
    name: Arc<str>,
    avatar_url: Arc<str>,
}

fn build_cache<K, V>() -> Cache<K, V>
where
    K: Eq + std::hash::Hash + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    Cache::builder()
        .time_to_live(std::time::Duration::from_secs(21_600))
        .time_to_idle(std::time::Duration::from_secs(1_800))
        .max_capacity(10_000)
        .build()
}

static BY_UUID: Lazy<Cache<Uuid, Arc<CachedUser>>> = Lazy::new(build_cache);
static BY_ID: Lazy<Cache<i32, Arc<CachedUser>>> = Lazy::new(build_cache);

fn upsert_cache(user: Arc<CachedUser>) -> ChatUser {
    BY_UUID.insert(user.uuid, Arc::clone(&user));
    BY_ID.insert(user.user_id, Arc::clone(&user));
    ChatUser(user)
}

pub(crate) async fn by_subject(
    users: &dyn UserProvider,
    avatars: Option<&dyn AvatarUrlProvider>,
    subject: &str,
) -> Result<Option<ChatUser>, String> {
    if let Ok(uuid) = Uuid::parse_str(subject)
        && let Some(user) = BY_UUID.get(&uuid)
    {
        return Ok(Some(ChatUser(user)));
    }
    let user = users
        .find_by_subject(subject)
        .await
        .map_err(|error| error.to_string())?;
    match user {
        Some(user) => {
            ChatUser::from_authenticated(user, avatars).await.map(Some)
        }
        None => Ok(None),
    }
}

pub(crate) async fn by_local_id(
    users: &dyn UserProvider,
    avatars: Option<&dyn AvatarUrlProvider>,
    local_id: i32,
) -> Result<Option<ChatUser>, String> {
    if let Some(user) = BY_ID.get(&local_id) {
        return Ok(Some(ChatUser(user)));
    }
    let user = users
        .find_by_local_id(i64::from(local_id))
        .await
        .map_err(|error| error.to_string())?;
    match user {
        Some(user) => {
            ChatUser::from_authenticated(user, avatars).await.map(Some)
        }
        None => Ok(None),
    }
}

#[cfg(feature = "quic")]
pub(crate) async fn by_identifier(
    users: &dyn UserProvider,
    avatars: Option<&dyn AvatarUrlProvider>,
    identifier: &str,
) -> Result<Option<ChatUser>, String> {
    let user = users
        .find_by_identifier(identifier)
        .await
        .map_err(|error| error.to_string())?;
    match user {
        Some(user) => {
            ChatUser::from_authenticated(user, avatars).await.map(Some)
        }
        None => Ok(None),
    }
}

pub(crate) async fn hydrate_users(
    users: &dyn UserProvider,
    avatars: Option<&dyn AvatarUrlProvider>,
    ids: &[i32],
) -> Result<Vec<ChatUser>, String> {
    let mut hydrated = Vec::with_capacity(ids.len());
    for id in ids {
        if let Some(user) = by_local_id(users, avatars, *id).await? {
            hydrated.push(user);
        }
    }
    Ok(hydrated)
}

pub(crate) async fn system_user(
    users: &dyn UserProvider,
    avatars: Option<&dyn AvatarUrlProvider>,
) -> ChatUser {
    if let Ok(Some(user)) =
        by_subject(users, avatars, DEFAULT_CHAT_USER_SUBJECT).await
    {
        return user;
    }
    let uuid = Uuid::parse_str(DEFAULT_CHAT_USER_SUBJECT)
        .expect("invalid default chat user UUID");
    ChatUser::from_trusted_snapshot(
        0,
        uuid,
        "svc",
        "Claviron Chat",
        format_avatar_from_name("Claviron Chat"),
    )
}

pub(crate) fn user_invalidate(
    uuid: Uuid,
    id: i32,
) {
    BY_UUID.invalidate(&uuid);
    BY_ID.invalidate(&id);
}

fn format_avatar_from_name(name: &str) -> String {
    format!(
        "https://ui-avatars.com/api/?name={}&size=128&background=random",
        urlencoding::encode(name)
    )
}
