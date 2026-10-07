# Chat — Architecture

`chat` provides optional chat transports for the main
edge/app server: HTTP/1 WebSocket upgrade, HTTP/2 CONNECT, and HTTP/3
WebTransport through `claviron-edge` and, behind the `quic` feature,
a raw QUIC demo/prototype transport.
The `webtransport` feature also enables `h3-quinn`, whose native bidirectional
stream type appears in the chat session adapter.

The crate is intentionally not on the critical serving path. Chat is a
separate feature surface and may be disabled or misconfigured while the
rest of Claviron should continue to boot and serve normal traffic.

## Crate Boundary

`chat` is a portable app-layer crate in `claviron-extra`. Its message,
receipt, and runtime storage use SQLx directly and own only `chat.*` tables.
It does not query an application's users, avatars, roles, or permissions.

Chat storage uses SQLx's runtime `query`, `query_as`, and `query_scalar`
functions with positional `.bind(...)` parameters. Row records derive `FromRow`;
scalar outputs are explicitly `i64` counts or `i32` user ids. Chat queries do
not require a build-time database connection or `.sqlx` query metadata. SQL
and database schema/type compatibility are checked when the query executes.
Column aliases are ordinary row field names, without SQLx query-macro `!`
annotations. The host's `SQLX_OFFLINE` build policy remains independent.

The ignored `models::tests::runtime_queries_preserve_chat_storage_contracts`
test exercises writes, nullable timestamps, counts, unread summaries and
receipts against an isolated empty PostgreSQL database. Run it with
`CLAVIRON_CHAT_TEST_DATABASE_URL` set and
`cargo test -p claviron-chat runtime_queries_preserve_chat_storage_contracts -- --ignored`.
It creates the test schema inside a transaction and rolls it back afterward;
it refuses to replace an existing `chat` schema.

That means chat must not become another internal owner of Claviron's
core runtime stack. For core Claviron surfaces such as runtime,
helpers, DB, HTTP, TLS/SSL, and vhosts, the preferred future dependency
shape is to consume the public `claviron::...` facade with the required
feature flags enabled, not direct path dependencies on:

- `claviron-runtime`
- `claviron-helpers`
- `claviron-db`
- `claviron-http`
- `claviron-ssl`
- `claviron-tls`
- `claviron-edge`

Authentication comes from `claviron::auth::orbit`; chat has no JWT dependency.
Claviron forwards the selected `auth` + `orbit` features to the optional adapter.
Orbitive remains framework-neutral and does not own chat semantics.

Bootstrap ownership is already at the app boundary: the app conditionally
registers chat from its own `src/bootstrap.rs`:

```rust
#[cfg(feature = "chat")]
claviron_chat::register(state)?;
```

The main server provides the host runtime and feature facade; the app
decides whether the chat service is installed.

### Database access

`sql/access.sql` is the one grant a deployment needs to let a role use chat
storage: row access (SELECT, INSERT, UPDATE, DELETE) on `chat.*` and default
privileges for tables later chat migrations create as `roman`. It never grants
CREATE, TRUNCATE or any other schema, so the role cannot change the tables it
uses. Run it as a superuser with `-v role=<role>`; `-v create=1` also creates
that role as a peer-auth LOGIN with no other rights.

## Browser Client Artifact

`client/` is a small JavaScript project owned by this crate. It is not a
Claviron server dependency; it exists so the chat wire/client contract can
be published with `chat` instead of being rediscovered inside
each host application.

The source is modern ESM in `client/src/index.js`. `client/build.sh`
installs the local npm dependencies when needed and bundles a browser
IIFE with esbuild. By default it writes:

```text
public/chat/claviron-chat.js
```

Set `CLAVIRON_CHAT_PUBLIC_DIR=/path/to/public/chat` when a host app
wants the artifact written directly into its own public tree.

The bundle exposes `window.ClavironChat` with the core `ChatClient`, UUID
helpers, and a minimal Shadow DOM widget mount function. Host apps may
serve this file directly, copy it to their asset pipeline, or replace the
widget while keeping the same `ChatClient` transport contract.

## Providers

`ChatConfigProvider` owns `chat.yaml`. It loads once during boot,
publishes an `ArcSwap` snapshot, and hot-reloads the same snapshot for
all chat runtimes. Chat providers do not read config files directly.

`ChatWebService` is a provider-only bridge into vhost-level transport
registries. It registers the configured WebSocket route
`<chat.path><chat.websocket_path>`, HTTP/2 CONNECT route
`<chat.path><chat.h2_connect_path>`, and, with the `webtransport`
feature, HTTP/3 WebTransport route
`<chat.path><chat.web_transport_path>` for each configured chat host key
during boot; it does not bind a separate socket. Defaults come from
serde default functions and are documented in `config/chat.yaml`
(`/chat/ws`, `/chat/h2`, and `/chat/wt` by default).

`config/chat.yaml` uses `hosts` (or singular `host`) with `Vec<Host>`
semantics, matching `config/grpc.yaml`. These entries are resolved vhost
keys, not raw public aliases. Public aliases belong in
`config/sites/*.yaml`; chat binds to the canonical host key produced by
the vhost resolver.

`ChatQuicService` remains a `Provider + Runnable` wrapper for the raw
QUIC demo/prototype transport. It is not registered on master. In
standalone it runs in the single process; in worker mode every worker may
run it. Its UDP socket is opened through `claviron-sys` reuse-port
helpers so the kernel can distribute QUIC flows across workers. Its listener,
connection tasks, and stream gate belong to one `RunContext` generation:
stopping that generation closes admission, lets accepted sessions drain under
the configured grace policy, and waits for the owned connection tasks.

Its `quic` block is a chat-owned `ChatQuicConfig`: admission/grace/handshake
fields are separate from nested `transport: QuicConfig`. It no longer consumes
the removed HTTP/3 server config or accepts HTTP body/listen fields. Address
remains `chat.address`. Stream limits are positive, null grace is indefinite,
and raw mTLS chat rejects early data. Vhost WebTransport remains bound to the
core named H3 listener and does not use this raw-QUIC configuration.

## Authentication Boundary

The host application binds one `dyn UserProvider` from core auth. Chat resolves
and caches that capability during `ChatServer::boot`; validated subjects, local
numeric message keys, and raw-QUIC identifiers are hydrated through it. A user
must expose an `i32`-compatible `auth_local_id` and, for the current wire
contract, a UUID `auth_subject`.

Chat also defines the optional `AvatarUrlProvider`. The downstream application
owns avatar storage, hash/MIME interpretation, CDN selection, and URL policy;
chat asks only for the final URL while constructing a `ChatUser` snapshot. A
missing provider, `None`, or provider error falls back to a name-based avatar.
Avatar resolution is not part of `Authenticated`.

Web transports accept the configured `chat.auth.guards`. The default is `orbit`;
`ticket` remains an explicitly separate cache-ticket path. JWT is no longer an
accepted guard or credential. Existing `jwt` configuration/client selections must
migrate to `orbit` and obtain an Orbit session-bound token from the host issuer.

- WebSocket uses `bearer.<base64url-unpadded-token>`; `ticket.` selects tickets.
- H2 CONNECT and WebTransport use `Authorization: Bearer <token>`.
- `Authorization: Ticket <ticket>` explicitly selects tickets. Bearer/raw
  credentials use `chat.auth.default`. The legacy `guard` key aliases `default`.
- `auth.orbit.audience` defaults to `urn:chat:svc`. The host's bound authenticator
  also enforces its own exact audience, purpose and required capabilities.

The host boots an `OrbitAuth<P>` using its existing authority and FleetAuth,
then binds `dyn TokenAuthenticator` before chat boot. Chat caches that capability;
it never creates a separate fleet or keyring. The supplied Fleet selects memory
or SHM. Independent processes need SHM, the same trust configuration/state key,
and the same explicit record-protection policy; membership alone grants no trust.
The same state handle remains available to the host for issuance/refresh/revoke.

The adapter validates the token and live session, then hydrates the user through
`UserProvider`. The authenticated transport handoff retains a `VerifiedSession`
alongside the user snapshot, never the raw token. After asynchronous user/avatar
lookup and before connection registration, chat rechecks validity. WS/H2/WT check
again before inbound dispatch and outbound delivery, including queued messages.
Those checks reuse the local verified Principal and current fleet session view;
no token hashing, decryption or user lookup occurs per message. Expiry applies on
cache hits and existing connections. Logout committed by another fleet participant
blocks the next admission/delivery. Already admitted work can finish. Idle H2/WT
connections are not proactively woken on logout; no revocation poller is added.
WebSocket's existing configured ping also checks validity.

The raw QUIC prototype keeps its separate mTLS certificate-identity authentication;
it does not silently acquire session-token semantics. The ticket path below also
retains its independent snapshot/consumption contract, not fleet session revoke.

WebSocket rejection logs include only guard names and sanitized errors, never the
subprotocol value carrying the credential.

The `ticket` guard authenticates long-lived WS/H2/WT sessions with a
short-lived opaque ticket stored in `claviron-cache`. The configured
cache store defaults to `chat`, and the ticket key is
`<key_prefix><sha256(ticket)>`; the stored value is JSON:

```json
{
  "v": 1,
  "id": 123,
  "uuid": "00000000-0000-0000-0000-000000000000",
  "username": "operator",
  "name": "Operator Name",
  "avatar_url": "https://..."
}
```

Ticket auth does not hydrate the user from chat's Postgres database.
The ticket issuer owns this login-time snapshot, and the snapshot remains
fixed for the lifetime of the chat connection. Chat still uses the
numeric `id` for message persistence, delivery receipts, and unread
state; if the external MySQL user mirror and chat Postgres state drift,
the write path fails where that invariant matters.

Ticket issuance is outside the transport layer. Today it can be a thin
site endpoint that writes the JSON into the shared `chat` cache store;
later it can move behind a `ChatProvider` or another host adapter. The
client contract stays the same: the issuer returns client-ready values
such as `websocket_subprotocol` and `authorization`.

## Run Gating

Chat has no selected-worker run gate anymore:

- `enabled: false` means the runnable returns immediately.
- master does not register raw QUIC chat.
- standalone runs raw QUIC in the single process.
- workers may all run raw QUIC and own only their accepted local clients.

Vhost WebSocket, H2 CONNECT, WebTransport, and raw QUIC all follow the
same ownership rule: accepted requests/connections may land on any
worker. `ChatServer` boots when chat is enabled so handlers have local
connection registries and DB-backed runtime state available where the
request is served.

## Worker Fanout

`ChatServer` keeps only worker-local connection and room membership
registries. These maps describe the clients currently attached to this
process; transport handles never cross a process boundary.

When a local client produces a delivery event, `ChatServer` first applies
local validation and persistence where needed, then emits a compact
`ChatDeliveryEvent` onto the process-local event bus. The
`claviron-orbit` typed event bridge publishes that event to the shared
Orbit event ring, and every worker subscribes back into its local bus.
Each worker then hydrates any large payload from chat storage and fans
out only to matching local clients or rooms.

The existing typed event bridge remains appropriate for this fanout. No
`orbit-stream` connection is needed: every worker receives compact delivery
references and loads message bodies from storage. Streams would be a separate
point-to-point data-transfer design, not an auth/session prerequisite. The event
ring is bounded ephemeral notification: a missed delivery is recovered from
persisted chat history, not made reliable merely by sharing an auth table.

Orbit is used as ephemeral same-host fanout, not as message storage.
Room and private message bodies remain in the chat database before the
delivery signal is emitted. This keeps Orbit payloads bounded and keeps
persistent history separate from transport delivery.

Live presence uses a second typed Orbit topic. Each process owns a random
worker-instance id and publishes compact counters keyed by
`(instance_id, user_id)` and `(instance_id, user_id, room)`. Counts preserve
multi-device semantics: two tabs on one worker and connections spread across
different workers all contribute independently. Query responses merge the
local authoritative maps with unexpired remote records, then deduplicate user
lists by application user id.

Workers refresh active presence periodically so a newly started worker learns
the existing fleet without a request/reply handshake. Remote records expire
after `presence.stale_after`, which removes records left by a hard-crashed
worker. Both `presence.sync_interval` (default `5s`) and
`presence.stale_after` (default `15s`) are operator configuration. Normal
connect/disconnect/join/leave transitions publish immediately; the periodic
refresh is recovery and convergence, not the primary update path.
`stale_after` must be at least twice `sync_interval`; invalid configurations
fail during config-provider boot/reload instead of creating intermittent
presence gaps.

Each worker-instance also assigns monotonically increasing revisions. Removal
records remain as short-lived tombstones, so an older periodic snapshot that
was captured before a concurrent disconnect cannot resurrect a user or room.
Periodic records are spread across the sync interval instead of being emitted
as one burst; this keeps both the local lifecycle channel and Orbit poller
inside their bounded capacities.

`UserOnline`, `UserOffline`, `UserJoined`, and `UserLeft` notifications use the
best-known fleet view. Simultaneous first connections on different workers can
produce duplicate transition notifications, so clients must treat these
notifications as idempotent. Explicit online/room list queries are the
converged snapshot within the configured stale window.

## Failure Policy

Chat listener startup failures are recoverable runtime failures. Missing
certificates, missing mTLS CA paths, bind conflicts, or hostname
host-key configuration mistakes must be logged but must not tear down the worker.

This is different from core listeners such as HTTPS/H3. Chat is a
best-effort auxiliary service; the edge/app server should keep serving
if chat cannot start.

To enforce that boundary, `ChatQuicService::run()` wraps listener
errors with `registry::Error::recoverable(...)`. The runtime logs the
failure and continues waiting on the remaining runnable providers. The
vhost WebSocket/H2 bindings are boot-time registrations; missing host
keys skip the binding with a warning.

## Transport Boundary

Vhost WebSocket, H2 CONNECT, and WebTransport chat do not own TLS. The
site/vhost listener terminates TLS, applies host policy, and dispatches
the long-lived session to the registered chat handler.

WebSocket remains the browser-friendly H1 upgrade path. H2 CONNECT is
the primary native/TUI/GUI path and uses the same authenticated chat
session and Orbit-backed fanout once connected. WebTransport is the
browser HTTP/3 path when the host listener supports it. H2/H3
connection-level keepalive and timeout policy belongs to the web
service that owns the connection, not to chat protocol messages. All
transport adapters enter through normal vhost routing and never own
global chat state.

The raw QUIC transport still derives a per-chat certificate resolver
from the shared TLS resolver. If none of the configured `hosts` exist in
the shared resolver, TLS returns an error such as “No hostname found in
shared host”. For chat, that is a recoverable configuration problem, not
a process-fatal condition.
