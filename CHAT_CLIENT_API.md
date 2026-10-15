# Chat Client API

This document is the client contract for Claviron chat. It should be enough to
implement a browser client, a Yew client, a TUI/native client, or a test client
without reading the server internals.

Chat delivery is worker-transparent: a client can connect to any worker through
any supported transport and still see the same logical rooms and events. Each
worker keeps only its local connected clients; live delivery is fanned out
between workers through typed Orbit events.

## Packaged Browser Client

`chat` ships a small browser client project under `client/`.
Build it with:

```sh
cd client
./build.sh
```

The build writes `public/chat/claviron-chat.js`. It exposes
`window.ClavironChat`:

To write directly into a host app public directory:

```sh
CLAVIRON_CHAT_PUBLIC_DIR=/sites/chat/public ./build.sh
```

```html
<script src="/chat/claviron-chat.js"></script>
<div id="nav-chat"></div>
<script>
ClavironChat.mount("#nav-chat", {
  auth: "ticket",
  ticketUrl: "/chat/ticket",
  room: "operators"
});
</script>
```

The widget provides a default chat icon. Host apps can override only the
symbol while keeping the same connection behavior:

```js
ClavironChat.mount("#nav-chat", {
  ticketUrl: "/chat/ticket",
  room: "operators",
  icon: "fa-comments-o"
});
```

For full control, pass trusted HTML from the host app:

```js
ClavironChat.mount("#nav-chat", {
  ticketUrl: "/chat/ticket",
  iconHtml: '<i class="fa fa-commenting-o"></i>'
});
```

For custom UI, use the core client directly:

```js
const client = new ClavironChat.ChatClient({
  auth: "ticket",
  ticketUrl: "/chat/ticket"
});
client.on("room_message", (message) => console.log(message));
await client.connect();
client.joinRoom("operators");
client.sendToRoom("operators", "hello");
```

For a same-origin application with an existing session:

```js
const client = new ClavironChat.ChatClient({
  auth: "session",
  websocketUrl: `${location.origin}/chat/ws`
});
```

For an Orbit session-bound token issued by the host application:

```js
const client = new ClavironChat.ChatClient({
  auth: "orbit",
  websocketUrl: "wss://chat.example.test/chat/ws",
  token: hostIssuedToken
});
```

## Transports

All production transports enter through normal host/vhost routing. No
production client should depend on a dedicated chat master, selected worker, or
special standalone listener.

Default paths are configured from `chat.yaml`:

| Transport | Default path | Best for | Wire payload |
|---|---:|---|---|
| WebSocket | `/chat/ws` | Browsers today | MessagePack binary frame |
| H2 CONNECT | `/chat/h2` | TUI, GUI, native clients | `u32be` length + MessagePack |
| WebTransport | `/chat/wt` | Browser HTTP/3 path where available | `u32be` length + MessagePack on one bidi stream |
| Raw QUIC | configured `quic` listener | Demo/prototype transport | Legacy framed envelope |

Use WebSocket for browser clients now. Use H2 CONNECT for native clients that do
not need browser APIs. WebTransport is the HTTP/3 browser path, but the current
handler expects bearer authentication in headers; browsers that cannot set an
`Authorization` header for WebTransport need an auth-binding adapter before WT
is the primary browser transport.

## Authentication

Auth is selected from the embedded binding's `auth.guards` in `chat.yaml`. WebSocket clients
select an enabled guard with their `bearer.` or `ticket.` subprotocol prefix.
For H2 CONNECT and WebTransport, `Ticket` selects ticket auth explicitly;
`Bearer` uses `auth.default`. Orbit's audience is configurable through
`auth.orbit.audience` and defaults to `urn:chat:svc`. JWT mode is not
supported; use a host-issued Orbit session-bound token instead.

`session` is an exclusive alternative, not a fallback: the host supplies
`SessionAuthenticator`, and chat accepts only a valid existing session and an
explicitly allowed Origin. It never creates a guest session. The browser client
uses the current origin's cookie without a credential subprotocol or ticket
request. H2/WebTransport session mode likewise requires session credentials in
the handshake; there is no first-message login protocol.

For clients using the optional `ticket` guard, a ticket issuer writes a
short-lived payload into the shared `chat` cache store and returns JSON that is
already shaped for clients:

```json
{
  "guard": "ticket",
  "ticket": "opaque-secret",
  "expires_in": 60,
  "websocket_url": "wss://chat.example.test/chat/ws",
  "h2_connect_url": "https://chat.example.test/chat/h2",
  "web_transport_url": "https://chat.example.test/chat/wt",
  "websocket_subprotocol": "ticket.<base64url-unpadded-ticket>",
  "authorization": "Ticket opaque-secret"
}
```

The client should treat `ticket` as opaque. Browser WebSocket clients use the
provided `websocket_subprotocol`; H2 CONNECT and WebTransport clients use the
provided `authorization` value. The server-side ticket payload is a JSON user
snapshot (`v`, numeric `id`, `uuid`, `username`, `name`, `avatar_url`) and is
not rehydrated from the chat database during authentication.

### WebSocket

Browser WebSocket cannot set arbitrary request headers, so the token is carried
in `Sec-WebSocket-Protocol`:

```text
bearer.<base64url-unpadded-orbit-token>
ticket.<base64url-unpadded-ticket>
```

Example:

```ts
function bearerSubprotocol(credential: string): string {
  const token = credential.replace(/^Bearer\s+/i, "").trim();
  const base64url = btoa(token)
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/g, "");
  return `bearer.${base64url}`;
}

const ws = new WebSocket("wss://chat.example.test/chat/ws", [
  bearerSubprotocol(hostIssuedToken),
]);
ws.binaryType = "arraybuffer";
```

With ticket auth, use the bootstrap JSON directly:

```ts
const boot = await fetch("/chat/ticket", { method: "POST" }).then((r) => r.json());
const ws = new WebSocket(boot.websocket_url, [boot.websocket_subprotocol]);
ws.binaryType = "arraybuffer";
```

### H2 CONNECT

H2 CONNECT uses a normal bearer header:

```text
Authorization: Bearer <orbit-token>
Authorization: Ticket <ticket>
```

The request method is `CONNECT`, the path is the configured H2 endpoint, and the
request/response bodies form the bidirectional chat stream.

### WebTransport

The current WebTransport binding also expects:

```text
Authorization: Bearer <orbit-token>
Authorization: Ticket <ticket>
```

After the WebTransport session is accepted, the client opens one bidirectional
stream for chat frames. The server accepts the first bidi stream for the current
session and uses it as the chat stream.

## Encoding

All chat messages are MessagePack maps with a `type` field.

Use named/map encoding, not tuple/array encoding. In Rust that means
`rmp_serde::to_vec_named`. In JavaScript `@msgpack/msgpack` encodes plain
objects as maps.

UUID fields are binary 16-byte values, not hyphenated strings on the wire. For
JavaScript, encode UUIDs as `Uint8Array(16)` and decode UUID fields from
`Uint8Array`. For Rust, `uuid::Uuid` with the same serde helper is used.

Timestamps are Unix timestamps in seconds.

## Framing

WebSocket does not add an application length prefix. Each WebSocket binary frame
contains exactly one MessagePack `ClientMessage` or `ServerMessage`.

H2 CONNECT and WebTransport use the same stream framing:

```text
4 bytes: unsigned big-endian payload length
N bytes: MessagePack payload
```

The length is the number of MessagePack payload bytes, excluding the 4-byte
prefix. Frames larger than `max_message_size` are rejected.

TypeScript helpers:

```ts
import { decode, encode } from "@msgpack/msgpack";

export function encodeFrame(message: unknown): Uint8Array {
  const payload = encode(message);
  const frame = new Uint8Array(4 + payload.length);
  new DataView(frame.buffer).setUint32(0, payload.length, false);
  frame.set(payload, 4);
  return frame;
}

export function tryDecodeFrames(
  buffer: Uint8Array,
): { messages: unknown[]; rest: Uint8Array } {
  const messages: unknown[] = [];
  let offset = 0;

  while (buffer.length - offset >= 4) {
    const len = new DataView(
      buffer.buffer,
      buffer.byteOffset + offset,
      4,
    ).getUint32(0, false);
    if (buffer.length - offset < 4 + len) break;
    const payload = buffer.slice(offset + 4, offset + 4 + len);
    messages.push(decode(payload));
    offset += 4 + len;
  }

  return { messages, rest: buffer.slice(offset) };
}
```

## Connection Flow

1. Connect and authenticate using the selected transport.
2. Set binary mode where the transport requires it.
3. Receive `welcome`.
4. Store `connection_id`, `user_uuid`, `username`, and unread summary.
5. Send `list_rooms`, `get_online_users`, or history requests as needed.
6. Optimistically render outgoing messages with a client-generated UUID.
7. Reconcile private acceptance (`sent`), recipient receipts and failures from
   `message_status`; a room's own server echo confirms its acceptance.
8. On reconnect, repeat from `welcome` and refresh unread badges.

The `welcome` message is delivered as a normal server message for all production
vhost transports.

## Client Messages

All client messages are MessagePack maps with `type` in snake case.

### Join a Room

```ts
{ type: "join_room", room: "operator" }
```

Server may respond with:

```ts
{ type: "success", message: "you joined room: operator" }
{ type: "user_joined", room: "operator", user_uuid, username }
{ type: "user_list", room: "operator", users: [...] }
```

### Leave a Room

```ts
{ type: "leave_room", room: "operator" }
```

### Send to a Room

```ts
{
  type: "send_to_room",
  uuid,        // 16-byte UUID generated by the client
  room: "operator",
  content: "hello"
}
```

The server persists the message, publishes a live delivery event, and every
worker fans out:

```ts
{
  type: "room_message",
  uuid,
  room: "operator",
  from_user_uuid,
  from_username,
  from_avatar_url,
  content: "hello",
  timestamp: 1760000000
}
```

On failure, the sender receives:

```ts
{ type: "message_status", uuid, status: "failed", timestamp: 1760000000 }
{ type: "error", message: "..." }
```

### Send a Direct Message

```ts
{
  type: "send_direct",
  uuid,
  to_user_uuid,
  content: "hello"
}
```

Recipient workers fan out:

```ts
{
  type: "direct_message",
  uuid,
  from_user_uuid,
  from_username,
  from_avatar_url,
  content: "hello",
  timestamp: 1760000000
}
```

After successful persistence, the originating connection also receives:

```ts
{ type: "message_status", uuid, status: "sent", timestamp: 1760000000 }
```

Keep a local outgoing message pending until this acknowledgement, a stronger
receipt, or restored history confirms it. A local `sendDirect()` return alone
is not server acceptance. Persistence or recipient-lookup failure reports
`message_status.failed` and `error`; unavailable storage is not success.

### Typing

```ts
{ type: "typing", room: "operator" }
{ type: "typing_direct", to_user_uuid }
```

Server events:

```ts
{ type: "user_typing", room: "operator", user_uuid, username }
{ type: "user_typing_direct", from_user_uuid, from_username }
```

Typing events are live hints. Do not persist them in the client message store.

### History

Room history:

```ts
{
  type: "get_history",
  room: "operator",
  limit: 50,
  since: null,
  before: null
}
```

Direct history:

```ts
{
  type: "get_direct_history",
  with_user_uuid,
  limit: 50,
  since: null,
  before: null
}
```

`since` and `before` are optional Unix timestamps in seconds.

Welcome publishes the effective `history` policy. Use `initial_limit` when a
conversation opens and `page_limit` when loading older messages. The server
caps every query shape, including `since`, at `max_limit`. Nonpositive limits
are rejected. Client buffers use `max_messages_per_conversation` and
`max_conversations`; these limits do not delete database history. Private
history entries also return persisted `status` (`sent`, `delivered`, or `read`).
Room history currently omits status and reader lists.

Responses:

```ts
{
  type: "message_history",
  room: "operator",
  messages: [
    { uuid, from_uuid, content: "hello", timestamp: 1760000000 }
  ]
}

{
  type: "direct_message_history",
  with_user_uuid,
  messages: [
    { uuid, from_uuid, content: "hello", timestamp: 1760000000, status: "read" }
  ]
}
```

### Rooms and Users

```ts
{ type: "list_rooms" }
{ type: "list_users", room: "operator" }
{ type: "get_online_users" }
```

Responses:

```ts
{
  type: "room_list",
  rooms: [
    { name: "operator", member_count: 3, crated_at: "..." }
  ]
}

{
  type: "user_list",
  room: "operator",
  users: [
    { uuid, username, name, avatar, online: true }
  ]
}

{
  type: "online_users",
  users: [
    { user_uuid, username }
  ]
}
```

`crated_at` is the current serialized field name. Clients should treat it as the
room creation timestamp and may alias it to `created_at` internally.

When the host binds the optional `UserDirectory`, `user_list` includes permitted
directory contacts, even offline users who have never connected. Chat derives
`online` from local and fleet presence. Without the provider, the response keeps
the connected-room-member behavior. Directory membership does not grant a
session, room membership, or access to messages. Use `user_online`/`user_offline`
as idempotent presence updates; roster data is not conversation text.

### Delivery and Read Receipts

```ts
{ type: "mark_delivered", uuid, room: "operator" }
{ type: "mark_read", uuid, room: "operator" }
```

For direct messages, omit `room` or set it to `null`:

```ts
{ type: "mark_read", uuid, room: null }
```

Status event:

```ts
{
  type: "message_status",
  uuid,
  status: "delivered" | "read" | "failed" | "sent",
  timestamp: 1760000000,
  read_by: [{ user_uuid, username }],
  total_recipients: 5
}
```

For private messages, only the recipient can acknowledge delivery/read.
`delivered` means the receiving client acknowledged the message, not merely that
the server queued it. Send `mark_read` only for a visible, active conversation;
receiving a frame in a background tab is not reading it. Read status persists
and is returned in private history. Late `sent` or `delivered` events must not
downgrade `read`.

For room messages, `room` must identify the message's actual room. Room read
events include `read_by` (reader UUIDs and usernames) and `total_recipients`.
Delivered room events may contain an empty `read_by`; this is not a reset.
A room receipt describes acknowledgement by a recipient, not necessarily all
members. The recipient count is not a read or delivered count.

The Ops overlay uses one check for server acceptance, double checks for delivery,
and green double checks for private reads. In rooms it keeps delivery checks
neutral and shows compact reader avatars from `read_by`. Reuse the user-directory
avatar, with initials as fallback. Merge/deduplicate readers by UUID and retain
them across late or empty snapshots; do not replace them with the last reader.
These reader lists are currently live-event metadata, not part of room history.

### Unread Counts

The `welcome` message contains cheap unread counts:

```ts
{
  type: "welcome",
  connection_id,
  user_uuid,
  username: "ada",
  name: "Ada Lovelace",
  message: "Welcome",
  unread: {
    private_messages_count: 4,
    room_messages: [
      { room: "operator", count: 2 }
    ]
  },
  history: {
    initial_limit: 50,
    page_limit: 50,
    max_limit: 100,
    max_messages_per_conversation: 500,
    max_conversations: 50
  }
}
```

Request details only when the UI needs them:

```ts
{ type: "get_unread_private" }
{ type: "get_unread_room", room: "operator" }
```

Responses:

```ts
{
  type: "unread_private_messages",
  messages: [
    { sender_uuid, sender_username, sender_avatar, count: 3 }
  ]
}

{ type: "unread_room_messages", room: "operator", count: 2 }
```

## Server Messages

Client code should handle at least these server message types:

```ts
type ServerMessage =
  | { type: "welcome"; connection_id: UuidBytes; user_uuid: UuidBytes; username: string; name: string; message: string; unread: UnreadSummary; history: HistoryPolicy }
  | { type: "room_message"; uuid: UuidBytes; room: string; from_user_uuid: UuidBytes; from_username: string; from_avatar_url: string; content: string; timestamp: number }
  | { type: "direct_message"; uuid: UuidBytes; from_user_uuid: UuidBytes; from_username: string; from_avatar_url: string; content: string; timestamp: number }
  | { type: "user_joined"; room: string; user_uuid: UuidBytes; username: string }
  | { type: "user_left"; room: string; user_uuid: UuidBytes; username: string }
  | { type: "user_online"; user_uuid: UuidBytes; username: string }
  | { type: "user_offline"; user_uuid: UuidBytes; username: string }
  | { type: "user_typing"; room: string; user_uuid: UuidBytes; username: string }
  | { type: "user_typing_direct"; from_user_uuid: UuidBytes; from_username: string }
  | { type: "message_history"; room: string; messages: HistoryMessage[] }
  | { type: "direct_message_history"; with_user_uuid: UuidBytes; messages: HistoryMessage[] }
  | { type: "room_list"; rooms: RoomInfo[] }
  | { type: "user_list"; room: string; users: UserInfo[] }
  | { type: "system"; content: string }
  | { type: "error"; message: string }
  | { type: "success"; message: string }
  | { type: "message_status"; uuid: UuidBytes; status: MessageStatus; timestamp: number; read_by?: ReadReceipt[]; total_recipients?: number }
  | { type: "unread_private_messages"; messages: UnreadPrivateDetail[] }
  | { type: "unread_room_messages"; room: string; count: number }
  | { type: "online_users"; users: OnlineUserInfo[] };
```

## Rust/Yew Client Notes

A Rust or Yew client should mirror the server's externally visible serde shape:

```rust
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientMessage {
    JoinRoom { room: String },
    LeaveRoom { room: String },
    SendToRoom {
        #[serde(with = "uuid_bytes")]
        uuid: Uuid,
        room: String,
        content: String
    },
    SendDirect {
        #[serde(with = "uuid_bytes")]
        uuid: Uuid,
        #[serde(with = "uuid_bytes")]
        to_user_uuid: Uuid,
        content: String
    }
}

pub fn encode_client_message(msg: &ClientMessage) -> Vec<u8> {
    rmp_serde::to_vec_named(msg).expect("client message encodes")
}
```

For a complete client, add the remaining variants from the "Client Messages"
section with the same `#[serde(tag = "type", rename_all = "snake_case")]`
layout. UUID serde must encode/decode 16 raw bytes. If the client crate cannot
reuse Claviron's helper directly, implement an equivalent `uuid_bytes` module
locally.

## Client State Rules

- Use `uuid` as the stable message key. Generate it before sending.
- Render outgoing messages optimistically, then merge the eventual server event
  by UUID.
- Keep pending distinct from server-confirmed `sent`. Merge private statuses
  monotonically from `sent` to `delivered` to `read`; keep failures visible.
- Merge room `read_by` identities by UUID. An empty delivered snapshot must not
  clear a known reader, and one room receipt must not be presented as all-read.
- Treat `message_status.failed` and `error` as authoritative failure signals.
- Treat `welcome` as a full unread badge refresh.
- Treat `online_users` as a snapshot and `user_online` / `user_offline` as
  incremental updates.
- Keep typing indicators ephemeral and clear them locally after a short UI
  timeout.
- Reconnect with exponential backoff. After reconnect, request room/history
  state needed by the visible UI.

## WebSocket Client Skeleton

```ts
import { decode, encode } from "@msgpack/msgpack";

type UuidBytes = Uint8Array;

export class ClavironChatSocket {
  private ws?: WebSocket;

  constructor(
    private readonly url: string,
    private readonly token: string,
    private readonly onMessage: (message: any) => void,
  ) {}

  connect() {
    this.ws = new WebSocket(this.url, [bearerSubprotocol(this.token)]);
    this.ws.binaryType = "arraybuffer";
    this.ws.onmessage = (event) => {
      this.onMessage(decode(new Uint8Array(event.data)));
    };
    this.ws.onclose = () => {
      this.onMessage({ type: "transport_closed" });
    };
  }

  send(message: unknown) {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) return;
    this.ws.send(encode(message));
  }
}
```

This minimal encoding example does not own reconnection. Prefer `ChatClient`
for its configured backoff, single pending retry and explicit-disconnect
cancellation rather than adding an unowned retry loop to a UI component.

## H2 CONNECT Client Notes

An H2 CONNECT client needs:

- HTTP/2 TLS connection to the vhost.
- `CONNECT /chat/h2`
- `Authorization: Bearer <orbit-token>` or `Authorization: Ticket <ticket>`
- Request body stream for client frames.
- Response body stream for server frames.
- `u32be` frame parser/writer around MessagePack payloads.

There is no chat-level ping requirement for H2. Connection keepalive and idle
policy belong to the HTTP/2 transport layer.

## WebTransport Client Notes

A WebTransport client needs:

- HTTP/3/WebTransport session to `/chat/wt`.
- `Authorization: Bearer <orbit-token>` or `Authorization: Ticket <ticket>`.
- One bidirectional stream opened after the session is accepted.
- `u32be` frame parser/writer around MessagePack payloads.

There is no chat-level ping requirement for WebTransport. H3/QUIC transport
keepalive owns liveness.

## Raw QUIC Demo Notes

Raw QUIC is kept as a demo/prototype transport, not the main production chat
topology. It uses a legacy envelope and little-endian length prefix. New client
implementations should prefer WebSocket, H2 CONNECT, or WebTransport unless they
are explicitly testing the raw QUIC prototype.
