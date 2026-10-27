# Claviron Chat

Chat for applications served by Claviron. Users can join rooms, exchange private
messages, browse message history and see who is online. Delivery and read receipts
track messages across a user's connections.

The service runs inside your Claviron server. It shares the server's HTTP listeners
and uses your application's user provider and authentication. PostgreSQL stores
messages and receipts; Orbit carries delivery and presence between workers.

## Integration

Register Chat during your server's provider registration:

```rust,ignore
claviron_chat::register(state)?;
```

Configure its endpoints in `chat.yaml` under the server's config directory. The
`embed` section selects existing vhost keys and the authentication guard. Supply
Claviron's `UserProvider`, the selected guard's authenticator and a `DbService`
with the `chat` schema installed. User lookup remains with your application.

Chat serves WebSocket over HTTP/1.1 or HTTP/2 and a separate HTTP/2 CONNECT message
stream. The `webtransport` feature adds HTTP/3 WebTransport. Each WebTransport
stream has independent input, output and room membership, with bounded queues
and graceful draining.

Message quotas can use a user or session key. User quotas cover all connections
for the same UUID and survive reconnects. Configure `LocalGuard` for a process
or `FleetGuard` for a shared worker budget. Optional application providers supply
a user directory and avatar URLs.

## Features

- `webtransport`: HTTP/3 WebTransport through Claviron's listener.
- `log-pipeline`: Chat records in Claviron's configured log channels.
- `metrics`: Chat connection and message counters.
- `quic`: experimental raw QUIC transport with its own configured listener.

This release targets Claviron `0.2.0-alpha.23`. Add the service with
`claviron-chat = "0.2.0-alpha.16"`. The browser client in `client/` is built
separately; see the
[client API](CHAT_CLIENT_API.md) for messages and transport framing and
[architecture](ARCHITECTURE.md) for configuration and lifecycle details.

## License

MIT OR Apache-2.0.
