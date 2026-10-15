# PHP ticket issuer stub

`php/ticket.php` is a framework-neutral extraction of the former application
ticket issuer. Copy/autoload it into an application that needs cross-origin
ticket authentication. The caller must authenticate and authorize the user
before calling it; never accept the identity snapshot from request input.

Pass the existing local numeric user ID, UUID, username, display name and final
avatar URL. The helper neither queries an application's schema nor creates
sessions. Its wire payload is version 1. A positive TTL, endpoint URLs, effective
key prefix and raw-JSON storage callback are explicit application inputs.
Configure the same shared store/prefix/version in `chat.yaml`, with `ticket`
enabled. Let the host's normal auth, CSRF and issuance limits protect the route.

For Laravel, call the helper from an authenticated controller and return
`response()->json($result)->header('Cache-Control', 'private, no-store')`.
A raw Redis writer can use the application's selected connection:

```php
$put = static function (string $key, string $json, int $ttl) use ($connection): void {
    if (!Redis::connection($connection)->setex($key, $ttl, $json)) {
        throw new RuntimeException('Chat ticket storage failed.');
    }
};
```

Include the connection's actual Redis prefix when matching keys across runtimes.
Ordinary Laravel cache serialization is not interchangeable with raw JSON;
choose a compatible storage adapter deliberately. Do not log/cache the issued
ticket response in a public cache. Ticket consumption remains the configured
chat guard's responsibility and retains its documented best-effort semantics.

Same-origin hosts can use `auth: "session"` instead. That mode needs no ticket
endpoint or PHP ticket storage; the browser sends its existing credential cookie.
