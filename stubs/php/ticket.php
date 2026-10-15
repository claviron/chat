<?php

declare(strict_types=1);

namespace Claviron\Chat;

/**
 * Issue a ticket for an identity already authenticated by the host application.
 *
 * $put stores raw JSON at the supplied key with the supplied TTL in seconds.
 * The backing store and effective key prefix must match chat.yaml's ticket guard.
 * No user lookup, session creation or framework-specific storage is performed.
 */
function issue_ticket(
    array $user,
    array $endpoints,
    int $ttl,
    string $keyPrefix,
    callable $put,
): array {
    if ($ttl <= 0 || $keyPrefix === '') {
        throw new \InvalidArgumentException('A positive ticket TTL and explicit key prefix are required.');
    }
    foreach (['id', 'uuid', 'username', 'name', 'avatar_url'] as $key) {
        if (!array_key_exists($key, $user)) {
            throw new \InvalidArgumentException("Missing chat identity field: {$key}");
        }
    }
    foreach (['websocket_url', 'h2_connect_url', 'web_transport_url'] as $key) {
        if (empty($endpoints[$key])) {
            throw new \InvalidArgumentException("Missing chat endpoint: {$key}");
        }
    }
    $ticket = bin2hex(random_bytes(32));
    $payload = [
        'v' => 1,
        'id' => (int) $user['id'],
        'uuid' => (string) $user['uuid'],
        'username' => (string) $user['username'],
        'name' => (string) $user['name'],
        'avatar_url' => (string) $user['avatar_url'],
    ];
    $put($keyPrefix.hash('sha256', $ticket), json_encode($payload, JSON_THROW_ON_ERROR), $ttl);

    return [
        'guard' => 'ticket',
        'ticket' => $ticket,
        'expires_in' => $ttl,
        'websocket_url' => $endpoints['websocket_url'],
        'h2_connect_url' => $endpoints['h2_connect_url'],
        'web_transport_url' => $endpoints['web_transport_url'],
        'websocket_subprotocol' => 'ticket.'.rtrim(strtr(base64_encode($ticket), '+/', '-_'), '='),
        'authorization' => 'Bearer '.$ticket,
    ];
}
