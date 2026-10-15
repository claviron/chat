import assert from 'node:assert/strict';
import {test} from 'node:test';
import {ChatClient} from '../src/index.js';

test('initial failure, reload closure and failed restart retry recover without duplicate timers', async () => {
  const original = {WebSocket: globalThis.WebSocket, setTimeout, clearTimeout};
  const timers = new Map();
  const sockets = [];
  let sequence = 0;
  class Socket {
    static OPEN = 1;
    readyState = 0;
    constructor() { sockets.push(this); }
    open() { this.readyState = 1; this.onopen(); }
    close(code = 1000) { this.readyState = 3; this.onclose({code}); }
  }
  globalThis.WebSocket = Socket;
  globalThis.setTimeout = (callback, delay) => {
    const id = ++sequence;
    timers.set(id, {callback, delay});
    return id;
  };
  globalThis.clearTimeout = id => timers.delete(id);
  try {
    const client = new ChatClient();
    client.bootstrap = async () => ({guard: 'session', websocket_url: 'wss://example.test/chat/ws'});
    const connection = client.connect();
    const rejected = assert.rejects(connection, /closed before opening/);
    await Promise.resolve();
    sockets[0].close();
    await rejected;
    assert.equal(client.connecting, false);
    assert.equal(timers.size, 1);
    const [id, timer] = [...timers.entries()][0];
    assert.equal(timer.delay, client.options.reconnectMinDelay);
    timers.delete(id);
    timer.callback();
    await Promise.resolve();
    sockets[1].open();
    await Promise.resolve();
    assert.equal(client.connected, true);
    // Graceful vhost retirement is not an explicit client disconnect.
    sockets[1].close();
    assert.equal(timers.size, 1);
    const [reloadId, reloadTimer] = [...timers.entries()][0];
    assert.equal(reloadTimer.delay, client.options.reconnectMinDelay);
    timers.delete(reloadId);
    reloadTimer.callback();
    await Promise.resolve();
    // The server is still restarting when the first reconnect reaches it.
    sockets[2].close(1006);
    await Promise.resolve();
    await Promise.resolve();
    assert.equal(timers.size, 1);
    const [retryId, retryTimer] = [...timers.entries()][0];
    assert.equal(retryTimer.delay, Math.min(client.options.reconnectMaxDelay, client.options.reconnectMinDelay * 2));
    timers.delete(retryId);
    retryTimer.callback();
    await Promise.resolve();
    sockets[3].open();
    await Promise.resolve();
    assert.equal(client.connected, true);
    assert.equal(client.reconnectAttempt, 0);
    assert.equal(timers.size, 0);
    sockets[3].close();
    assert.equal(timers.size, 1);
    client.disconnect();
    assert.equal(timers.size, 0);

    let release;
    const cancelled = new ChatClient();
    cancelled.bootstrap = () => new Promise(resolve => {release = resolve;});
    const pending = cancelled.connect();
    cancelled.disconnect();
    release({guard: 'session', websocket_url: 'wss://example.test/chat/ws'});
    await pending;
    assert.equal(sockets.length, 4);
    assert.equal(timers.size, 0);
  } finally {
    globalThis.WebSocket = original.WebSocket;
    globalThis.setTimeout = original.setTimeout;
    globalThis.clearTimeout = original.clearTimeout;
  }
});
