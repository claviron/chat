import { decode, encode } from "@msgpack/msgpack";

const DEFAULT_TICKET_URL = "/chat/ticket";
const DEFAULT_ROOM = "operators";
const RECONNECT_MIN_MS = 500;
const RECONNECT_MAX_MS = 15_000;

export class ChatClient extends EventTarget {
  constructor(options = {}) {
    super();
    this.options = {
      auth: "ticket",
      ticketUrl: DEFAULT_TICKET_URL,
      transport: "websocket",
      reconnect: true,
      reconnectMinDelay: RECONNECT_MIN_MS,
      reconnectMaxDelay: RECONNECT_MAX_MS,
      fetchOptions: {},
      ...options,
    };
    this.socket = null;
    this.boot = null;
    this.connected = false;
    this.connecting = false;
    this.closedByClient = false;
    this.reconnectAttempt = 0;
  }

  async connect() {
    if (this.connected || this.connecting) return this;
    this.connecting = true;
    this.closedByClient = false;
    try {
      this.boot = await this.bootstrap();
      if (this.options.transport !== "websocket" && this.options.transport !== "auto") {
        throw new Error(`unsupported browser transport: ${this.options.transport}`);
      }
      await this.connectWebSocket(this.boot);
      return this;
    } finally {
      this.connecting = false;
    }
  }

  disconnect() {
    this.closedByClient = true;
    this.connected = false;
    if (this.socket) {
      this.socket.close(1000, "client disconnect");
      this.socket = null;
    }
    this.emit("state", { state: "disconnected" });
  }

  async fetchTicket() {
    const base = {
      method: "POST",
      credentials: "same-origin",
      headers: { Accept: "application/json" },
    };
    const response = await fetch(this.options.ticketUrl, {
      ...base,
      ...this.options.fetchOptions,
      headers: {
        ...base.headers,
        ...(this.options.fetchOptions?.headers || {}),
      },
    });
    if (!response.ok) {
      throw new Error(`chat ticket failed: HTTP ${response.status}`);
    }
    return response.json();
  }

  connectWebSocket(boot) {
    return new Promise((resolve, reject) => {
      if (!boot?.websocket_url || (boot.guard !== "session" && !boot.websocket_subprotocol)) {
        reject(new Error("chat ticket response is missing websocket binding"));
        return;
      }

      const socket = boot.guard === "session"
        ? new WebSocket(boot.websocket_url)
        : new WebSocket(boot.websocket_url, [boot.websocket_subprotocol]);
      socket.binaryType = "arraybuffer";
      this.socket = socket;

      socket.onopen = () => {
        this.connected = true;
        this.reconnectAttempt = 0;
        this.emit("state", { state: "connected", boot });
        resolve(this);
      };

      socket.onmessage = async (event) => {
        try {
          const payload = await wsPayloadBytes(event.data);
          const message = decode(payload);
          this.emit("message", message);
          if (message?.type) this.emit(message.type, message);
        } catch (error) {
          this.emit("error", { error });
        }
      };

      socket.onerror = () => {
        const error = new Error("chat websocket error");
        this.emit("error", { error });
        if (!this.connected) reject(error);
      };

      socket.onclose = (event) => {
        const wasConnected = this.connected;
        this.connected = false;
        this.socket = null;
        this.emit("state", { state: "closed", event });
        if (wasConnected && !this.closedByClient && this.options.reconnect) {
          this.scheduleReconnect();
        }
      };
    });
  }

  scheduleReconnect() {
    const delay = Math.min(
      this.options.reconnectMaxDelay,
      this.options.reconnectMinDelay * 2 ** this.reconnectAttempt,
    );
    this.reconnectAttempt += 1;
    this.emit("state", { state: "reconnecting", delay });
    setTimeout(() => {
      if (!this.closedByClient) {
        this.connect().catch((error) => {
          this.emit("error", { error });
          this.scheduleReconnect();
        });
      }
    }, delay);
  }

  async bootstrap() {
    switch (this.options.auth) {
      case "ticket":
        return this.fetchTicket();
      case "orbit":
        return this.orbitBoot();
      case "session": {
        const address = this.options.websocketUrl || this.options.websocket_url;
        if (!address) throw new Error("session chat auth requires websocketUrl");
        const url = new URL(address, globalThis.location.href);
        url.protocol = url.protocol === "https:" || url.protocol === "wss:" ? "wss:" : "ws:";
        const protocol = globalThis.location.protocol === "https:" ? "wss:" : "ws:";
        if (url.host !== globalThis.location.host || url.protocol !== protocol) {
          throw new Error("session chat requires the current origin");
        }
        return {guard: "session", websocket_url: url.href};
      }
      default:
        throw new Error(`unsupported chat auth mode: ${this.options.auth}`);
    }
  }

  send(message) {
    if (!this.socket || this.socket.readyState !== WebSocket.OPEN) {
      throw new Error("chat websocket is not open");
    }
    this.socket.send(encode(message));
  }

  joinRoom(room = DEFAULT_ROOM) {
    this.send({ type: "join_room", room });
  }

  leaveRoom(room = DEFAULT_ROOM) {
    this.send({ type: "leave_room", room });
  }

  listRooms() {
    this.send({ type: "list_rooms" });
  }

  listUsers(room = DEFAULT_ROOM) {
    this.send({ type: "list_users", room });
  }

  onlineUsers() {
    this.send({ type: "get_online_users" });
  }

  typing(room = DEFAULT_ROOM) {
    this.send({ type: "typing", room });
  }

  sendToRoom(room, content, uuid = uuidBytes()) {
    this.send({ type: "send_to_room", uuid, room, content });
    return uuid;
  }

  sendDirect(toUserUuid, content, uuid = uuidBytes()) {
    this.send({
      type: "send_direct",
      uuid,
      to_user_uuid: uuidToBytes(toUserUuid),
      content,
    });
    return uuid;
  }

  history(room = DEFAULT_ROOM, options = {}) {
    this.send({ type: "get_history", room, ...historyWindow(options) });
  }

  markDelivered(uuid, room = null) {
    this.send({ type: "mark_delivered", uuid: uuidToBytes(uuid), room });
  }

  markRead(uuid, room = null) {
    this.send({ type: "mark_read", uuid: uuidToBytes(uuid), room });
  }

  on(type, handler, options) {
    const wrapped = (event) => handler(event.detail);
    this.addEventListener(type, wrapped, options);
    return () => this.removeEventListener(type, wrapped, options);
  }

  emit(type, detail) {
    this.dispatchEvent(new CustomEvent(type, { detail }));
  }

  orbitBoot() {
    const websocketUrl = this.options.websocketUrl || this.options.websocket_url;
    const websocketSubprotocol =
      this.options.websocketSubprotocol
      || this.options.websocket_subprotocol
      || bearerSubprotocol(this.options.token);
    if (!websocketUrl) {
      throw new Error("orbit chat auth requires websocketUrl");
    }
    return {
      guard: "orbit",
      websocket_url: websocketUrl,
      websocket_subprotocol: websocketSubprotocol,
    };
  }
}

export function mount(target, options = {}) {
  const host = typeof target === "string" ? document.querySelector(target) : target;
  if (!host) throw new Error("chat mount target not found");

  const room = options.room || DEFAULT_ROOM;
  const client = new ChatClient(options);
  const { root, container, navItem } = createWidgetRoot(host, options);
  root.appendChild(widgetTemplate({ navItem, icon: options.icon, iconHtml: options.iconHtml }));

  const button = root.querySelector("[data-chat-toggle]");
  const panel = root.querySelector("[data-chat-panel]");
  const closeButton = root.querySelector("[data-chat-close]");
  const status = root.querySelector("[data-chat-status]");
  const badge = root.querySelector("[data-chat-badge]");
  const list = root.querySelector("[data-chat-messages]");
  const form = root.querySelector("[data-chat-form]");
  const input = root.querySelector("[data-chat-input]");

  let open = false;
  let unread = 0;
  let joined = false;

  const setOpen = async (value) => {
    open = value;
    panel.hidden = !open;
    button.setAttribute("aria-expanded", String(open));
    if (open) {
      unread = 0;
      badge.hidden = true;
      input.focus();
      if (!client.connected && !client.connecting) {
        await client.connect();
      }
      if (client.connected && !joined) {
        client.joinRoom(room);
        joined = true;
      }
    }
  };

  button.addEventListener("click", (event) => {
    event.preventDefault();
    event.stopPropagation();
    setOpen(!open).catch((error) => renderSystem(list, error.message, "error"));
  });

  button.addEventListener("keydown", (event) => {
    if (event.key !== "Enter" && event.key !== " ") return;
    event.preventDefault();
    event.stopPropagation();
    setOpen(!open).catch((error) => renderSystem(list, error.message, "error"));
  });

  closeButton.addEventListener("click", () => {
    setOpen(false).catch((error) => renderSystem(list, error.message, "error"));
  });

  form.addEventListener("submit", (event) => {
    event.preventDefault();
    const content = input.value.trim();
    if (!content) return;
    input.value = "";
    client.sendToRoom(room, content);
  });

  client.on("state", ({ state }) => {
    status.textContent = state;
    if (state === "connected" && !joined) {
      client.joinRoom(room);
      joined = true;
    }
    if (state === "closed") joined = false;
  });

  client.on("welcome", (message) => {
    renderSystem(list, message.message || `connected as ${message.username}`);
    const count = Number(message.unread?.private_messages_count || 0)
      + (message.unread?.room_messages || []).reduce((sum, item) => sum + Number(item.count || 0), 0);
    if (count > 0) {
      unread += count;
      badge.textContent = String(unread);
      badge.hidden = open;
    }
  });

  client.on("room_message", (message) => {
    renderMessage(list, message, client.boot);
    if (!open) {
      unread += 1;
      badge.textContent = String(unread);
      badge.hidden = false;
    }
  });

  client.on("direct_message", (message) => {
    renderMessage(list, message, client.boot);
    if (!open) {
      unread += 1;
      badge.textContent = String(unread);
      badge.hidden = false;
    }
  });

  client.on("system", (message) => {
    if (options.showSystemMessages) renderSystem(list, message.content);
  });
  client.on("success", (message) => {
    if (options.showSystemMessages) renderSystem(list, message.message);
  });
  client.on("error", ({ error, message }) => renderSystem(list, message || error?.message || "chat error", "error"));

  if (options.connectOnMount) {
    client.connect().catch((error) => renderSystem(list, error.message, "error"));
  }

  return {
    client,
    open: () => setOpen(true),
    close: () => setOpen(false),
    destroy: () => {
      client.disconnect();
      if (container === host) {
        root.replaceChildren();
      } else {
        container.remove();
      }
    },
  };
}

export function uuidBytes() {
  if (crypto.randomUUID) return uuidToBytes(crypto.randomUUID());
  const bytes = new Uint8Array(16);
  crypto.getRandomValues(bytes);
  bytes[6] = (bytes[6] & 0x0f) | 0x40;
  bytes[8] = (bytes[8] & 0x3f) | 0x80;
  return bytes;
}

export function uuidToBytes(uuid) {
  if (uuid instanceof Uint8Array) return uuid;
  const hex = String(uuid).replace(/-/g, "");
  if (!/^[0-9a-fA-F]{32}$/.test(hex)) throw new Error("invalid uuid");
  const bytes = new Uint8Array(16);
  for (let i = 0; i < 16; i += 1) {
    bytes[i] = Number.parseInt(hex.slice(i * 2, i * 2 + 2), 16);
  }
  return bytes;
}

export function uuidFromBytes(bytes) {
  if (!bytes || bytes.length !== 16) return "";
  const hex = [...bytes].map((byte) => byte.toString(16).padStart(2, "0")).join("");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

function historyWindow(options) {
  const out = {};
  if (options.limit) out.limit = options.limit;
  if (options.since) out.since = toUnixSeconds(options.since);
  if (options.before) out.before = toUnixSeconds(options.before);
  return out;
}

async function wsPayloadBytes(data) {
  if (data instanceof ArrayBuffer) return new Uint8Array(data);
  if (data instanceof Uint8Array) return data;
  if (typeof Blob !== "undefined" && data instanceof Blob) {
    return new Uint8Array(await data.arrayBuffer());
  }
  throw new Error("unsupported chat websocket payload");
}

function toUnixSeconds(value) {
  if (typeof value === "number") return Math.floor(value);
  return Math.floor(new Date(value).getTime() / 1000);
}

function bearerSubprotocol(token) {
  if (!token) throw new Error("orbit chat auth requires token");
  const clean = String(token).replace(/^Bearer\s+/i, "").trim();
  return `bearer.${base64Url(clean)}`;
}

function base64Url(value) {
  const bytes = new TextEncoder().encode(value);
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
}

function widgetTemplate(options = {}) {
  const navItem = Boolean(options.navItem);
  const iconHtml = chatIconMarkup(options);
  const fragment = document.createDocumentFragment();
  const style = document.createElement("style");
  style.textContent = `
    :host{font:14px/1.4 system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#15171a}
    .cv-chat-mounted{position:relative}
    .cv-chat{position:relative;display:inline-flex;align-items:center;justify-content:center;min-height:44px;vertical-align:middle}
    .cv-chat-nav-toggle{display:flex;align-items:center;justify-content:center}
    .cv-toggle{width:34px;height:34px;border:0;border-radius:7px;background:transparent;color:#47515f;cursor:pointer;display:grid;place-items:center}
    .cv-toggle:hover,.cv-close:hover{background:#eef2f7;color:#15171a}
    .cv-chat-nav-toggle.cv-toggle{width:auto;height:50px;min-height:0;padding-top:0;padding-bottom:0;border-radius:0;color:inherit;text-decoration:none}
    .cv-chat-nav-toggle.cv-toggle:hover{background:transparent;color:inherit;text-decoration:none}
    .cv-chat-nav-toggle .cv-fa-icon{font-size:22px;line-height:1}
    .cv-toggle svg{width:19px;height:19px;stroke:currentColor}
    .cv-close svg{width:18px;height:18px;stroke:currentColor}
    .cv-toggle:focus:not(:focus-visible),.cv-close:focus:not(:focus-visible){outline:0}
    .cv-toggle:focus-visible,.cv-close:focus-visible,.cv-send:focus-visible,.cv-input:focus-visible{outline:2px solid rgba(44,123,229,.55);outline-offset:1px}
    .cv-badge{position:absolute;top:1px;right:1px;min-width:15px;height:15px;padding:0 4px;box-sizing:border-box;border:2px solid #fff;border-radius:999px;background:#d83a34;color:#fff;font-size:10px;font-weight:700;line-height:11px;text-align:center}
    .cv-chat-mounted>.cv-badge{top:7px;right:8px}
    .cv-panel{position:absolute;right:0;top:44px;width:min(360px,calc(100vw - 24px));height:440px;background:#fff;border:1px solid #d6dae0;border-radius:8px;box-shadow:0 12px 40px rgba(14,20,31,.2);display:grid;grid-template-rows:auto 1fr auto;overflow:hidden;z-index:1000}
    .cv-chat-mounted>.cv-panel{top:100%}
    .cv-panel[hidden]{display:none}
    .cv-head{display:flex;align-items:center;justify-content:space-between;padding:10px 12px;border-bottom:1px solid #eceff3;background:#f8f9fb}
    .cv-head-main{min-width:0}.cv-close{width:30px;height:30px;border:0;border-radius:8px;background:transparent;color:#68707d;cursor:pointer;display:grid;place-items:center}
    .cv-title{font-weight:650}.cv-status{font-size:12px;color:#68707d}
    .cv-messages{padding:12px;overflow:auto;display:flex;flex-direction:column;gap:8px;background:#fbfcfe}
    .cv-msg{max-width:88%;padding:8px 10px;border-radius:8px;background:#eef2f7;word-wrap:break-word}
    .cv-msg.own{align-self:flex-end;background:#dff0ff}
    .cv-meta{display:flex;gap:6px;align-items:center;font-size:12px;color:#68707d;margin-bottom:3px}
    .cv-avatar{width:20px;height:20px;border-radius:50%;object-fit:cover;background:#dde2e8}
    .cv-system{align-self:center;font-size:12px;color:#68707d}.cv-system.error{color:#b42318}
    .cv-form{display:grid;grid-template-columns:1fr auto;gap:8px;padding:10px;border-top:1px solid #eceff3;background:#fff}
    .cv-input{min-width:0;border:1px solid #cfd5dd;border-radius:8px;padding:8px 10px;font:inherit}
    .cv-send{border:0;border-radius:8px;background:#171a1f;color:#fff;padding:0 12px;font:inherit;cursor:pointer}
  `;
  const markup = `
    <a class="cv-toggle cv-chat-nav-toggle" role="button" tabindex="0" data-chat-toggle aria-label="Chat" aria-expanded="false">
      ${iconHtml}
    </a>
    <span class="cv-badge" data-chat-badge hidden></span>
    <section class="cv-panel" data-chat-panel hidden>
      <header class="cv-head">
        <div class="cv-head-main"><span class="cv-title">Chat</span><span class="cv-status" data-chat-status>idle</span></div>
        <button class="cv-close" type="button" data-chat-close aria-label="Close chat">
          <svg viewBox="0 0 24 24" aria-hidden="true" fill="none" stroke-width="2" stroke-linecap="round">
            <path d="M18 6 6 18"></path><path d="m6 6 12 12"></path>
          </svg>
        </button>
      </header>
      <div class="cv-messages" data-chat-messages></div>
      <form class="cv-form" data-chat-form>
        <input class="cv-input" data-chat-input autocomplete="off" />
        <button class="cv-send" type="submit">Send</button>
      </form>
    </section>
  `;
  if (navItem) {
    const template = document.createElement("template");
    template.innerHTML = markup;
    fragment.append(style, template.content);
    return fragment;
  }

  const wrapper = document.createElement("div");
  wrapper.className = "cv-chat";
  wrapper.innerHTML = markup.replace("cv-chat-nav-toggle", "");
  fragment.append(style, wrapper);
  return fragment;
}

function chatIconMarkup(options = {}) {
  if (options.iconHtml) return String(options.iconHtml);
  if (options.navItem) {
    const icon = options.icon || "fa-comment-o";
    if (icon.startsWith("fa-")) {
      return `<i class="fa ${escapeClassToken(icon)} cv-fa-icon" aria-hidden="true"></i>`;
    }
  }
  return `<svg viewBox="0 0 24 24" aria-hidden="true" fill="none" stroke-width="2.1" stroke-linecap="round" stroke-linejoin="round">
    <path d="M7.9 19.2a9 9 0 1 0-3.1-3.1L3.6 20.4z"></path>
    <path d="M8.2 12h.01"></path><path d="M12 12h.01"></path><path d="M15.8 12h.01"></path>
  </svg>`;
}

function escapeClassToken(value) {
  return String(value).replace(/[^a-zA-Z0-9_-]/g, "");
}

function createWidgetRoot(host, options = {}) {
  const navItem = options.navItem ?? isNavListItem(host);
  if (navItem) {
    host.classList.add("cv-chat-mounted");
    return { root: host, container: host, navItem: true };
  }

  if (host.shadowRoot) {
    return { root: host.shadowRoot, container: host, navItem: false };
  }

  if (host.attachShadow) {
    try {
      return { root: host.attachShadow({ mode: "open" }), container: host, navItem: false };
    } catch (error) {
      if (error?.name !== "NotSupportedError") throw error;
    }
  }

  const container = document.createElement("div");
  container.setAttribute("data-claviron-chat-host", "");
  host.appendChild(container);
  if (container.attachShadow) {
    return { root: container.attachShadow({ mode: "open" }), container, navItem: false };
  }
  return { root: container, container, navItem: false };
}

function isNavListItem(host) {
  return host?.tagName === "LI" && host.closest?.(".navbar-nav,.nav");
}

function renderMessage(list, message, boot) {
  const item = document.createElement("article");
  const own = sameUuid(message.from_user_uuid, boot?.user_uuid);
  item.className = `cv-msg${own ? " own" : ""}`;
  const meta = document.createElement("div");
  meta.className = "cv-meta";
  if (message.from_avatar_url) {
    const avatar = document.createElement("img");
    avatar.className = "cv-avatar";
    avatar.src = message.from_avatar_url;
    avatar.alt = "";
    meta.appendChild(avatar);
  }
  const name = document.createElement("span");
  name.textContent = message.from_username || "user";
  meta.appendChild(name);
  const body = document.createElement("div");
  body.textContent = message.content || "";
  item.append(meta, body);
  list.appendChild(item);
  list.scrollTop = list.scrollHeight;
}

function renderSystem(list, text, kind = "") {
  const item = document.createElement("div");
  item.className = `cv-system ${kind}`.trim();
  item.textContent = text;
  list.appendChild(item);
  list.scrollTop = list.scrollHeight;
}

function sameUuid(left, right) {
  if (!left || !right) return false;
  return uuidFromBytes(left) === uuidFromBytes(right);
}

export default { ChatClient, mount, uuidBytes, uuidToBytes, uuidFromBytes };
