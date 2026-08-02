import crypto from "node:crypto";
import os from "node:os";
import process from "node:process";
import WebSocket from "ws";
import { createClient } from "@supabase/supabase-js";

const required = ["SUPABASE_URL", "FRA_LIVE_SYNC_WORKER_SECRET"];
for (const name of required) {
  if (!process.env[name]) throw new Error(`${name} is required`);
}
const SERVICE_ROLE_KEY = process.env.SUPABASE_SECRET_KEY || process.env.SUPABASE_SERVICE_ROLE_KEY;
if (!SERVICE_ROLE_KEY) throw new Error("SUPABASE_SECRET_KEY or SUPABASE_SERVICE_ROLE_KEY is required");

const SUPABASE_URL = process.env.SUPABASE_URL.replace(/\/$/, "");
const WORKER_SECRET = process.env.FRA_LIVE_SYNC_WORKER_SECRET;
const TARGET_REFRESH_MS = Number(process.env.TARGET_REFRESH_MS || 30000);
const PULSE_MIN_INTERVAL_MS = Number(process.env.PULSE_MIN_INTERVAL_MS || 2500);
const HEARTBEAT_MS = Number(process.env.HEARTBEAT_MS || 15000);
const TRADOVATE_HEARTBEAT_MS = Math.max(1000, Math.min(Number(process.env.TRADOVATE_HEARTBEAT_MS || 2000), 2400));
const INSTANCE_ID = `${os.hostname()}-${process.pid}-${crypto.randomBytes(3).toString("hex")}`;
const VERSION = "7.7.6";

const supabase = createClient(SUPABASE_URL, SERVICE_ROLE_KEY, {
  auth: { persistSession: false, autoRefreshToken: false },
  realtime: { transport: WebSocket },
});

const sessions = new Map();
let stopping = false;

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const nowIso = () => new Date().toISOString();
const asText = value => String(value ?? "");

async function requestWorkerSession(connectionId) {
  const response = await fetch(`${SUPABASE_URL}/functions/v1/tradovate-live-pulse`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Authorization": `Bearer ${SERVICE_ROLE_KEY}`,
      "apikey": SERVICE_ROLE_KEY,
      "x-fra-worker-secret": WORKER_SECRET,
    },
    body: JSON.stringify({ connection_id: connectionId, action: "session" }),
  });
  const payload = await response.json().catch(() => ({}));
  if (!response.ok || payload?.error) {
    throw new Error(payload?.error || `Unable to obtain Tradovate live session (${response.status})`);
  }
  const token = asText(payload.access_token).trim();
  const userIds = Array.isArray(payload.user_ids)
    ? payload.user_ids.map(Number).filter(Number.isFinite)
    : [];
  if (!token) throw new Error("Supabase did not return a Tradovate access token");
  if (!userIds.length) throw new Error("Tradovate returned no user IDs for live sync");
  return {
    token,
    userIds,
    environment: payload.environment === "live" ? "live" : "demo",
    wsUrl: asText(payload.websocket_url).trim(),
  };
}

function isPaidTarget(ownerId, profileById, subscriptionByOwner) {
  const profile = profileById.get(ownerId) || {};
  const subscription = subscriptionByOwner.get(ownerId) || {};
  const role = asText(profile.role).toLowerCase();
  const plan = asText(profile.plan || subscription.plan).toLowerCase();
  const status = asText(subscription.status).toLowerCase();
  if (role === "admin" || ["founder", "business", "desk"].includes(plan)) return true;
  return ["entry", "plus", "pro"].includes(plan) && ["active", "trialing", "past_due"].includes(status);
}

async function loadTargets() {
  const { data: connections, error } = await supabase.from("provider_connections")
    .select("id,owner_id,provider,environment,status,display_name,metadata,live_sync_enabled")
    .eq("provider", "tradovate")
    .in("status", ["connected", "syncing"])
    .eq("live_sync_enabled", true);
  if (error) throw error;
  const ownerIds = [...new Set((connections || []).map(row => row.owner_id).filter(Boolean))];
  if (!ownerIds.length) return [];
  const [{ data: profiles, error: profileError }, { data: subscriptions, error: subscriptionError }] = await Promise.all([
    supabase.from("profiles").select("id,role,plan").in("id", ownerIds),
    supabase.from("subscriptions").select("owner_id,plan,status,current_period_end").in("owner_id", ownerIds),
  ]);
  if (profileError) throw profileError;
  if (subscriptionError) throw subscriptionError;
  const profileById = new Map((profiles || []).map(row => [row.id, row]));
  const subscriptionByOwner = new Map((subscriptions || []).map(row => [row.owner_id, row]));
  return (connections || []).filter(row => isPaidTarget(row.owner_id, profileById, subscriptionByOwner));
}

async function writeStatus(connection, patch) {
  const environment = asText(connection?.metadata?.technical_environment || connection.environment) === "live" ? "live" : "demo";
  const { error } = await supabase.from("tradovate_live_status").upsert({
    connection_id: connection.id,
    owner_id: connection.owner_id,
    provider_environment: environment,
    worker_instance: INSTANCE_ID,
    transport: "websocket",
    ...patch,
    updated_at: nowIso(),
  }, { onConflict: "connection_id" });
  if (error) console.error("status update failed", connection.id, error.message);
  await supabase.from("provider_connections").update({
    live_sync_state: patch.state || "live",
    live_sync_last_event_at: patch.last_event_at,
    live_sync_last_heartbeat_at: patch.last_heartbeat_at || nowIso(),
  }).eq("id", connection.id);
}

function sendRequest(ws, endpoint, id, body) {
  const payload = body === undefined || body === null
    ? `${endpoint}\n${id}\n\n`
    : `${endpoint}\n${id}\n\n${typeof body === "string" ? body : JSON.stringify(body)}`;
  ws.send(payload);
}

function frameError(frame, fallback) {
  const detail = frame?.d;
  if (typeof detail === "string" && detail.trim()) return detail.trim();
  if (detail && typeof detail === "object") {
    return asText(detail.errorText || detail.error || detail.message || fallback);
  }
  return fallback;
}

function unpackFrame(rawValue) {
  const raw = asText(rawValue);
  if (!raw || raw === "o" || raw === "h") return [];
  if (!raw.startsWith("a")) return [];
  let outer;
  try { outer = JSON.parse(raw.slice(1)); } catch { return []; }
  const items = [];
  for (const value of Array.isArray(outer) ? outer : [outer]) {
    if (typeof value === "string") {
      try { items.push(JSON.parse(value)); } catch { items.push({ raw: value }); }
    } else if (value && typeof value === "object") items.push(value);
  }
  return items;
}

class LiveSession {
  constructor(connection) {
    this.connection = connection;
    this.ws = null;
    this.stopped = false;
    this.authorized = false;
    this.subscribed = false;
    this.reconnectCount = 0;
    this.eventCount = 0;
    this.pulseCount = 0;
    this.lastPulseAt = 0;
    this.pulseTimer = null;
    this.pulseInFlight = false;
    this.pulseQueued = false;
    this.heartbeatTimer = null;
    this.socketHeartbeatTimer = null;
    this.lastClientHeartbeatAt = 0;
    this.lastCloseInfo = "";
    this.backoffMs = 1000;
  }

  updateConnection(connection) { this.connection = connection; }

  async start() {
    while (!this.stopped && !stopping) {
      try {
        await this.connectOnce();
      } catch (error) {
        if (this.stopped || stopping) break;
        const message = error instanceof Error ? error.message : String(error);
        console.error("live connection failed", {
          connection_id: this.connection.id,
          display_name: this.connection.display_name || "Tradovate login",
          environment: this.connection.environment || "demo",
          error: message,
        });
        this.reconnectCount += 1;
        await writeStatus(this.connection, {
          state: "reconnecting",
          reconnect_count: this.reconnectCount,
          last_error: message,
          last_heartbeat_at: nowIso(),
        });
        await sleep(this.backoffMs + Math.floor(Math.random() * 500));
        this.backoffMs = Math.min(this.backoffMs * 2, 30000);
      }
    }
  }

  async loadCredential() {
    return requestWorkerSession(this.connection.id);
  }

  async connectOnce() {
    this.authorized = false;
    this.subscribed = false;
    this.lastCloseInfo = "";
    const { token, userIds, environment, wsUrl: brokeredWsUrl } = await this.loadCredential();
    const wsUrl = brokeredWsUrl || `wss://${environment}.tradovateapi.com/v1/websocket`;

    await writeStatus(this.connection, {
      state: "connecting",
      last_error: null,
      reconnect_count: this.reconnectCount,
      last_heartbeat_at: nowIso(),
    });

    await new Promise((resolve, reject) => {
      const ws = new WebSocket(wsUrl, { handshakeTimeout: 20000 });
      this.ws = ws;
      let settled = false;

      const finishReject = error => {
        if (!settled) {
          settled = true;
          try { ws.close(1011, "connection-failed"); } catch {}
          reject(error);
        }
      };

      const sendSocketHeartbeat = () => {
        if (ws.readyState !== WebSocket.OPEN) return;
        try {
          ws.send("[]");
          this.lastClientHeartbeatAt = Date.now();
        } catch (error) {
          console.error("Tradovate heartbeat send failed", this.connection.id, error instanceof Error ? error.message : String(error));
        }
      };

      ws.on("open", () => {
        sendRequest(ws, "authorize", 0, token);
        this.socketHeartbeatTimer = setInterval(sendSocketHeartbeat, TRADOVATE_HEARTBEAT_MS);
      });
      ws.on("message", async raw => {
        const rawText = asText(raw);
        if (rawText === "h") {
          if (Date.now() - this.lastClientHeartbeatAt >= 1000) sendSocketHeartbeat();
          return;
        }
        const frames = unpackFrame(rawText);
        if (!frames.length) return;
        for (const frame of frames) {
          if (Number(frame.i) === 0) {
            if (Number(frame.s) !== 200) {
              return finishReject(new Error(`websocket authorize: ${frameError(frame, "Tradovate WebSocket authorization failed")}`));
            }
            this.authorized = true;
            sendRequest(ws, "user/syncrequest", 1, { users: userIds, splitResponses: true });
            continue;
          }
          if (Number(frame.i) === 1 && Number(frame.s) >= 400) {
            return finishReject(new Error(`user/syncrequest: ${frameError(frame, "Tradovate user synchronization failed")}`));
          }
          if (Number(frame.i) === 1 && Number(frame.s) === 200 && !this.subscribed) {
            this.subscribed = true;
            this.backoffMs = 1000;
            await writeStatus(this.connection, {
              state: "live",
              last_connected_at: nowIso(),
              last_heartbeat_at: nowIso(),
              reconnect_count: this.reconnectCount,
              event_count: this.eventCount,
              pulse_count: this.pulseCount,
              last_error: null,
            });
            this.schedulePulse("initial-live-snapshot", 350);
            if (!settled) { settled = true; resolve(); }
            continue;
          }

          if (this.authorized) {
            this.eventCount += 1;
            const eventAt = nowIso();
            await writeStatus(this.connection, {
              state: "live",
              last_event_at: eventAt,
              last_heartbeat_at: eventAt,
              event_count: this.eventCount,
              reconnect_count: this.reconnectCount,
              last_error: null,
            });
            this.schedulePulse("tradovate-user-event");
          }
        }
      });
      ws.on("error", finishReject);
      ws.on("close", (code, reason) => {
        this.lastCloseInfo = `Tradovate WebSocket closed (${code}) ${asText(reason)}`.trim();
        this.clearHeartbeat();
        this.authorized = false;
        this.subscribed = false;
        this.ws = null;
        if (!this.stopped && !stopping) finishReject(new Error(`Tradovate WebSocket closed (${code}) ${asText(reason)}`.trim()));
        else if (!settled) { settled = true; resolve(); }
      });

      this.heartbeatTimer = setInterval(() => {
        writeStatus(this.connection, {
          state: this.subscribed ? "live" : "connecting",
          last_heartbeat_at: nowIso(),
          event_count: this.eventCount,
          pulse_count: this.pulseCount,
          reconnect_count: this.reconnectCount,
        }).catch(error => console.error("heartbeat failed", error));
      }, HEARTBEAT_MS);
    });

    while (!this.stopped && !stopping && this.ws?.readyState === WebSocket.OPEN) await sleep(1000);
    if (!this.stopped && !stopping) throw new Error(this.lastCloseInfo || "Tradovate WebSocket disconnected");
  }

  clearHeartbeat() {
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
    if (this.socketHeartbeatTimer) clearInterval(this.socketHeartbeatTimer);
    this.heartbeatTimer = null;
    this.socketHeartbeatTimer = null;
  }

  schedulePulse(reason, delay = PULSE_MIN_INTERVAL_MS) {
    if (this.stopped || stopping) return;
    if (this.pulseTimer) clearTimeout(this.pulseTimer);
    const sinceLast = Date.now() - this.lastPulseAt;
    const wait = Math.max(delay, PULSE_MIN_INTERVAL_MS - sinceLast, 0);
    this.pulseTimer = setTimeout(() => this.runPulse(reason), wait);
  }

  async runPulse(reason) {
    this.pulseTimer = null;
    if (this.pulseInFlight) {
      this.pulseQueued = true;
      return;
    }
    this.pulseInFlight = true;
    this.lastPulseAt = Date.now();
    try {
      const response = await fetch(`${SUPABASE_URL}/functions/v1/tradovate-live-pulse`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Authorization": `Bearer ${SERVICE_ROLE_KEY}`,
          "apikey": SERVICE_ROLE_KEY,
          "x-fra-worker-secret": WORKER_SECRET,
        },
        body: JSON.stringify({ connection_id: this.connection.id, reason }),
      });
      const payload = await response.json().catch(() => ({}));
      if (!response.ok || payload?.error) throw new Error(payload?.error || `Live pulse failed (${response.status})`);
      this.pulseCount += 1;
      await writeStatus(this.connection, {
        state: payload.requires_full_sync ? "attention" : "live",
        last_pulse_at: nowIso(),
        last_success_at: nowIso(),
        last_heartbeat_at: nowIso(),
        pulse_count: this.pulseCount,
        event_count: this.eventCount,
        last_error: payload.requires_full_sync ? "A newly detected account needs one full sync" : null,
        metadata: { last_pulse_result: payload, last_reason: reason },
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      console.error("live pulse failed", this.connection.id, message);
      await writeStatus(this.connection, {
        state: "attention",
        last_heartbeat_at: nowIso(),
        last_error: message,
        pulse_count: this.pulseCount,
        event_count: this.eventCount,
      });
    } finally {
      this.pulseInFlight = false;
      if (this.pulseQueued) {
        this.pulseQueued = false;
        this.schedulePulse("queued-provider-event", 350);
      }
    }
  }

  async stop(reason = "stopped") {
    this.stopped = true;
    if (this.pulseTimer) clearTimeout(this.pulseTimer);
    this.clearHeartbeat();
    try { this.ws?.close(1000, reason); } catch {}
    this.ws = null;
    await writeStatus(this.connection, {
      state: reason === "not-eligible" ? "manual" : "offline",
      last_heartbeat_at: nowIso(),
      last_error: reason === "not-eligible" ? "Live sync is available on paid plans" : null,
    });
  }
}

async function reconcileTargets() {
  const targets = await loadTargets();
  const targetById = new Map(targets.map(row => [row.id, row]));

  for (const [id, session] of sessions) {
    const target = targetById.get(id);
    if (!target) {
      sessions.delete(id);
      await session.stop("not-eligible");
    } else {
      session.updateConnection(target);
    }
  }

  for (const target of targets) {
    if (sessions.has(target.id)) continue;
    const session = new LiveSession(target);
    sessions.set(target.id, session);
    session.start().catch(error => console.error("session stopped unexpectedly", target.id, error));
  }

  const liveCount = [...sessions.values()].filter(session => session.subscribed && session.ws?.readyState === WebSocket.OPEN).length;
  const connectingCount = Math.max(sessions.size - liveCount, 0);
  console.log(`[${nowIso()}] live targets=${targets.length} subscribed=${liveCount} connecting=${connectingCount}`);
}

async function shutdown(signal) {
  if (stopping) return;
  stopping = true;
  console.log(`received ${signal}; closing ${sessions.size} live sessions`);
  await Promise.allSettled([...sessions.values()].map(session => session.stop("worker-shutdown")));
  process.exit(0);
}

process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));
process.on("unhandledRejection", error => console.error("unhandled rejection", error));
process.on("uncaughtException", error => console.error("uncaught exception", error));

console.log(`FRA Prop HQ Tradovate live worker v${VERSION} starting as ${INSTANCE_ID}`);
await reconcileTargets();
while (!stopping) {
  await sleep(TARGET_REFRESH_MS);
  try { await reconcileTargets(); } catch (error) { console.error("target refresh failed", error); }
}
