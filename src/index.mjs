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
const SESSION_RECONNECT_MS = Number(process.env.SESSION_RECONNECT_MS || 55 * 60 * 1000);
const VERSION = "7.7.19";
const COPIER_EVENT_MAX_AGE_MS = Number(process.env.COPIER_EVENT_MAX_AGE_MS || 15 * 60 * 1000);
const COPIER_POSITION_SCAN_MS = Number(process.env.COPIER_POSITION_SCAN_MS || 1000);
const COPIER_POSITION_FALLBACK_GRACE_MS = Number(process.env.COPIER_POSITION_FALLBACK_GRACE_MS || 1500);
const COPIER_PRIMARY_MATCH_WINDOW_MS = Number(process.env.COPIER_PRIMARY_MATCH_WINDOW_MS || 2000);
const COPIER_PARTIAL_FILL_QUIET_MS = Math.max(75, Number(process.env.COPIER_PARTIAL_FILL_QUIET_MS || 175));
const COPIER_PARTIAL_FILL_MAX_MS = Math.max(COPIER_PARTIAL_FILL_QUIET_MS, Number(process.env.COPIER_PARTIAL_FILL_MAX_MS || 400));
const COPIER_REST_FALLBACK_DELAY_MS = Math.max(300, Number(process.env.COPIER_REST_FALLBACK_DELAY_MS || 750));
const COPIER_COMMAND_POLL_MS = Math.max(100, Number(process.env.COPIER_COMMAND_POLL_MS || 200));
const COPIER_FLATTEN_VERIFY_MS = Math.max(300, Number(process.env.COPIER_FLATTEN_VERIFY_MS || 750));
const COPIER_FLATTEN_VERIFY_ATTEMPTS = Math.max(2, Math.min(Number(process.env.COPIER_FLATTEN_VERIFY_ATTEMPTS || 5), 10));

const supabase = createClient(SUPABASE_URL, SERVICE_ROLE_KEY, {
  auth: { persistSession: false, autoRefreshToken: false },
  realtime: { transport: WebSocket },
});

const sessions = new Map();
let stopping = false;

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const nowIso = () => new Date().toISOString();
const asText = value => String(value ?? "");

const num = (value, fallback = 0) => {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
};

function normaliseProviderId(value) {
  return asText(value).trim();
}

function propsEvents(frame) {
  if (asText(frame?.e).toLowerCase() !== "props") return [];
  const details = Array.isArray(frame?.d) ? frame.d : [frame?.d];
  return details.filter(item => item && typeof item === "object");
}

async function tradovateGet(environment, path, token) {
  const response = await fetch(`https://${environment}.tradovateapi.com/v1${path}`, {
    headers: {
      "Accept": "application/json",
      "Authorization": `Bearer ${token}`,
    },
  });
  const payload = await response.json().catch(() => ({}));
  if (!response.ok || payload?.errorText) {
    throw new Error(payload?.errorText || payload?.error || `${path} failed (${response.status})`);
  }
  return payload;
}

async function tradovatePost(environment, path, token, body) {
  const response = await fetch(`https://${environment}.tradovateapi.com/v1${path}`, {
    method: "POST",
    headers: {
      "Accept": "application/json",
      "Content-Type": "application/json",
      "Authorization": `Bearer ${token}`,
    },
    body: JSON.stringify(body ?? {}),
  });
  const payload = await response.json().catch(() => ({}));
  const failureReason = asText(payload?.failureReason).trim();
  const failureText = asText(payload?.failureText).trim();
  const errorText = asText(payload?.errorText || payload?.error).trim();
  const explicitFailure = payload?.ok === false || failureText || errorText || (failureReason && failureReason.toLowerCase() !== "success");
  if (!response.ok || explicitFailure) {
    throw new Error(failureText || errorText || failureReason || `${path} failed (${response.status})`);
  }
  return payload;
}

function symbolAllowed(symbol, allowedSymbols) {
  const normalized = asText(symbol).trim().toUpperCase();
  const allowed = Array.isArray(allowedSymbols)
    ? allowedSymbols.map(value => asText(value).trim().toUpperCase()).filter(Boolean)
    : [];
  if (!allowed.length) return true;
  if (!normalized) return false;
  return allowed.some(value => normalized === value || normalized.startsWith(value));
}

function copierClientOrderId(groupId, followerAccountId, providerFillId) {
  const digest = crypto.createHash("sha256")
    .update(`${groupId}:${followerAccountId}:${providerFillId}`)
    .digest("hex")
    .slice(0, 28);
  return `FRA-${digest}`;
}

async function requestWorkerSession(connectionId) {
  const response = await fetch(`${SUPABASE_URL}/functions/v1/tradovate-session`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Authorization": `Bearer ${SERVICE_ROLE_KEY}`,
      "apikey": SERVICE_ROLE_KEY,
      "x-fra-worker-secret": WORKER_SECRET,
    },
    body: JSON.stringify({ connection_id: connectionId }),
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

  const environment = payload.environment === "live" ? "live" : "demo";
  let accountIds = [];
  try {
    const brokerAccounts = await tradovateGet(environment, "/account/list", token);
    accountIds = [...new Set((Array.isArray(brokerAccounts) ? brokerAccounts : [])
      .map(row => Number(row?.id))
      .filter(Number.isFinite))];
  } catch (error) {
    console.error("unable to preload Tradovate account ids", connectionId, error instanceof Error ? error.message : String(error));
  }
  if (!accountIds.length) {
    const { data: links, error: linksError } = await supabase
      .from("provider_account_links")
      .select("provider_account_id")
      .eq("connection_id", connectionId)
      .not("provider_account_id", "is", null);
    if (linksError) throw linksError;
    accountIds = [...new Set((links || [])
      .map(row => Number(row.provider_account_id))
      .filter(Number.isFinite))];
  }

  let accountSpec = "";
  try {
    const user = await tradovateGet(environment, `/user/item?id=${encodeURIComponent(userIds[0])}`, token);
    accountSpec = asText(user?.name).trim();
  } catch (error) {
    console.error("unable to resolve Tradovate accountSpec", connectionId, error instanceof Error ? error.message : String(error));
  }

  return {
    token,
    userIds,
    accountIds,
    accountSpec,
    environment,
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
    this.sessionRefreshTimer = null;
    this.copierFillScanTimer = null;
    this.lastClientHeartbeatAt = 0;
    this.lastCloseInfo = "";
    this.backoffMs = 1000;
    this.accessToken = "";
    this.accountSpec = "";
    this.environment = "demo";
    this.contractNameCache = new Map();
    this.contractNamePromiseCache = new Map();
    this.followerOrderInFlight = new Set();
    this.copierEventInFlight = new Set();
    this.copierFillScanTimer = null;
    this.copierFillScanInFlight = false;
    this.copierPositionScanInterval = null;
    this.copierPositionScanInFlight = false;
    this.copierPositionSnapshot = new Map();
    this.copierPositionInitialized = false;
    this.recentPrimaryLeaderFills = new Map();
    this.pendingPositionFallbacks = new Map();
    this.pendingLeaderFillBatches = new Map();
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

  async resolveContractName(contractId) {
    const key = normaliseProviderId(contractId);
    if (!key) return "";
    if (this.contractNameCache.has(key)) return this.contractNameCache.get(key);
    if (this.contractNamePromiseCache.has(key)) return this.contractNamePromiseCache.get(key);

    const lookup = (async () => {
      try {
        const contract = await tradovateGet(this.environment, `/contract/item?id=${encodeURIComponent(key)}`, this.accessToken);
        const name = asText(contract?.name).trim();
        if (name) this.contractNameCache.set(key, name);
        return name;
      } catch (error) {
        console.error("contract lookup failed", this.connection.id, key, error instanceof Error ? error.message : String(error));
        return "";
      } finally {
        this.contractNamePromiseCache.delete(key);
      }
    })();

    this.contractNamePromiseCache.set(key, lookup);
    return lookup;
  }

  primeContractName(contractId) {
    const key = normaliseProviderId(contractId);
    if (!key || this.contractNameCache.has(key) || this.contractNamePromiseCache.has(key)) return;
    this.resolveContractName(key).catch(() => {});
  }

  primaryFillBucketKey(report) {
    const accountId = normaliseProviderId(report?.accountId);
    const contractId = normaliseProviderId(report?.contractId);
    const action = asText(report?.action).trim().toLowerCase();
    if (!accountId || !contractId || !action) return "";
    return `${accountId}:${contractId}:${action}`;
  }

  rememberPrimaryLeaderFill(report) {
    const key = this.primaryFillBucketKey(report);
    if (!key) return;
    const quantity = Math.abs(num(report?.lastQty));
    if (quantity < 1) return;
    const providerTimestampMs = new Date(asText(report?.timestamp)).getTime();
    const observedAt = Date.now();
    const entries = this.recentPrimaryLeaderFills.get(key) || [];
    entries.push({
      quantity,
      providerTimestampMs: Number.isFinite(providerTimestampMs) ? providerTimestampMs : observedAt,
      observedAt,
      providerFillId: normaliseProviderId(report?.execRefId || report?.id),
    });
    const cutoff = observedAt - Math.max(COPIER_PRIMARY_MATCH_WINDOW_MS * 4, 10000);
    this.recentPrimaryLeaderFills.set(key, entries.filter(entry => entry.observedAt >= cutoff));
  }

  hasPrimaryCoverageForPositionDelta(report) {
    const key = this.primaryFillBucketKey(report);
    if (!key) return false;
    const requiredQuantity = Math.abs(num(report?.lastQty));
    if (requiredQuantity < 1) return false;
    const targetTimestampMs = new Date(asText(report?.rawPosition?.timestamp || report?.timestamp)).getTime();
    const now = Date.now();
    const entries = (this.recentPrimaryLeaderFills.get(key) || []).filter(entry => {
      if (now - entry.observedAt > Math.max(COPIER_PRIMARY_MATCH_WINDOW_MS * 3, 6000)) return false;
      if (!Number.isFinite(targetTimestampMs)) return true;
      return Math.abs(entry.providerTimestampMs - targetTimestampMs) <= COPIER_PRIMARY_MATCH_WINDOW_MS;
    });
    const coveredQuantity = entries.reduce((sum, entry) => sum + Math.abs(num(entry.quantity)), 0);
    return coveredQuantity >= requiredQuantity;
  }

  queuePositionDeltaFallback(report, eventType = "position-delta") {
    const accountId = normaliseProviderId(report?.accountId);
    const contractId = normaliseProviderId(report?.contractId);
    const action = asText(report?.action).trim();
    const quantity = Math.abs(num(report?.lastQty));
    const before = num(report?.previousNetPos);
    const after = num(report?.currentNetPos);
    if (!accountId || !contractId || !action || quantity < 1) return;

    const key = `${accountId}:${contractId}:${before}:${after}:${action}:${quantity}`;
    if (this.pendingPositionFallbacks.has(key)) return;

    const timer = setTimeout(async () => {
      this.pendingPositionFallbacks.delete(key);
      if (this.stopped || stopping) return;
      if (this.hasPrimaryCoverageForPositionDelta(report)) {
        console.log("copier position fallback suppressed by primary Tradovate fill", {
          connection_id: this.connection.id,
          provider_account_id: accountId,
          contract_id: contractId,
          action,
          quantity,
          previous_net_position: before,
          current_net_position: after,
        });
        return;
      }
      await this.detectLeaderExecution(report, eventType);
    }, Math.max(250, COPIER_POSITION_FALLBACK_GRACE_MS));
    timer.unref?.();
    this.pendingPositionFallbacks.set(key, timer);
  }

  async getFollowerPosition(providerAccountId, contractId) {
    try {
      const positions = await tradovateGet(this.environment, "/position/list", this.accessToken);
      const row = (Array.isArray(positions) ? positions : []).find(item =>
        normaliseProviderId(item?.accountId) === normaliseProviderId(providerAccountId) &&
        normaliseProviderId(item?.contractId) === normaliseProviderId(contractId)
      );
      return num(row?.netPos, num(row?.netPosition, 0));
    } catch (error) {
      console.error("follower position check failed", this.connection.id, providerAccountId, contractId, error instanceof Error ? error.message : String(error));
      return null;
    }
  }

  queueCopierAudit(row, label = "copier audit") {
    try {
      supabase.from("copier_events").insert(row).then(({ error }) => {
        if (error && error.code !== "23505") {
          console.error(label, this.connection.id, error.message);
        }
      }).catch(error => {
        console.error(label, this.connection.id, error instanceof Error ? error.message : String(error));
      });
    } catch (error) {
      console.error(label, this.connection.id, error instanceof Error ? error.message : String(error));
    }
  }

  async tripCopierSafety(group, providerFillId, reason, details = {}) {
    try {
      const { error } = await supabase.from("copier_groups").update({
        armed: false,
        desired_armed: false,
        updated_at: nowIso(),
      }).eq("id", group.id).eq("owner_id", this.connection.owner_id);
      if (error) throw error;
      this.queueCopierAudit({
        owner_id: this.connection.owner_id,
        group_id: group.id,
        leader_account_id: group.leader_account_id,
        event_type: "copier_safety_trip",
        status: "error",
        dedupe_key: `safety-trip:${group.id}:${providerFillId || "no-fill"}:${reason}`,
        provider_fill_id: providerFillId || null,
        message: `COPIER SAFETY TRIP · ${reason} · copier disarmed`,
        payload: {
          reason,
          worker_version: VERSION,
          worker_instance: INSTANCE_ID,
          ...details,
        },
      }, "copier safety-trip audit failed");
    } catch (error) {
      console.error("copier safety trip failed", group?.id, error instanceof Error ? error.message : String(error));
    }
  }

  latencyPayload(telemetry = {}, dispatchStartedAtMs = Date.now(), responseAtMs = null) {
    const providerTimestampMs = num(telemetry.providerTimestampMs, 0);
    const workerReceivedAtMs = num(telemetry.workerReceivedAtMs, 0);
    const rpcStartedAtMs = num(telemetry.rpcStartedAtMs, 0);
    const rpcFinishedAtMs = num(telemetry.rpcFinishedAtMs, 0);
    const out = {
      fast_path: true,
      worker_version: VERSION,
      provider_timestamp: providerTimestampMs ? new Date(providerTimestampMs).toISOString() : null,
      worker_received_at: workerReceivedAtMs ? new Date(workerReceivedAtMs).toISOString() : null,
      dispatch_started_at: new Date(dispatchStartedAtMs).toISOString(),
      provider_to_worker_ms: providerTimestampMs && workerReceivedAtMs ? Math.max(0, workerReceivedAtMs - providerTimestampMs) : null,
      worker_to_dispatch_ms: workerReceivedAtMs ? Math.max(0, dispatchStartedAtMs - workerReceivedAtMs) : null,
      provider_to_dispatch_ms: providerTimestampMs ? Math.max(0, dispatchStartedAtMs - providerTimestampMs) : null,
      dispatch_prepare_rpc_ms: rpcStartedAtMs && rpcFinishedAtMs ? Math.max(0, rpcFinishedAtMs - rpcStartedAtMs) : null,
    };
    if (responseAtMs) {
      out.provider_response_at = new Date(responseAtMs).toISOString();
      out.order_api_ms = Math.max(0, responseAtMs - dispatchStartedAtMs);
      out.provider_to_response_ms = providerTimestampMs ? Math.max(0, responseAtMs - providerTimestampMs) : null;
    }
    return out;
  }

  async executeFollowerOrder({ group, follower, followerAccount, leaderEvent, symbol, action, quantity, providerFillId, contractId, telemetry }) {
    const targetConnectionId = asText(followerAccount?.source_connection_id).trim();
    const providerAccountId = normaliseProviderId(followerAccount?.external_id);
    if (!targetConnectionId || !providerAccountId) return;

    const targetSession = sessions.get(targetConnectionId);
    if (!targetSession || !targetSession.accessToken || !targetSession.subscribed || targetSession.ws?.readyState !== WebSocket.OPEN) {
      this.queueCopierAudit({
        owner_id: this.connection.owner_id,
        group_id: group.id,
        leader_account_id: group.leader_account_id,
        follower_account_id: follower.account_id,
        event_type: "follower_order_blocked",
        status: "blocked",
        dedupe_key: `route:${group.id}:${providerFillId}:${follower.account_id}:session-unavailable`,
        provider_fill_id: providerFillId,
        symbol,
        action,
        quantity,
        message: `Follower blocked · ${followerAccount?.name || providerAccountId} session unavailable`,
        payload: { reason: "follower_session_unavailable", target_connection_id: targetConnectionId, ...this.latencyPayload(telemetry) },
      }, "copier session-block audit failed");
      await this.tripCopierSafety(group, providerFillId, "follower_session_unavailable", { target_connection_id: targetConnectionId, follower_account_id: follower.account_id });
      return;
    }

    const accountSpec = asText(followerAccount?.name || targetSession.accountSpec).trim();
    if (!accountSpec) {
      this.queueCopierAudit({
        owner_id: this.connection.owner_id,
        group_id: group.id,
        leader_account_id: group.leader_account_id,
        follower_account_id: follower.account_id,
        event_type: "follower_order_blocked",
        status: "blocked",
        dedupe_key: `route:${group.id}:${providerFillId}:${follower.account_id}:accountspec-missing`,
        provider_fill_id: providerFillId,
        symbol,
        action,
        quantity,
        message: `Follower blocked · Tradovate accountSpec unavailable for ${followerAccount?.name || providerAccountId}`,
        payload: { reason: "tradovate_accountspec_missing", target_connection_id: targetConnectionId, ...this.latencyPayload(telemetry) },
      }, "copier accountSpec audit failed");
      await this.tripCopierSafety(group, providerFillId, "tradovate_accountspec_missing", { target_connection_id: targetConnectionId, follower_account_id: follower.account_id });
      return;
    }

    const multiplier = Math.max(0, num(follower.multiplier, 1));
    const computedQty = Math.floor(Math.abs(quantity) * multiplier + 1e-9);
    const maxQty = Math.max(1, Math.floor(num(follower.max_qty, 1)));
    if (computedQty < 1 || computedQty > maxQty) {
      this.queueCopierAudit({
        owner_id: this.connection.owner_id,
        group_id: group.id,
        leader_account_id: group.leader_account_id,
        follower_account_id: follower.account_id,
        event_type: "follower_order_blocked",
        status: "blocked",
        dedupe_key: `route:${group.id}:${providerFillId}:${follower.account_id}:qty-blocked`,
        provider_fill_id: providerFillId,
        symbol,
        action,
        quantity: computedQty || quantity,
        message: `Follower blocked · computed quantity ${computedQty} exceeds limits`,
        payload: { reason: "quantity_limit", multiplier, max_qty: maxQty, leader_quantity: quantity, ...this.latencyPayload(telemetry) },
      }, "copier quantity-block audit failed");
      return;
    }

    if (!symbolAllowed(symbol, follower.allowed_symbols)) {
      this.queueCopierAudit({
        owner_id: this.connection.owner_id,
        group_id: group.id,
        leader_account_id: group.leader_account_id,
        follower_account_id: follower.account_id,
        event_type: "follower_order_blocked",
        status: "blocked",
        dedupe_key: `route:${group.id}:${providerFillId}:${follower.account_id}:symbol-blocked`,
        provider_fill_id: providerFillId,
        symbol,
        action,
        quantity: computedQty,
        message: `Follower blocked · ${symbol} not allowed for ${followerAccount?.name || providerAccountId}`,
        payload: { reason: "symbol_not_allowed", allowed_symbols: follower.allowed_symbols || [], ...this.latencyPayload(telemetry) },
      }, "copier symbol-block audit failed");
      return;
    }

    const dailyLossLimit = Math.abs(num(follower.max_daily_loss, 0));
    const dailyPnl = num(followerAccount?.daily_pnl, 0);
    if (dailyLossLimit > 0 && dailyPnl <= -dailyLossLimit) {
      this.queueCopierAudit({
        owner_id: this.connection.owner_id,
        group_id: group.id,
        leader_account_id: group.leader_account_id,
        follower_account_id: follower.account_id,
        event_type: "follower_order_blocked",
        status: "blocked",
        dedupe_key: `route:${group.id}:${providerFillId}:${follower.account_id}:daily-loss`,
        provider_fill_id: providerFillId,
        symbol,
        action,
        quantity: computedQty,
        message: `Follower blocked · daily loss limit reached on ${followerAccount?.name || providerAccountId}`,
        payload: { reason: "daily_loss_limit", max_daily_loss: dailyLossLimit, daily_pnl: dailyPnl, ...this.latencyPayload(telemetry) },
      }, "copier daily-loss audit failed");
      return;
    }

    const routeKey = `${group.id}:${follower.account_id}:${providerFillId}`;
    if (this.followerOrderInFlight.has(routeKey)) return;
    this.followerOrderInFlight.add(routeKey);

    const clientOrderId = copierClientOrderId(group.id, follower.account_id, providerFillId);
    const dispatchStartedAtMs = Date.now();
    const latency = this.latencyPayload(telemetry, dispatchStartedAtMs);

    this.queueCopierAudit({
      owner_id: this.connection.owner_id,
      group_id: group.id,
      leader_account_id: group.leader_account_id,
      follower_account_id: follower.account_id,
      event_type: "follower_order_reserved",
      status: "pending",
      dedupe_key: `route:${group.id}:${providerFillId}:${follower.account_id}:reserved`,
      provider_fill_id: providerFillId,
      symbol,
      action,
      quantity: computedQty,
      message: `Reserved → ${followerAccount?.name || providerAccountId}: ${action} ${computedQty} ${symbol}`,
      payload: {
        cl_ord_id: clientOrderId,
        target_connection_id: targetConnectionId,
        target_provider_account_id: providerAccountId,
        ...latency,
      },
    }, "copier reserve audit failed");

    try {
      const response = await tradovatePost(targetSession.environment, "/order/placeorder", targetSession.accessToken, {
        accountSpec,
        accountId: Number(providerAccountId),
        clOrdId: clientOrderId,
        action,
        symbol,
        orderQty: computedQty,
        orderType: "Market",
        isAutomated: true,
      });
      const responseAtMs = Date.now();
      const providerOrderId = normaliseProviderId(response?.orderId || response?.commandId);
      this.queueCopierAudit({
        owner_id: this.connection.owner_id,
        group_id: group.id,
        leader_account_id: group.leader_account_id,
        follower_account_id: follower.account_id,
        event_type: "follower_order_submitted",
        status: "success",
        dedupe_key: `route:${group.id}:${providerFillId}:${follower.account_id}:submitted`,
        provider_order_id: providerOrderId || null,
        provider_fill_id: providerFillId,
        symbol,
        action,
        quantity: computedQty,
        message: `LIVE COPY → ${followerAccount?.name || providerAccountId}: ${action} ${computedQty} ${symbol}`,
        payload: {
          cl_ord_id: clientOrderId,
          target_connection_id: targetConnectionId,
          target_provider_account_id: providerAccountId,
          provider_response: response,
          leader_event_id: leaderEvent?.id || null,
          ...this.latencyPayload(telemetry, dispatchStartedAtMs, responseAtMs),
        },
      }, "copier submitted audit failed");
    } catch (error) {
      const responseAtMs = Date.now();
      this.queueCopierAudit({
        owner_id: this.connection.owner_id,
        group_id: group.id,
        leader_account_id: group.leader_account_id,
        follower_account_id: follower.account_id,
        event_type: "follower_order_rejected",
        status: "error",
        dedupe_key: `route:${group.id}:${providerFillId}:${follower.account_id}:rejected`,
        provider_fill_id: providerFillId,
        symbol,
        action,
        quantity: computedQty,
        message: `Follower order rejected · ${error instanceof Error ? error.message : String(error)}`,
        payload: {
          cl_ord_id: clientOrderId,
          target_connection_id: targetConnectionId,
          target_provider_account_id: providerAccountId,
          ...this.latencyPayload(telemetry, dispatchStartedAtMs, responseAtMs),
        },
      }, "copier rejected audit failed");
      await this.tripCopierSafety(group, providerFillId, "follower_order_rejected", {
        follower_account_id: follower.account_id,
        follower_account_name: followerAccount?.name || providerAccountId,
        target_connection_id: targetConnectionId,
        error: error instanceof Error ? error.message : String(error),
      });
    } finally {
      setTimeout(() => this.followerOrderInFlight.delete(routeKey), 30000).unref?.();
    }
  }

  aggregateFillId(providerOrderId, fillIds, action, symbol) {
    const digest = crypto.createHash("sha256")
      .update(`${this.connection.id}:${providerOrderId || "no-order"}:${asText(action).toLowerCase()}:${asText(symbol).toUpperCase()}:${[...fillIds].sort().join(",")}`)
      .digest("hex")
      .slice(0, 28);
    return `agg-${digest}`;
  }

  async prepareAndRouteLeaderFill({ providerAccountId, providerFillId, providerOrderId, symbol, action, quantity, contractId, leaderEvent, eventType, telemetry }) {
    if (this.stopped || stopping || !this.subscribed || this.ws?.readyState !== WebSocket.OPEN) return;

    const rpcStartedAtMs = Date.now();
    const providerTimestampMs = num(telemetry?.providerTimestampMs, 0);
    const providerTimestamp = providerTimestampMs ? new Date(providerTimestampMs).toISOString() : (leaderEvent?.timestamp || nowIso());
    const payload = {
      source_entity_type: leaderEvent?.source_entity_type || null,
      component_fill_ids: leaderEvent?.component_fill_ids || [providerFillId],
      contract_id: contractId || null,
      event_type: eventType || null,
      worker_received_at: telemetry?.workerReceivedAtMs ? new Date(telemetry.workerReceivedAtMs).toISOString() : null,
      last_price: telemetry?.lastPrice ?? null,
      avg_price: telemetry?.avgPrice ?? null,
      worker_version: VERSION,
    };

    const { data, error } = await supabase.rpc("prepare_live_copier_dispatch_v1", {
      p_connection_id: this.connection.id,
      p_owner_id: this.connection.owner_id,
      p_provider_account_id: providerAccountId,
      p_provider_fill_id: providerFillId,
      p_provider_order_id: providerOrderId || null,
      p_symbol: symbol,
      p_action: action,
      p_quantity: quantity,
      p_provider_timestamp: providerTimestamp,
      p_event_type: eventType || null,
      p_payload: payload,
    });
    const rpcFinishedAtMs = Date.now();

    if (error) throw error;
    if (!data?.ok) {
      if (data?.reason && data.reason !== "leader_account_not_mapped") {
        console.warn("copier fast dispatch not prepared", this.connection.id, data.reason);
      }
      return;
    }

    const plans = Array.isArray(data.groups) ? data.groups : [];
    if (!plans.length) return;

    const fastTelemetry = {
      ...telemetry,
      providerTimestampMs,
      rpcStartedAtMs,
      rpcFinishedAtMs,
    };

    console.log("copier fast dispatch ready", {
      connection_id: this.connection.id,
      provider_fill_id: providerFillId,
      provider_order_id: providerOrderId || null,
      symbol,
      action,
      quantity,
      groups: plans.length,
      provider_to_worker_ms: providerTimestampMs && telemetry?.workerReceivedAtMs ? Math.max(0, telemetry.workerReceivedAtMs - providerTimestampMs) : null,
      prepare_rpc_ms: rpcFinishedAtMs - rpcStartedAtMs,
      provider_to_dispatch_ready_ms: providerTimestampMs ? Math.max(0, rpcFinishedAtMs - providerTimestampMs) : null,
    });

    const routes = [];
    for (const plan of plans) {
      const group = plan?.group || {};
      const followers = Array.isArray(plan?.followers) ? plan.followers : [];
      for (const route of followers) {
        if (!route?.follower || !route?.account) continue;
        routes.push({ group, follower: route.follower, followerAccount: route.account });
      }
    }

    const startedAt = Date.now();
    const results = await Promise.allSettled(routes.map(({ group, follower, followerAccount }) =>
      this.executeFollowerOrder({
        group,
        follower,
        followerAccount,
        leaderEvent,
        symbol,
        action,
        quantity,
        providerFillId,
        contractId,
        telemetry: fastTelemetry,
      })
    ));

    const rejected = results.filter(result => result.status === "rejected");
    console.log("copier parallel fast-path batch complete", {
      connection_id: this.connection.id,
      provider_fill_id: providerFillId,
      followers: routes.length,
      rejected: rejected.length,
      elapsed_ms: Date.now() - startedAt,
    });
  }

  async flushLeaderFillBatch(batchKey) {
    const batch = this.pendingLeaderFillBatches.get(batchKey);
    if (!batch) return;
    if (batch.timer) clearTimeout(batch.timer);
    this.pendingLeaderFillBatches.delete(batchKey);

    if (this.stopped || stopping || !batch.fillIds.size || batch.quantity < 1) return;

    const aggregateProviderFillId = this.aggregateFillId(batch.providerOrderId, batch.fillIds, batch.action, batch.symbol);
    const aggregateQuantity = Math.abs(num(batch.quantity));

    console.log("copier leader fill batch ready", {
      connection_id: this.connection.id,
      provider_order_id: batch.providerOrderId || null,
      fill_ids: [...batch.fillIds],
      action: batch.action,
      quantity: aggregateQuantity,
      symbol: batch.symbol,
      final_fill_seen: !!batch.finalFillSeen,
      batch_age_ms: Date.now() - batch.firstSeenAt,
    });

    await this.prepareAndRouteLeaderFill({
      providerAccountId: batch.providerAccountId,
      providerFillId: aggregateProviderFillId,
      providerOrderId: batch.providerOrderId,
      symbol: batch.symbol,
      action: batch.action,
      quantity: aggregateQuantity,
      contractId: batch.contractId,
      eventType: batch.eventType,
      leaderEvent: {
        id: batch.leaderEvent?.id || aggregateProviderFillId,
        timestamp: batch.providerTimestampMs ? new Date(batch.providerTimestampMs).toISOString() : (batch.leaderEvent?.timestamp || nowIso()),
        source_entity_type: batch.leaderEvent?.source_entity_type || "aggregated-primary-fill",
        component_fill_ids: [...batch.fillIds],
        provider_order_id: batch.providerOrderId || null,
      },
      telemetry: {
        providerTimestampMs: batch.providerTimestampMs,
        workerReceivedAtMs: batch.workerReceivedAtMs,
        lastPrice: batch.lastPrice,
        avgPrice: batch.avgPrice,
      },
    });
  }

  enqueueLeaderFillBatch({ providerAccountId, leaderEvent, providerFillId, providerOrderId, symbol, action, quantity, contractId, eventType, finalFill, telemetry }) {
    if (!providerOrderId) {
      this.prepareAndRouteLeaderFill({
        providerAccountId,
        providerFillId,
        providerOrderId: null,
        symbol,
        action,
        quantity,
        contractId,
        leaderEvent,
        eventType,
        telemetry,
      }).catch(error => console.error("copier immediate follower route failed", this.connection.id, error instanceof Error ? error.message : String(error)));
      return;
    }

    const batchKey = `${providerOrderId}:${asText(action).trim().toLowerCase()}:${asText(symbol).trim().toUpperCase()}`;
    const now = Date.now();
    let batch = this.pendingLeaderFillBatches.get(batchKey);
    if (!batch) {
      batch = {
        providerAccountId,
        leaderEvent,
        providerOrderId,
        symbol,
        action,
        contractId,
        eventType,
        fillIds: new Set(),
        quantity: 0,
        firstSeenAt: now,
        lastSeenAt: now,
        providerTimestampMs: num(telemetry?.providerTimestampMs, 0),
        workerReceivedAtMs: num(telemetry?.workerReceivedAtMs, now),
        lastPrice: telemetry?.lastPrice ?? null,
        avgPrice: telemetry?.avgPrice ?? null,
        finalFillSeen: false,
        timer: null,
      };
      this.pendingLeaderFillBatches.set(batchKey, batch);
    }

    if (batch.fillIds.has(providerFillId)) {
      if (finalFill && !batch.finalFillSeen) {
        batch.finalFillSeen = true;
        if (batch.timer) clearTimeout(batch.timer);
        batch.timer = setTimeout(() => {
          this.flushLeaderFillBatch(batchKey).catch(error =>
            console.error("copier final fill flush failed", this.connection.id, error instanceof Error ? error.message : String(error))
          );
        }, 0);
        batch.timer.unref?.();
      }
      return;
    }

    batch.fillIds.add(providerFillId);
    batch.quantity += Math.abs(num(quantity));
    batch.lastSeenAt = now;
    batch.leaderEvent = leaderEvent || batch.leaderEvent;
    batch.eventType = eventType || batch.eventType;
    batch.lastPrice = telemetry?.lastPrice ?? batch.lastPrice;
    batch.avgPrice = telemetry?.avgPrice ?? batch.avgPrice;
    const providerTs = num(telemetry?.providerTimestampMs, 0);
    if (providerTs && (!batch.providerTimestampMs || providerTs < batch.providerTimestampMs)) batch.providerTimestampMs = providerTs;
    const receivedTs = num(telemetry?.workerReceivedAtMs, 0);
    if (receivedTs && (!batch.workerReceivedAtMs || receivedTs < batch.workerReceivedAtMs)) batch.workerReceivedAtMs = receivedTs;

    if (batch.timer) clearTimeout(batch.timer);

    if (finalFill) {
      batch.finalFillSeen = true;
      batch.timer = setTimeout(() => {
        this.flushLeaderFillBatch(batchKey).catch(error =>
          console.error("copier final fill flush failed", this.connection.id, error instanceof Error ? error.message : String(error))
        );
      }, 0);
      batch.timer.unref?.();
      return;
    }

    const age = now - batch.firstSeenAt;
    const wait = age >= COPIER_PARTIAL_FILL_MAX_MS
      ? 0
      : Math.min(COPIER_PARTIAL_FILL_QUIET_MS, Math.max(0, COPIER_PARTIAL_FILL_MAX_MS - age));
    batch.timer = setTimeout(() => {
      this.flushLeaderFillBatch(batchKey).catch(error =>
        console.error("copier partial fill batch flush failed", this.connection.id, error instanceof Error ? error.message : String(error))
      );
    }, wait);
    batch.timer.unref?.();
  }

  async detectLeaderExecution(report, eventType) {
    const workerReceivedAtMs = num(report?.workerReceivedAtMs, Date.now());
    const reportId = normaliseProviderId(report?.id);
    const providerAccountId = normaliseProviderId(report?.accountId);
    const contractId = normaliseProviderId(report?.contractId);
    const execType = asText(report?.execType).trim().toLowerCase();
    const ordStatus = asText(report?.ordStatus).trim().toLowerCase();
    const action = asText(report?.action).trim();
    const quantity = Math.abs(num(report?.lastQty));
    const providerTimestamp = asText(report?.timestamp).trim();
    const providerTimestampMs = providerTimestamp ? new Date(providerTimestamp).getTime() : 0;
    if (providerTimestampMs && Date.now() - providerTimestampMs > COPIER_EVENT_MAX_AGE_MS) return;

    const isFillReport = quantity >= 1 && (
      execType === "trade" ||
      execType === "completed" ||
      ordStatus === "filled"
    );
    if (!reportId || !providerAccountId || !isFillReport) return;

    const inFlightKey = `${this.connection.id}:${reportId}`;
    if (this.copierEventInFlight.has(inFlightKey)) return;
    this.copierEventInFlight.add(inFlightKey);

    try {
      const sourceEntityType = asText(report?.sourceEntityType || eventType).trim().toLowerCase();
      if (sourceEntityType !== "position-delta") this.rememberPrimaryLeaderFill(report);

      const symbol = await this.resolveContractName(contractId);
      const providerFillId = normaliseProviderId(report?.execRefId || report?.id);
      const providerOrderId = normaliseProviderId(report?.orderId);
      const timestamp = providerTimestamp || nowIso();
      const finalFill = ordStatus === "filled" || execType === "completed";

      console.log("copier leader fill detected", {
        connection_id: this.connection.id,
        provider_account_id: providerAccountId,
        action,
        quantity,
        symbol: symbol || null,
        report_id: reportId,
        provider_order_id: providerOrderId || null,
        final_fill: finalFill,
        provider_to_worker_ms: providerTimestampMs ? Math.max(0, workerReceivedAtMs - providerTimestampMs) : null,
      });

      this.enqueueLeaderFillBatch({
        providerAccountId,
        leaderEvent: { id: reportId, timestamp, source_entity_type: sourceEntityType },
        providerFillId: providerFillId || reportId,
        providerOrderId,
        symbol: symbol || `Contract ${contractId}`,
        action,
        quantity,
        contractId,
        eventType,
        finalFill,
        telemetry: {
          providerTimestampMs: Number.isFinite(providerTimestampMs) ? providerTimestampMs : 0,
          workerReceivedAtMs,
          lastPrice: num(report?.lastPx, null),
          avgPrice: num(report?.avgPx, null),
        },
      });
    } catch (error) {
      console.error("copier leader detection failed", this.connection.id, error instanceof Error ? error.message : String(error));
    } finally {
      setTimeout(() => this.copierEventInFlight.delete(inFlightKey), 30000).unref?.();
    }
  }

  async detectLeaderFill(fill, eventType) {
    const fillId = normaliseProviderId(fill?.id);
    const orderId = normaliseProviderId(fill?.orderId);
    if (!fillId || !orderId) return;

    const fillTimestamp = asText(fill?.timestamp).trim();
    const fillTimestampMs = fillTimestamp ? new Date(fillTimestamp).getTime() : 0;
    if (fillTimestampMs && Date.now() - fillTimestampMs > COPIER_EVENT_MAX_AGE_MS) return;

    let order = {};
    const needsOrder = !fill?.accountId || !fill?.contractId || !fill?.action;
    if (needsOrder) {
      try {
        order = await tradovateGet(this.environment, `/order/item?id=${encodeURIComponent(orderId)}`, this.accessToken);
      } catch (error) {
        console.error("copier fill order lookup failed", this.connection.id, orderId, error instanceof Error ? error.message : String(error));
        return;
      }
    }

    const providerAccountId = normaliseProviderId(fill?.accountId || order?.accountId);
    const contractId = normaliseProviderId(fill?.contractId || order?.contractId);
    const action = asText(fill?.action || order?.action).trim();
    const quantity = Math.abs(num(fill?.qty, num(fill?.quantity)));
    if (!providerAccountId || !contractId || quantity < 1) return;

    await this.detectLeaderExecution({
      id: fillId,
      accountId: providerAccountId,
      contractId,
      execType: "Trade",
      ordStatus: asText(fill?.ordStatus || order?.ordStatus).trim() || "Working",
      action,
      lastQty: quantity,
      orderId,
      execRefId: fillId,
      timestamp: fillTimestamp || nowIso(),
      lastPx: num(fill?.price, null),
      avgPx: num(fill?.price, null),
      sourceEntityType: "fill",
      workerReceivedAtMs: num(fill?.workerReceivedAtMs, Date.now()),
      rawFill: fill,
    }, eventType || "fill");
  }

  handleCopierFrame(frame) {
    const frameReceivedAtMs = Date.now();
    for (const detail of propsEvents(frame)) {
      const entityType = asText(detail?.entityType).trim().toLowerCase();
      const eventType = asText(detail?.eventType).trim();
      const entities = Array.isArray(detail?.entity) ? detail.entity : [detail?.entity];
      for (const entity of entities) {
        if (!entity || typeof entity !== "object") continue;

        if (entity?.contractId) this.primeContractName(entity.contractId);

        if (!["executionreport", "fill"].includes(entityType)) continue;
        const enriched = { ...entity, workerReceivedAtMs: frameReceivedAtMs };
        const handler = entityType === "fill"
          ? this.detectLeaderFill(enriched, eventType)
          : this.detectLeaderExecution(enriched, eventType);
        handler.catch(error =>
          console.error("copier frame handler failed", this.connection.id, entityType, error instanceof Error ? error.message : String(error))
        );
      }
    }
  }

  scheduleCopierFillScan(reason = "user-event", delay = 120) {
    if (this.stopped || stopping || !this.accessToken) return;
    if (this.copierFillScanTimer) return;
    this.copierFillScanTimer = setTimeout(() => {
      this.copierFillScanTimer = null;
      this.scanRecentLeaderFills(reason).catch(error =>
        console.error("copier REST fill scan failed", this.connection.id, error instanceof Error ? error.message : String(error))
      );
    }, Math.max(0, delay));
    this.copierFillScanTimer.unref?.();
  }

  async scanRecentLeaderFills(reason = "user-event") {
    if (this.copierFillScanInFlight || !this.accessToken) return;
    this.copierFillScanInFlight = true;
    try {
      const cutoff = Date.now() - COPIER_EVENT_MAX_AGE_MS;
      const [fillsPayload, ordersPayload] = await Promise.all([
        tradovateGet(this.environment, "/fill/list", this.accessToken),
        tradovateGet(this.environment, "/order/list", this.accessToken),
      ]);
      const fills = Array.isArray(fillsPayload) ? fillsPayload : [];
      const orders = Array.isArray(ordersPayload) ? ordersPayload : [];
      const ordersById = new Map(orders.map(order => [normaliseProviderId(order?.id), order]));
      const recent = fills
        .filter(fill => {
          const ts = new Date(asText(fill?.timestamp)).getTime();
          return Number.isFinite(ts) && ts >= cutoff;
        })
        .sort((a, b) => new Date(asText(a?.timestamp)).getTime() - new Date(asText(b?.timestamp)).getTime());

      for (const fill of recent) {
        const fillId = normaliseProviderId(fill?.id);
        const orderId = normaliseProviderId(fill?.orderId);
        if (!fillId || !orderId) continue;
        const order = ordersById.get(orderId) || {};
        const providerAccountId = normaliseProviderId(fill?.accountId || order?.accountId);
        const contractId = normaliseProviderId(fill?.contractId || order?.contractId);
        const action = asText(fill?.action || order?.action).trim();
        const quantity = Math.abs(num(fill?.qty, num(fill?.quantity)));
        if (!providerAccountId || !contractId || quantity < 1) continue;
        await this.detectLeaderExecution({
          id: fillId,
          accountId: providerAccountId,
          contractId,
          execType: "Trade",
          ordStatus: asText(order?.ordStatus).trim() || "Working",
          action,
          lastQty: quantity,
          orderId,
          execRefId: fillId,
          timestamp: asText(fill?.timestamp).trim() || nowIso(),
          lastPx: num(fill?.price, null),
          avgPx: num(fill?.price, null),
          sourceEntityType: "fill-list-fallback",
          workerReceivedAtMs: Date.now(),
          scanReason: reason,
          rawFill: fill,
          rawOrder: order,
        }, "fill-list-fallback");
      }
    } finally {
      this.copierFillScanInFlight = false;
    }
  }

  async scanLeaderPositions(reason = "periodic-position-scan") {
    if (this.copierPositionScanInFlight || !this.accessToken || !this.subscribed) return;
    this.copierPositionScanInFlight = true;
    try {
      const positionsPayload = await tradovateGet(this.environment, "/position/list", this.accessToken);
      const positions = Array.isArray(positionsPayload) ? positionsPayload : [];
      const current = new Map();
      for (const row of positions) {
        const accountId = normaliseProviderId(row?.accountId);
        const contractId = normaliseProviderId(row?.contractId);
        if (!accountId || !contractId) continue;
        const netPos = num(row?.netPos, num(row?.netPosition, 0));
        current.set(`${accountId}:${contractId}`, {
          accountId,
          contractId,
          netPos,
          raw: row,
        });
      }

      if (!this.copierPositionInitialized) {
        this.copierPositionSnapshot = current;
        this.copierPositionInitialized = true;
        console.log("copier position baseline ready", {
          connection_id: this.connection.id,
          positions: current.size,
          reason,
        });
        return;
      }

      const keys = new Set([...this.copierPositionSnapshot.keys(), ...current.keys()]);
      for (const key of keys) {
        const before = this.copierPositionSnapshot.get(key) || { accountId: key.split(":")[0], contractId: key.split(":")[1], netPos: 0 };
        const after = current.get(key) || { accountId: before.accountId, contractId: before.contractId, netPos: 0 };
        const delta = num(after.netPos) - num(before.netPos);
        if (!delta) continue;

        const action = delta > 0 ? "Buy" : "Sell";
        const quantity = Math.abs(delta);
        const positionTimestamp = asText(after.raw?.timestamp).trim() || nowIso();
        const eventId = `pos-${after.accountId}-${after.contractId}-${num(before.netPos)}-${num(after.netPos)}-${Date.now()}`;
        this.queuePositionDeltaFallback({
          id: eventId,
          accountId: after.accountId,
          contractId: after.contractId,
          execType: "Trade",
          ordStatus: "Filled",
          action,
          lastQty: quantity,
          orderId: null,
          execRefId: eventId,
          timestamp: positionTimestamp,
          sourceEntityType: "position-delta",
          scanReason: reason,
          previousNetPos: num(before.netPos),
          currentNetPos: num(after.netPos),
          rawPosition: after.raw || null,
        }, "position-delta");
      }

      this.copierPositionSnapshot = current;
    } catch (error) {
      console.error("copier position scan failed", this.connection.id, error instanceof Error ? error.message : String(error));
    } finally {
      this.copierPositionScanInFlight = false;
    }
  }

  startCopierPositionScanner() {
    if (this.copierPositionScanInterval) clearInterval(this.copierPositionScanInterval);
    this.copierPositionInitialized = false;
    this.copierPositionSnapshot = new Map();
    this.scanLeaderPositions("initial-position-baseline").catch(error =>
      console.error("initial copier position scan failed", this.connection.id, error instanceof Error ? error.message : String(error))
    );
    this.copierPositionScanInterval = setInterval(() => {
      this.scanLeaderPositions("periodic-position-scan").catch(error =>
        console.error("periodic copier position scan failed", this.connection.id, error instanceof Error ? error.message : String(error))
      );
    }, Math.max(500, COPIER_POSITION_SCAN_MS));
    this.copierPositionScanInterval.unref?.();
  }

  async connectOnce() {
    this.authorized = false;
    this.subscribed = false;
    this.lastCloseInfo = "";
    const { token, userIds, accountIds, accountSpec, environment, wsUrl: brokeredWsUrl } = await this.loadCredential();
    this.accessToken = token;
    this.accountSpec = accountSpec || "";
    this.environment = environment;
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
            const syncBody = accountIds?.length
              ? {
                  accounts: accountIds,
                  splitResponses: true,
                  entityTypes: [
                    "account",
                    "accountRiskStatus",
                    "cashBalance",
                    "commandReport",
                    "command",
                    "executionReport",
                    "fill",
                    "fillPair",
                    "order",
                    "orderStrategy",
                    "position",
                  ],
                }
              : { users: userIds, splitResponses: true };

            sendRequest(ws, "user/syncrequest", 1, syncBody);
            continue;
          }

          if (Number(frame.i) === 1 && Number(frame.s) >= 400) {
            return finishReject(new Error(`user/syncrequest: ${frameError(frame, "Tradovate user synchronization failed")}`));
          }

          if (Number(frame.i) === 1 && Number(frame.s) === 200 && !this.subscribed) {
            this.subscribed = true;
            this.backoffMs = 1000;

            await supabase.from("provider_sync_checkpoints").upsert({
              connection_id: this.connection.id,
              checkpoint_key: "copier_worker_version",
              checkpoint_value: VERSION,
              metadata: {
                worker_instance: INSTANCE_ID,
                detection_mode: "fast-rpc-dispatch-with-delayed-position-fallback",
                follower_execution_enabled: true,
                execution_gate: "atomic prepare_live_copier_dispatch_v1 + follower.enabled",
                provider_order_mode: "market-isAutomated",
                follower_dispatch_mode: "parallel-fast-path",
                dispatch_preflight: "single-supabase-rpc",
                audit_mode: "async-after-dispatch-reservation",
                latency_telemetry: true,
                partial_fill_mode: "final-fill-immediate-partial-burst",
                partial_fill_quiet_ms: COPIER_PARTIAL_FILL_QUIET_MS,
                partial_fill_max_ms: COPIER_PARTIAL_FILL_MAX_MS,
                emergency_flatten_enabled: true,
                emergency_flatten_endpoint: "order/liquidateposition",
                emergency_command_poll_ms: COPIER_COMMAND_POLL_MS,
                safety_disarm_on_execution_error: true,
                rest_fallback_delay_ms: COPIER_REST_FALLBACK_DELAY_MS,
              },
              updated_at: nowIso(),
            }, { onConflict: "connection_id,checkpoint_key" });

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
            this.scheduleCopierFillScan("initial-live-snapshot", 500);
            this.startCopierPositionScanner();

            if (this.sessionRefreshTimer) clearTimeout(this.sessionRefreshTimer);

            this.sessionRefreshTimer = setTimeout(() => {
              if (this.ws?.readyState === WebSocket.OPEN) {
                console.log("scheduled Tradovate token refresh reconnect", this.connection.id);
                try { this.ws.close(1000, "scheduled-token-refresh"); } catch {}
              }
            }, SESSION_RECONNECT_MS);

            this.sessionRefreshTimer.unref?.();

            if (!settled) {
              settled = true;
              resolve();
            }

            continue;
          }

          if (this.authorized) {
            this.handleCopierFrame(frame);
            this.scheduleCopierFillScan("tradovate-user-event", COPIER_REST_FALLBACK_DELAY_MS);
            this.eventCount += 1;
            const eventAt = nowIso();

            writeStatus(this.connection, {
              state: "live",
              last_event_at: eventAt,
              last_heartbeat_at: eventAt,
              event_count: this.eventCount,
              reconnect_count: this.reconnectCount,
              last_error: null,
            }).catch(error =>
              console.error("event status update failed", this.connection.id, error instanceof Error ? error.message : String(error))
            );

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

        if (!this.stopped && !stopping) {
          finishReject(new Error(`Tradovate WebSocket closed (${code}) ${asText(reason)}`.trim()));
        } else if (!settled) {
          settled = true;
          resolve();
        }
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

    while (!this.stopped && !stopping && this.ws?.readyState === WebSocket.OPEN) {
      await sleep(1000);
    }

    if (!this.stopped && !stopping) {
      throw new Error(this.lastCloseInfo || "Tradovate WebSocket disconnected");
    }
  }

  clearHeartbeat() {
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
    if (this.socketHeartbeatTimer) clearInterval(this.socketHeartbeatTimer);
    if (this.sessionRefreshTimer) clearTimeout(this.sessionRefreshTimer);
    if (this.copierFillScanTimer) clearTimeout(this.copierFillScanTimer);
    if (this.copierPositionScanInterval) clearInterval(this.copierPositionScanInterval);

    for (const timer of this.pendingPositionFallbacks.values()) clearTimeout(timer);
    for (const batch of this.pendingLeaderFillBatches.values()) {
      if (batch?.timer) clearTimeout(batch.timer);
    }

    this.pendingLeaderFillBatches.clear();
    this.pendingPositionFallbacks.clear();
    this.recentPrimaryLeaderFills.clear();
    this.contractNamePromiseCache.clear();

    this.heartbeatTimer = null;
    this.socketHeartbeatTimer = null;
    this.sessionRefreshTimer = null;
    this.copierFillScanTimer = null;
    this.copierPositionScanInterval = null;
    this.copierPositionInitialized = false;
    this.copierPositionSnapshot = new Map();
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
        body: JSON.stringify({
          connection_id: this.connection.id,
          reason,
        }),
      });

      const payload = await response.json().catch(() => ({}));

      if (!response.ok || payload?.error) {
        throw new Error(payload?.error || `Live pulse failed (${response.status})`);
      }

      this.pulseCount += 1;

      await writeStatus(this.connection, {
        state: payload.requires_full_sync ? "attention" : "live",
        last_pulse_at: nowIso(),
        last_success_at: nowIso(),
        last_heartbeat_at: nowIso(),
        pulse_count: this.pulseCount,
        event_count: this.eventCount,
        last_error: payload.requires_full_sync
          ? "A newly detected account needs one full sync"
          : null,
        metadata: {
          last_pulse_result: payload,
          last_reason: reason,
        },
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

    try {
      this.ws?.close(1000, reason);
    } catch {}

    this.ws = null;

    await writeStatus(this.connection, {
      state: reason === "not-eligible" ? "manual" : "offline",
      last_heartbeat_at: nowIso(),
      last_error: reason === "not-eligible"
        ? "Live sync is available on paid plans"
        : null,
    });
  }
}

const OPEN_ORDER_STATUSES = new Set([
  "working",
  "pendingnew",
  "pendingreplace",
  "pendingcancel",
  "suspended",
  "unknown",
]);

function isOpenTradovateOrder(order) {
  const status = asText(order?.ordStatus).trim().toLowerCase();
  return OPEN_ORDER_STATUSES.has(status);
}

function normalizeFlattenTargets(command) {
  const rawTargets = Array.isArray(command?.payload?.targets)
    ? command.payload.targets
    : [];

  const seen = new Set();
  const targets = [];

  for (const row of rawTargets) {
    const accountId = asText(row?.account_id).trim();
    const connectionId = asText(row?.source_connection_id).trim();
    const providerAccountId = normaliseProviderId(row?.provider_account_id);

    if (!accountId || !connectionId || !providerAccountId) continue;

    const key = `${connectionId}:${providerAccountId}`;
    if (seen.has(key)) continue;

    seen.add(key);

    targets.push({
      account_id: accountId,
      name: asText(row?.name).trim() || providerAccountId,
      source_connection_id: connectionId,
      provider_account_id: providerAccountId,
      was_enabled: row?.was_enabled === true,
    });
  }

  return targets;
}

async function loadFlattenTargetsFromDatabase(command) {
  const ownerId = asText(command?.owner_id).trim();
  const groupId = asText(command?.group_id).trim();
  if (!ownerId || !groupId) return [];

  const { data: followers, error: followerError } = await supabase
    .from("copier_followers")
    .select("account_id,enabled")
    .eq("group_id", groupId)
    .eq("owner_id", ownerId);

  if (followerError) throw followerError;

  const accountIds = [...new Set(
    (followers || []).map(row => row.account_id).filter(Boolean)
  )];

  if (!accountIds.length) return [];

  const { data: accounts, error: accountError } = await supabase
    .from("accounts")
    .select("id,name,external_id,source_connection_id,is_archived")
    .eq("owner_id", ownerId)
    .in("id", accountIds);

  if (accountError) throw accountError;

  const enabledById = new Map(
    (followers || []).map(row => [String(row.account_id), row.enabled === true])
  );

  return (accounts || [])
    .filter(row =>
      !row.is_archived &&
      row.source_connection_id &&
      row.external_id
    )
    .map(row => ({
      account_id: row.id,
      name: asText(row.name).trim() || normaliseProviderId(row.external_id),
      source_connection_id: asText(row.source_connection_id).trim(),
      provider_account_id: normaliseProviderId(row.external_id),
      was_enabled: enabledById.get(String(row.id)) === true,
    }));
}

async function emergencyRestSession(connectionId) {
  const live = sessions.get(connectionId);

  if (live?.accessToken && live?.environment) {
    return {
      token: live.accessToken,
      environment: live.environment,
      source: "live-session",
    };
  }

  const brokered = await requestWorkerSession(connectionId);

  return {
    token: brokered.token,
    environment: brokered.environment,
    source: "fresh-rest-session",
  };
}

async function writeFlattenEvent(command, eventType, status, message, payload = {}) {
  const dedupeKey = `flatten:${command.id}:${eventType}`;

  const { error } = await supabase.from("copier_events").insert({
    owner_id: command.owner_id,
    group_id: command.group_id,
    event_type: eventType,
    status,
    dedupe_key: dedupeKey,
    message,
    payload: {
      command_id: command.id,
      worker_instance: INSTANCE_ID,
      worker_version: VERSION,
      ...payload,
    },
  });

  if (error && error.code !== "23505") {
    console.error("flatten audit insert failed", command.id, error.message);
  }
}

async function completeCopierCommand(command, status, result = {}, errorMessage = null) {
  const { error } = await supabase
    .from("copier_commands")
    .update({
      status,
      result,
      error: errorMessage,
      completed_at: nowIso(),
      worker_instance: INSTANCE_ID,
    })
    .eq("id", command.id)
    .eq("worker_instance", INSTANCE_ID);

  if (error) throw error;
}

async function forceGroupDisarmed(command) {
  const { error } = await supabase
    .from("copier_groups")
    .update({
      armed: false,
      desired_armed: false,
      updated_at: nowIso(),
    })
    .eq("id", command.group_id)
    .eq("owner_id", command.owner_id);

  if (error) throw error;
}

async function flattenConnectionTargets(command, connectionId, targets) {
  const rest = await emergencyRestSession(connectionId);

  const providerIds = new Set(
    targets
      .map(row => normaliseProviderId(row.provider_account_id))
      .filter(Boolean)
  );

  const numericProviderIds = new Set(
    [...providerIds].map(Number).filter(Number.isFinite)
  );

  const result = {
    connection_id: connectionId,
    session_source: rest.source,
    environment: rest.environment,
    account_count: targets.length,
    cancelled_orders: 0,
    cancel_errors: [],
    liquidation_requests: 0,
    liquidation_errors: [],
    residual_positions: [],
    residual_orders: [],
    verification_ok: false,
  };

  let orders = [];

  try {
    const payload = await tradovateGet(
      rest.environment,
      "/order/list",
      rest.token
    );

    orders = Array.isArray(payload) ? payload : [];
  } catch (error) {
    result.cancel_errors.push({
      scope: "order-list",
      error: error instanceof Error ? error.message : String(error),
    });
  }

  const openOrders = orders.filter(order => {
    const accountId = Number(order?.accountId);

    return (
      Number.isFinite(accountId) &&
      numericProviderIds.has(accountId) &&
      isOpenTradovateOrder(order) &&
      Number(order?.id) > 0
    );
  });

  const cancelResults = await Promise.allSettled(
    openOrders.map(async order => {
      await tradovatePost(
        rest.environment,
        "/order/cancelorder",
        rest.token,
        {
          orderId: Number(order.id),
        }
      );

      return {
        order_id: Number(order.id),
        account_id: Number(order.accountId),
        contract_id: Number(order.contractId) || null,
      };
    })
  );

  cancelResults.forEach((entry, index) => {
    if (entry.status === "fulfilled") {
      result.cancelled_orders += 1;
    } else {
      result.cancel_errors.push({
        order_id: Number(openOrders[index]?.id) || null,
        error: entry.reason instanceof Error
          ? entry.reason.message
          : String(entry.reason),
      });
    }
  });

  const liquidationStates = new Set();

  const submitLiquidation = async position => {
    const accountId = Number(position?.accountId);
    const contractId = Number(position?.contractId);
    const netPos = num(
      position?.netPos,
      num(position?.netPosition, 0)
    );

    if (
      !Number.isFinite(accountId) ||
      !Number.isFinite(contractId) ||
      !contractId ||
      !netPos
    ) {
      return;
    }

    const stateKey =
      `${accountId}:${contractId}:${Math.sign(netPos)}:${Math.abs(netPos)}`;

    if (liquidationStates.has(stateKey)) return;

    liquidationStates.add(stateKey);

    try {
      const response = await tradovatePost(
        rest.environment,
        "/order/liquidateposition",
        rest.token,
        {
          accountId,
          contractId,
          admin: false,
          isAutomated: true,
          customTag50:
            `FRA-FLAT-${asText(command.id).slice(0, 8)}`,
        }
      );

      result.liquidation_requests += 1;
      return response;
    } catch (error) {
      result.liquidation_errors.push({
        account_id: accountId,
        contract_id: contractId,
        net_position: netPos,
        error: error instanceof Error
          ? error.message
          : String(error),
      });
    }
  };

  let positions = [];

  try {
    const payload = await tradovateGet(
      rest.environment,
      "/position/list",
      rest.token
    );

    positions = Array.isArray(payload) ? payload : [];
  } catch (error) {
    result.liquidation_errors.push({
      scope: "position-list",
      error: error instanceof Error ? error.message : String(error),
    });
  }

  const initialOpenPositions = positions.filter(position => {
    const accountId = Number(position?.accountId);
    const netPos = num(
      position?.netPos,
      num(position?.netPosition, 0)
    );

    return (
      Number.isFinite(accountId) &&
      numericProviderIds.has(accountId) &&
      netPos !== 0
    );
  });

  await Promise.allSettled(
    initialOpenPositions.map(submitLiquidation)
  );

  let finalPositions = initialOpenPositions;
  let finalOrders = openOrders;

  for (
    let attempt = 0;
    attempt < COPIER_FLATTEN_VERIFY_ATTEMPTS;
    attempt += 1
  ) {
    await sleep(COPIER_FLATTEN_VERIFY_MS);

    const [positionResult, orderResult] =
      await Promise.allSettled([
        tradovateGet(
          rest.environment,
          "/position/list",
          rest.token
        ),
        tradovateGet(
          rest.environment,
          "/order/list",
          rest.token
        ),
      ]);

    if (positionResult.status === "fulfilled") {
      const rows = Array.isArray(positionResult.value)
        ? positionResult.value
        : [];

      finalPositions = rows.filter(position => {
        const accountId = Number(position?.accountId);
        const netPos = num(
          position?.netPos,
          num(position?.netPosition, 0)
        );

        return (
          Number.isFinite(accountId) &&
          numericProviderIds.has(accountId) &&
          netPos !== 0
        );
      });
    }

    if (orderResult.status === "fulfilled") {
      const rows = Array.isArray(orderResult.value)
        ? orderResult.value
        : [];

      finalOrders = rows.filter(order => {
        const accountId = Number(order?.accountId);

        return (
          Number.isFinite(accountId) &&
          numericProviderIds.has(accountId) &&
          isOpenTradovateOrder(order) &&
          Number(order?.id) > 0
        );
      });
    }

    if (
      positionResult.status === "fulfilled" &&
      orderResult.status === "fulfilled" &&
      !finalPositions.length &&
      !finalOrders.length
    ) {
      result.verification_ok = true;
      break;
    }

    if (finalOrders.length) {
      await Promise.allSettled(
        finalOrders.map(async order => {
          try {
            await tradovatePost(
              rest.environment,
              "/order/cancelorder",
              rest.token,
              {
                orderId: Number(order.id),
              }
            );
          } catch {}
        })
      );
    }

    if (finalPositions.length && attempt >= 1) {
      await Promise.allSettled(
        finalPositions.map(submitLiquidation)
      );
    }
  }

  result.residual_positions =
    finalPositions.map(position => ({
      account_id: Number(position?.accountId) || null,
      contract_id: Number(position?.contractId) || null,
      net_position: num(
        position?.netPos,
        num(position?.netPosition, 0)
      ),
    }));

  result.residual_orders =
    finalOrders.map(order => ({
      order_id: Number(order?.id) || null,
      account_id: Number(order?.accountId) || null,
      contract_id: Number(order?.contractId) || null,
      status: asText(order?.ordStatus).trim() || null,
    }));

  return result;
}

async function processFlattenCommand(command) {
  const startedAt = Date.now();

  await forceGroupDisarmed(command);

  await writeFlattenEvent(
    command,
    "flatten_started",
    "pending",
    "EMERGENCY FLATTEN STARTED · all configured followers are being checked",
    {
      requested_at: command.requested_at || null,
    }
  );

  let targets = normalizeFlattenTargets(command);

  if (!targets.length) {
    targets = await loadFlattenTargetsFromDatabase(command);
  }

  if (!targets.length) {
    const result = {
      ok: true,
      target_count: 0,
      connection_results: [],
      elapsed_ms: Date.now() - startedAt,
    };

    await completeCopierCommand(
      command,
      "success",
      result,
      null
    );

    await writeFlattenEvent(
      command,
      "flatten_completed",
      "success",
      "EMERGENCY FLATTEN COMPLETE · no configured follower accounts to close",
      result
    );

    return;
  }

  const byConnection = new Map();

  for (const target of targets) {
    const key = target.source_connection_id;
    const list = byConnection.get(key) || [];
    list.push(target);
    byConnection.set(key, list);
  }

  const entries = [...byConnection.entries()];

  const settled = await Promise.allSettled(
    entries.map(([connectionId, connectionTargets]) =>
      flattenConnectionTargets(
        command,
        connectionId,
        connectionTargets
      )
    )
  );

  const connectionResults =
    settled.map((entry, index) => {
      const connectionId = entries[index][0];

      if (entry.status === "fulfilled") {
        return entry.value;
      }

      return {
        connection_id: connectionId,
        account_count:
          byConnection.get(connectionId)?.length || 0,
        fatal_error:
          entry.reason instanceof Error
            ? entry.reason.message
            : String(entry.reason),
        residual_positions: [],
        residual_orders: [],
        verification_ok: false,
      };
    });

  const fatalCount =
    connectionResults.filter(row => row.fatal_error).length;

  const residualPositionCount =
    connectionResults.reduce(
      (sum, row) =>
        sum + (row.residual_positions?.length || 0),
      0
    );

  const residualOrderCount =
    connectionResults.reduce(
      (sum, row) =>
        sum + (row.residual_orders?.length || 0),
      0
    );

  const actionErrorCount =
    connectionResults.reduce(
      (sum, row) =>
        sum +
        (row.cancel_errors?.length || 0) +
        (row.liquidation_errors?.length || 0),
      0
    );

  const verificationFailureCount =
    connectionResults.filter(
      row => row.verification_ok !== true
    ).length;

  const clean =
    fatalCount === 0 &&
    verificationFailureCount === 0 &&
    residualPositionCount === 0 &&
    residualOrderCount === 0;

  const status = clean
    ? "success"
    : (
      fatalCount === connectionResults.length
        ? "error"
        : "partial"
    );

  const result = {
    ok: clean,
    target_count: targets.length,
    connection_count: byConnection.size,
    fatal_count: fatalCount,
    verification_failure_count:
      verificationFailureCount,
    action_error_count: actionErrorCount,
    residual_position_count:
      residualPositionCount,
    residual_order_count:
      residualOrderCount,
    elapsed_ms: Date.now() - startedAt,
    connection_results: connectionResults,
  };

  const errorMessage = clean
    ? null
    : `Flatten incomplete: ${residualPositionCount} open position(s), ${residualOrderCount} working order(s), ${fatalCount} unavailable connection(s), ${verificationFailureCount} unverified connection(s)`;

  await completeCopierCommand(
    command,
    status,
    result,
    errorMessage
  );

  await writeFlattenEvent(
    command,
    clean
      ? "flatten_completed"
      : "flatten_attention",
    status,
    clean
      ? `EMERGENCY FLATTEN COMPLETE · ${targets.length} follower account(s) checked and flat`
      : `EMERGENCY FLATTEN NEEDS ATTENTION · ${errorMessage}`,
    result
  );
}

async function failCopierCommand(command, error) {
  const message =
    error instanceof Error
      ? error.message
      : String(error);

  try {
    await forceGroupDisarmed(command);

    await completeCopierCommand(
      command,
      "error",
      {
        ok: false,
        worker_version: VERSION,
        worker_instance: INSTANCE_ID,
      },
      message
    );

    await writeFlattenEvent(
      command,
      "flatten_failed",
      "error",
      `EMERGENCY FLATTEN FAILED · ${message}`,
      {
        error: message,
      }
    );
  } catch (secondaryError) {
    console.error(
      "unable to record failed copier command",
      command?.id,
      secondaryError instanceof Error
        ? secondaryError.message
        : String(secondaryError)
    );
  }
}

async function runCopierCommandPump() {
  console.log(
    `copier emergency command pump active · ${COPIER_COMMAND_POLL_MS}ms`
  );

  while (!stopping) {
    try {
      const { data, error } = await supabase.rpc(
        "claim_copier_commands_v1",
        {
          p_worker_instance: INSTANCE_ID,
          p_limit: 4,
        }
      );

      if (error) throw error;

      const commands =
        Array.isArray(data) ? data : [];

      for (const command of commands) {
        if (stopping) break;

        if (
          asText(command?.command_type).trim() !==
          "flatten_followers"
        ) {
          await completeCopierCommand(
            command,
            "error",
            {
              ok: false,
            },
            `Unsupported copier command: ${command?.command_type || "unknown"}`
          );

          continue;
        }

        try {
          await processFlattenCommand(command);
        } catch (error) {
          console.error(
            "copier emergency command failed",
            command?.id,
            error instanceof Error
              ? error.message
              : String(error)
          );

          await failCopierCommand(
            command,
            error
          );
        }
      }
    } catch (error) {
      console.error(
        "copier command pump failed",
        error instanceof Error
          ? error.message
          : String(error)
      );
    }

    await sleep(COPIER_COMMAND_POLL_MS);
  }
}

async function reconcileTargets() {
  const targets = await loadTargets();
  const targetById =
    new Map(targets.map(row => [row.id, row]));

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

    session.start().catch(error =>
      console.error(
        "session stopped unexpectedly",
        target.id,
        error
      )
    );
  }

  const liveCount =
    [...sessions.values()].filter(
      session =>
        session.subscribed &&
        session.ws?.readyState === WebSocket.OPEN
    ).length;

  const connectingCount =
    Math.max(sessions.size - liveCount, 0);

  console.log(
    `[${nowIso()}] live targets=${targets.length} subscribed=${liveCount} connecting=${connectingCount}`
  );
}

async function shutdown(signal) {
  if (stopping) return;

  stopping = true;

  console.log(
    `received ${signal}; closing ${sessions.size} live sessions`
  );

  await Promise.allSettled(
    [...sessions.values()].map(
      session => session.stop("worker-shutdown")
    )
  );

  process.exit(0);
}

process.on(
  "SIGTERM",
  () => shutdown("SIGTERM")
);

process.on(
  "SIGINT",
  () => shutdown("SIGINT")
);

process.on(
  "unhandledRejection",
  error =>
    console.error(
      "unhandled rejection",
      error
    )
);

process.on(
  "uncaughtException",
  error =>
    console.error(
      "uncaught exception",
      error
    )
);

console.log(
  `FRA Prop HQ Tradovate live worker v${VERSION} starting as ${INSTANCE_ID}`
);

runCopierCommandPump().catch(error =>
  console.error(
    "copier command pump stopped unexpectedly",
    error
  )
);

await reconcileTargets();

while (!stopping) {
  await sleep(TARGET_REFRESH_MS);

  try {
    await reconcileTargets();
  } catch (error) {
    console.error(
      "target refresh failed",
      error
    );
  }
}
