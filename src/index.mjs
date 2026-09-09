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
const VERSION = "7.7.17";
const COPIER_EVENT_MAX_AGE_MS = Number(process.env.COPIER_EVENT_MAX_AGE_MS || 15 * 60 * 1000);
const COPIER_POSITION_SCAN_MS = Number(process.env.COPIER_POSITION_SCAN_MS || 1000);
const COPIER_POSITION_FALLBACK_GRACE_MS = Number(process.env.COPIER_POSITION_FALLBACK_GRACE_MS || 1500);
const COPIER_PRIMARY_MATCH_WINDOW_MS = Number(process.env.COPIER_PRIMARY_MATCH_WINDOW_MS || 2000);
const COPIER_FILL_BURST_QUIET_MS = Math.max(100, Number(process.env.COPIER_FILL_BURST_QUIET_MS || 450));
const COPIER_FILL_BURST_MAX_MS = Math.max(COPIER_FILL_BURST_QUIET_MS, Number(process.env.COPIER_FILL_BURST_MAX_MS || 900));

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
  const failure = payload?.failureText || payload?.failureReason || payload?.errorText || payload?.error;
  if (!response.ok || failure) {
    throw new Error(failure || `${path} failed (${response.status})`);
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
    try {
      const contract = await tradovateGet(this.environment, `/contract/item?id=${encodeURIComponent(key)}`, this.accessToken);
      const name = asText(contract?.name).trim();
      if (name) this.contractNameCache.set(key, name);
      return name;
    } catch (error) {
      console.error("contract lookup failed", this.connection.id, key, error instanceof Error ? error.message : String(error));
      return "";
    }
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

  async executeFollowerOrder({ group, follower, followerAccount, leaderEvent, symbol, action, quantity, providerFillId, contractId }) {
    const targetConnectionId = asText(followerAccount?.source_connection_id).trim();
    const providerAccountId = normaliseProviderId(followerAccount?.external_id);
    if (!targetConnectionId || !providerAccountId) return;

    const targetSession = sessions.get(targetConnectionId);
    if (!targetSession || !targetSession.accessToken || !targetSession.subscribed) {
      await supabase.from("copier_events").insert({
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
        payload: { reason: "follower_session_unavailable", target_connection_id: targetConnectionId },
      });
      return;
    }

    const accountSpec = asText(followerAccount?.name || targetSession.accountSpec).trim();
    if (!accountSpec) {
      await supabase.from("copier_events").insert({
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
        payload: { reason: "tradovate_accountspec_missing", target_connection_id: targetConnectionId },
      });
      return;
    }

    const multiplier = Math.max(0, num(follower.multiplier, 1));
    const computedQty = Math.floor(Math.abs(quantity) * multiplier + 1e-9);
    const maxQty = Math.max(1, Math.floor(num(follower.max_qty, 1)));
    if (computedQty < 1 || computedQty > maxQty) {
      await supabase.from("copier_events").insert({
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
        payload: { reason: "quantity_limit", multiplier, max_qty: maxQty, leader_quantity: quantity },
      });
      return;
    }

    if (!symbolAllowed(symbol, follower.allowed_symbols)) {
      await supabase.from("copier_events").insert({
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
        payload: { reason: "symbol_not_allowed", allowed_symbols: follower.allowed_symbols || [] },
      });
      return;
    }

    const dailyLossLimit = Math.abs(num(follower.max_daily_loss, 0));
    if (dailyLossLimit > 0) {
      const { data: accountRow } = await supabase.from("accounts")
        .select("daily_pnl")
        .eq("id", follower.account_id)
        .maybeSingle();
      if (num(accountRow?.daily_pnl, 0) <= -dailyLossLimit) {
        await supabase.from("copier_events").insert({
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
          payload: { reason: "daily_loss_limit", max_daily_loss: dailyLossLimit, daily_pnl: num(accountRow?.daily_pnl, 0) },
        });
        return;
      }
    }

    const routeKey = `${group.id}:${follower.account_id}:${providerFillId}`;
    if (this.followerOrderInFlight.has(routeKey)) return;
    this.followerOrderInFlight.add(routeKey);

    const clientOrderId = copierClientOrderId(group.id, follower.account_id, providerFillId);
    const reserveDedupeKey = `route:${group.id}:${providerFillId}:${follower.account_id}:reserved`;
    const { error: reserveError } = await supabase.from("copier_events").insert({
      owner_id: this.connection.owner_id,
      group_id: group.id,
      leader_account_id: group.leader_account_id,
      follower_account_id: follower.account_id,
      event_type: "follower_order_reserved",
      status: "pending",
      dedupe_key: reserveDedupeKey,
      provider_fill_id: providerFillId,
      symbol,
      action,
      quantity: computedQty,
      message: `Reserved → ${followerAccount?.name || providerAccountId}: ${action} ${computedQty} ${symbol}`,
      payload: { cl_ord_id: clientOrderId, target_connection_id: targetConnectionId, target_provider_account_id: providerAccountId },
    });
    if (reserveError) {
      this.followerOrderInFlight.delete(routeKey);
      if (reserveError.code === "23505") return;
      throw reserveError;
    }

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
      const providerOrderId = normaliseProviderId(response?.orderId || response?.commandId);
      await supabase.from("copier_events").insert({
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
        },
      });
    } catch (error) {
      await supabase.from("copier_events").insert({
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
        payload: { cl_ord_id: clientOrderId, target_connection_id: targetConnectionId, target_provider_account_id: providerAccountId },
      });
    } finally {
      setTimeout(() => this.followerOrderInFlight.delete(routeKey), 30000).unref?.();
    }
  }

  aggregateFillId(groupId, providerOrderId, fillIds) {
    const digest = crypto.createHash("sha256")
      .update(`${groupId}:${providerOrderId || "no-order"}:${[...fillIds].sort().join(",")}`)
      .digest("hex")
      .slice(0, 28);
    return `agg-${digest}`;
  }

  async flushLeaderFillBatch(batchKey) {
    const batch = this.pendingLeaderFillBatches.get(batchKey);
    if (!batch) return;
    if (batch.timer) clearTimeout(batch.timer);
    this.pendingLeaderFillBatches.delete(batchKey);

    if (this.stopped || stopping || !batch.fillIds.size || batch.quantity < 1) return;

    const { data: currentGroup, error: groupError } = await supabase.from("copier_groups")
      .select("id,owner_id,name,mode,armed,leader_account_id,max_leader_qty,allowed_symbols")
      .eq("id", batch.group.id)
      .eq("owner_id", this.connection.owner_id)
      .maybeSingle();
    if (groupError) throw groupError;
    if (!currentGroup || asText(currentGroup.mode).trim().toLowerCase() !== "live" || !currentGroup.armed) return;

    const aggregateProviderFillId = this.aggregateFillId(currentGroup.id, batch.providerOrderId, batch.fillIds);
    const aggregateQuantity = Math.abs(num(batch.quantity));
    const maxLeaderQty = Math.max(1, num(currentGroup.max_leader_qty, 1));

    if (!symbolAllowed(batch.symbol, currentGroup.allowed_symbols) || aggregateQuantity > maxLeaderQty) {
      const reason = !symbolAllowed(batch.symbol, currentGroup.allowed_symbols)
        ? `${batch.symbol} is not allowed by this copier group`
        : `Aggregated leader fill quantity ${aggregateQuantity} exceeds copier max ${maxLeaderQty}`;
      await supabase.from("copier_events").insert({
        owner_id: this.connection.owner_id,
        group_id: currentGroup.id,
        leader_account_id: currentGroup.leader_account_id,
        event_type: "live_leader_fill_aggregate_blocked",
        status: "blocked",
        dedupe_key: `live:${this.connection.id}:aggregate:${aggregateProviderFillId}:blocked`,
        provider_order_id: batch.providerOrderId || null,
        provider_fill_id: aggregateProviderFillId,
        symbol: batch.symbol || null,
        action: batch.action || null,
        quantity: aggregateQuantity,
        message: `LIVE LEADER FILL BLOCKED · ${reason}`,
        payload: {
          aggregated: true,
          leader_fill_ids: [...batch.fillIds],
          leader_provider_order_id: batch.providerOrderId || null,
          burst_quiet_ms: COPIER_FILL_BURST_QUIET_MS,
          burst_max_ms: COPIER_FILL_BURST_MAX_MS,
        },
      });
      return;
    }

    console.log("copier leader fill batch ready", {
      connection_id: this.connection.id,
      group_id: currentGroup.id,
      provider_order_id: batch.providerOrderId || null,
      fill_ids: [...batch.fillIds],
      action: batch.action,
      quantity: aggregateQuantity,
      symbol: batch.symbol,
      batch_age_ms: Date.now() - batch.firstSeenAt,
    });

    await this.routeLeaderFillToFollowers({
      group: currentGroup,
      leaderEvent: {
        id: batch.leaderEvent?.id || aggregateProviderFillId,
        timestamp: batch.leaderEvent?.timestamp || nowIso(),
        source_entity_type: "aggregated-primary-fill",
        component_fill_ids: [...batch.fillIds],
        provider_order_id: batch.providerOrderId || null,
      },
      providerFillId: aggregateProviderFillId,
      symbol: batch.symbol,
      action: batch.action,
      quantity: aggregateQuantity,
      contractId: batch.contractId,
    });
  }

  enqueueLeaderFillBatch({ group, leaderEvent, providerFillId, providerOrderId, symbol, action, quantity, contractId }) {
    if (!providerOrderId) {
      this.routeLeaderFillToFollowers({
        group, leaderEvent, providerFillId, symbol, action, quantity, contractId,
      }).catch(error => console.error("copier immediate follower route failed", this.connection.id, error instanceof Error ? error.message : String(error)));
      return;
    }

    const batchKey = `${group.id}:${providerOrderId}:${asText(action).trim().toLowerCase()}:${asText(symbol).trim().toUpperCase()}`;
    const now = Date.now();
    let batch = this.pendingLeaderFillBatches.get(batchKey);
    if (!batch) {
      batch = {
        group,
        leaderEvent,
        providerOrderId,
        symbol,
        action,
        contractId,
        fillIds: new Set(),
        quantity: 0,
        firstSeenAt: now,
        lastSeenAt: now,
        timer: null,
      };
      this.pendingLeaderFillBatches.set(batchKey, batch);
    }

    if (batch.fillIds.has(providerFillId)) return;
    batch.fillIds.add(providerFillId);
    batch.quantity += Math.abs(num(quantity));
    batch.lastSeenAt = now;
    batch.leaderEvent = leaderEvent || batch.leaderEvent;

    if (batch.timer) clearTimeout(batch.timer);
    const age = now - batch.firstSeenAt;
    const wait = age >= COPIER_FILL_BURST_MAX_MS
      ? 0
      : Math.min(COPIER_FILL_BURST_QUIET_MS, Math.max(0, COPIER_FILL_BURST_MAX_MS - age));
    batch.timer = setTimeout(() => {
      this.flushLeaderFillBatch(batchKey).catch(error =>
        console.error("copier fill batch flush failed", this.connection.id, error instanceof Error ? error.message : String(error))
      );
    }, wait);
    batch.timer.unref?.();
  }

  async routeLeaderFillToFollowers({ group, leaderEvent, providerFillId, symbol, action, quantity, contractId }) {
    if (asText(group.mode).trim().toLowerCase() !== "live" || !group.armed) return;
    const { data: followers, error: followerError } = await supabase.from("copier_followers")
      .select("id,group_id,owner_id,account_id,enabled,multiplier,max_qty,max_daily_loss,allowed_symbols")
      .eq("group_id", group.id)
      .eq("owner_id", this.connection.owner_id)
      .eq("enabled", true);
    if (followerError) throw followerError;
    if (!followers?.length) return;

    const followerAccountIds = followers.map(row => row.account_id).filter(Boolean);
    const { data: followerAccounts, error: accountError } = await supabase.from("accounts")
      .select("id,name,external_id,source_connection_id,status,is_archived")
      .in("id", followerAccountIds);
    if (accountError) throw accountError;
    const accountById = new Map((followerAccounts || []).map(row => [row.id, row]));

    const activeRoutes = followers.map(follower => {
      const followerAccount = accountById.get(follower.account_id);
      if (!followerAccount || followerAccount.is_archived || asText(followerAccount.status).toLowerCase() !== "active") return null;
      return { follower, followerAccount };
    }).filter(Boolean);

    const startedAt = Date.now();
    const results = await Promise.allSettled(activeRoutes.map(({ follower, followerAccount }) =>
      this.executeFollowerOrder({
        group, follower, followerAccount, leaderEvent, symbol, action, quantity, providerFillId, contractId,
      })
    ));

    const rejected = results.filter(result => result.status === "rejected");
    if (rejected.length) {
      console.error("copier parallel follower batch had errors", {
        connection_id: this.connection.id,
        group_id: group.id,
        provider_fill_id: providerFillId,
        followers: activeRoutes.length,
        rejected: rejected.length,
        elapsed_ms: Date.now() - startedAt,
      });
    } else {
      console.log("copier parallel follower batch complete", {
        connection_id: this.connection.id,
        group_id: group.id,
        provider_fill_id: providerFillId,
        followers: activeRoutes.length,
        elapsed_ms: Date.now() - startedAt,
      });
    }
  }

  async detectLeaderExecution(report, eventType) {
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
      const { data: link, error: linkError } = await supabase
        .from("provider_account_links")
        .select("account_id,provider_account_id")
        .eq("connection_id", this.connection.id)
        .eq("provider_account_id", providerAccountId)
        .maybeSingle();
      if (linkError) throw linkError;

      let localAccountId = link?.account_id || null;
      if (!localAccountId) {
        const { data: fallbackAccount, error: fallbackError } = await supabase
          .from("accounts")
          .select("id")
          .eq("owner_id", this.connection.owner_id)
          .eq("external_id", providerAccountId)
          .eq("is_archived", false)
          .maybeSingle();
        if (fallbackError) throw fallbackError;
        localAccountId = fallbackAccount?.id || null;
      }
      if (!localAccountId) return;

      const { data: groups, error: groupError } = await supabase
        .from("copier_groups")
        .select("id,owner_id,name,mode,armed,leader_account_id,max_leader_qty,allowed_symbols")
        .eq("owner_id", this.connection.owner_id)
        .eq("leader_account_id", localAccountId)
        .eq("armed", true);
      if (groupError) throw groupError;
      if (!groups?.length) return;

      const sourceEntityType = asText(report?.sourceEntityType || eventType).trim().toLowerCase();
      if (sourceEntityType !== "position-delta") this.rememberPrimaryLeaderFill(report);

      const symbol = await this.resolveContractName(contractId);
      const providerFillId = normaliseProviderId(report?.execRefId || report?.id);
      const providerOrderId = normaliseProviderId(report?.orderId);
      const timestamp = asText(report?.timestamp).trim() || nowIso();

      for (const group of groups) {
        const symbolBlocked = !symbolAllowed(symbol, group.allowed_symbols);
        const quantityBlocked = quantity > Math.max(1, num(group.max_leader_qty, 1));
        const status = symbolBlocked || quantityBlocked ? "blocked" : "success";
        const reason = symbolBlocked
          ? `${symbol || `Contract ${contractId}`} is not allowed by this copier group`
          : quantityBlocked
            ? `Leader fill quantity ${quantity} exceeds copier max ${Math.max(1, num(group.max_leader_qty, 1))}`
            : null;
        const message = reason
          ? `LIVE LEADER FILL BLOCKED · ${reason}`
          : `LIVE LEADER FILL DETECTED · ${action || "Trade"} ${quantity} ${symbol || `Contract ${contractId}`}`;
        const dedupeKey = `live:${this.connection.id}:fill:${providerFillId || reportId}:group:${group.id}`;

        const { error: insertError } = await supabase.from("copier_events").insert({
          owner_id: this.connection.owner_id,
          group_id: group.id,
          leader_account_id: localAccountId,
          event_type: "live_leader_fill_detected",
          status,
          dedupe_key: dedupeKey,
          provider_order_id: providerOrderId || null,
          provider_fill_id: providerFillId || reportId,
          symbol: symbol || null,
          action: action || null,
          quantity,
          message,
          payload: {
            stage: "detection_only",
            execution_enabled: false,
            connection_id: this.connection.id,
            provider_account_id: providerAccountId,
            contract_id: contractId || null,
            execution_report_id: reportId,
            event_type: eventType || null,
            provider_timestamp: timestamp,
            last_price: num(report?.lastPx, null),
            avg_price: num(report?.avgPx, null),
            raw_execution_report: report,
          },
          created_at: timestamp,
        });

        if (insertError && insertError.code !== "23505") throw insertError;

        if (!insertError) {
          console.log("copier leader fill detected", {
            connection_id: this.connection.id,
            group_id: group.id,
            leader_account_id: localAccountId,
            provider_account_id: providerAccountId,
            action,
            quantity,
            symbol: symbol || null,
            report_id: reportId,
          });

          if (status === "success") {
            this.enqueueLeaderFillBatch({
              group,
              leaderEvent: { id: reportId, timestamp, source_entity_type: sourceEntityType },
              providerFillId: providerFillId || reportId,
              providerOrderId,
              symbol: symbol || `Contract ${contractId}`,
              action,
              quantity,
              contractId,
            });
          }
        }
      }
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
      ordStatus: "Filled",
      action,
      lastQty: quantity,
      orderId,
      execRefId: fillId,
      timestamp: fillTimestamp || nowIso(),
      lastPx: num(fill?.price, null),
      avgPx: num(fill?.price, null),
      sourceEntityType: "fill",
      rawFill: fill,
    }, eventType || "fill");
  }

  handleCopierFrame(frame) {
    for (const detail of propsEvents(frame)) {
      const entityType = asText(detail?.entityType).trim().toLowerCase();
      const eventType = asText(detail?.eventType).trim();
      if (!["executionreport", "fill"].includes(entityType)) continue;
      const entities = Array.isArray(detail?.entity) ? detail.entity : [detail?.entity];
      for (const entity of entities) {
        if (!entity || typeof entity !== "object") continue;
        const handler = entityType === "fill"
          ? this.detectLeaderFill(entity, eventType)
          : this.detectLeaderExecution(entity, eventType);
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
          ordStatus: "Filled",
          action,
          lastQty: quantity,
          orderId,
          execRefId: fillId,
          timestamp: asText(fill?.timestamp).trim() || nowIso(),
          lastPx: num(fill?.price, null),
          avgPx: num(fill?.price, null),
          sourceEntityType: "fill-list-fallback",
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
        const before = this.copierPositionSnapshot.get(key) || {
          accountId: key.split(":")[0],
          contractId: key.split(":")[1],
          netPos: 0,
        };
        const after = current.get(key) || {
          accountId: before.accountId,
          contractId: before.contractId,
          netPos: 0,
        };

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

    const {
      token,
      userIds,
      accountIds,
      accountSpec,
      environment,
      wsUrl: brokeredWsUrl,
    } = await this.loadCredential();

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
          try {
            ws.close(1011, "connection-failed");
          } catch {}
          reject(error);
        }
      };

      const sendSocketHeartbeat = () => {
        if (ws.readyState !== WebSocket.OPEN) return;

        try {
          ws.send("[]");
          this.lastClientHeartbeatAt = Date.now();
        } catch (error) {
          console.error(
            "Tradovate heartbeat send failed",
            this.connection.id,
            error instanceof Error ? error.message : String(error)
          );
        }
      };

      ws.on("open", () => {
        sendRequest(ws, "authorize", 0, token);
        this.socketHeartbeatTimer = setInterval(
          sendSocketHeartbeat,
          TRADOVATE_HEARTBEAT_MS
        );
      });

      ws.on("message", async raw => {
        const rawText = asText(raw);

        if (rawText === "h") {
          if (Date.now() - this.lastClientHeartbeatAt >= 1000) {
            sendSocketHeartbeat();
          }
          return;
        }

        const frames = unpackFrame(rawText);
        if (!frames.length) return;

        for (const frame of frames) {
          if (Number(frame.i) === 0) {
            if (Number(frame.s) !== 200) {
              return finishReject(
                new Error(
                  `websocket authorize: ${frameError(
                    frame,
                    "Tradovate WebSocket authorization failed"
                  )}`
                )
              );
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
              : {
                  users: userIds,
                  splitResponses: true,
                };

            sendRequest(ws, "user/syncrequest", 1, syncBody);
            continue;
          }

          if (Number(frame.i) === 1 && Number(frame.s) >= 400) {
            return finishReject(
              new Error(
                `user/syncrequest: ${frameError(
                  frame,
                  "Tradovate user synchronization failed"
                )}`
              )
            );
          }

          if (
            Number(frame.i) === 1 &&
            Number(frame.s) === 200 &&
            !this.subscribed
          ) {
            this.subscribed = true;
            this.backoffMs = 1000;

            await supabase
              .from("provider_sync_checkpoints")
              .upsert(
                {
                  connection_id: this.connection.id,
                  checkpoint_key: "copier_worker_version",
                  checkpoint_value: VERSION,
                  metadata: {
                    worker_instance: INSTANCE_ID,
                    detection_mode:
                      "primary-fill-burst-aggregate-with-delayed-position-fallback",
                    follower_execution_enabled: true,
                    execution_gate:
                      "copier_groups.mode=live + armed + follower.enabled",
                    provider_order_mode: "market-isAutomated",
                    follower_dispatch_mode: "parallel",
                    partial_fill_mode: "burst-aggregate",
                    partial_fill_quiet_ms: COPIER_FILL_BURST_QUIET_MS,
                    partial_fill_max_ms: COPIER_FILL_BURST_MAX_MS,
                  },
                  updated_at: nowIso(),
                },
                {
                  onConflict: "connection_id,checkpoint_key",
                }
              );

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

            if (this.sessionRefreshTimer) {
              clearTimeout(this.sessionRefreshTimer);
            }

            this.sessionRefreshTimer = setTimeout(() => {
              if (this.ws?.readyState === WebSocket.OPEN) {
                console.log(
                  "scheduled Tradovate token refresh reconnect",
                  this.connection.id
                );

                try {
                  this.ws.close(1000, "scheduled-token-refresh");
                } catch {}
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
            this.scheduleCopierFillScan("tradovate-user-event", 120);

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
        this.lastCloseInfo =
          `Tradovate WebSocket closed (${code}) ${asText(reason)}`.trim();

        this.clearHeartbeat();
        this.authorized = false;
        this.subscribed = false;
        this.ws = null;

        if (!this.stopped && !stopping) {
          finishReject(
            new Error(
              `Tradovate WebSocket closed (${code}) ${asText(reason)}`.trim()
            )
          );
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

    while (
      !this.stopped &&
      !stopping &&
      this.ws?.readyState === WebSocket.OPEN
    ) {
      await sleep(1000);
    }

    if (!this.stopped && !stopping) {
      throw new Error(
        this.lastCloseInfo || "Tradovate WebSocket disconnected"
      );
    }
  }

  clearHeartbeat() {
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
    if (this.socketHeartbeatTimer) clearInterval(this.socketHeartbeatTimer);
    if (this.sessionRefreshTimer) clearTimeout(this.sessionRefreshTimer);
    if (this.copierFillScanTimer) clearTimeout(this.copierFillScanTimer);
    if (this.copierPositionScanInterval) {
      clearInterval(this.copierPositionScanInterval);
    }

    for (const timer of this.pendingPositionFallbacks.values()) {
      clearTimeout(timer);
    }

    for (const batch of this.pendingLeaderFillBatches.values()) {
      if (batch?.timer) clearTimeout(batch.timer);
    }

    this.pendingLeaderFillBatches.clear();
    this.pendingPositionFallbacks.clear();
    this.recentPrimaryLeaderFills.clear();

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

    if (this.pulseTimer) {
      clearTimeout(this.pulseTimer);
    }

    const sinceLast = Date.now() - this.lastPulseAt;
    const wait = Math.max(
      delay,
      PULSE_MIN_INTERVAL_MS - sinceLast,
      0
    );

    this.pulseTimer = setTimeout(
      () => this.runPulse(reason),
      wait
    );
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
      const response = await fetch(
        `${SUPABASE_URL}/functions/v1/tradovate-live-pulse`,
        {
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
        }
      );

      const payload = await response.json().catch(() => ({}));

      if (!response.ok || payload?.error) {
        throw new Error(
          payload?.error || `Live pulse failed (${response.status})`
        );
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
      const message =
        error instanceof Error ? error.message : String(error);

      console.error(
        "live pulse failed",
        this.connection.id,
        message
      );

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
        this.schedulePulse(
          "queued-provider-event",
          350
        );
      }
    }
  }

  async stop(reason = "stopped") {
    this.stopped = true;

    if (this.pulseTimer) {
      clearTimeout(this.pulseTimer);
    }

    this.clearHeartbeat();

    try {
      this.ws?.close(1000, reason);
    } catch {}

    this.ws = null;

    await writeStatus(this.connection, {
      state: reason === "not-eligible"
        ? "manual"
        : "offline",
      last_heartbeat_at: nowIso(),
      last_error:
        reason === "not-eligible"
          ? "Live sync is available on paid plans"
          : null,
    });
  }
}

async function reconcileTargets() {
  const targets = await loadTargets();
  const targetById = new Map(
    targets.map(row => [row.id, row])
  );

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

  const liveCount = [...sessions.values()].filter(
    session =>
      session.subscribed &&
      session.ws?.readyState === WebSocket.OPEN
  ).length;

  const connectingCount = Math.max(
    sessions.size - liveCount,
    0
  );

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
    [...sessions.values()].map(session =>
      session.stop("worker-shutdown")
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
  error => console.error(
    "unhandled rejection",
    error
  )
);

process.on(
  "uncaughtException",
  error => console.error(
    "uncaught exception",
    error
  )
);

console.log(
  `FRA Prop HQ Tradovate live worker v${VERSION} starting as ${INSTANCE_ID}`
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
