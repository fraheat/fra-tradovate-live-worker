import crypto from "node:crypto";
import os from "node:os";
import process from "node:process";
import WebSocket from "ws";
import { createClient } from "@supabase/supabase-js";

const required = ["SUPABASE_URL", "FRA_LIVE_SYNC_WORKER_SECRET"];
for (const name of required) {
  if (!process.env[name]) throw new Error(`${name} is required`);
}

const SERVICE_ROLE_KEY =
  process.env.SUPABASE_SECRET_KEY ||
  process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!SERVICE_ROLE_KEY) {
  throw new Error(
    "SUPABASE_SECRET_KEY or SUPABASE_SERVICE_ROLE_KEY is required"
  );
}

const SUPABASE_URL = process.env.SUPABASE_URL.replace(/\/$/, "");
const WORKER_SECRET = process.env.FRA_LIVE_SYNC_WORKER_SECRET;

const TARGET_REFRESH_MS =
  Number(process.env.TARGET_REFRESH_MS || 30000);

const COPIER_CONFIG_REFRESH_MS = Math.max(
  500,
  Number(process.env.COPIER_CONFIG_REFRESH_MS || 1000)
);

const COPIER_CONFIG_MAX_STALE_MS = Math.max(
  2500,
  Number(process.env.COPIER_CONFIG_MAX_STALE_MS || 5000)
);

const PULSE_MIN_INTERVAL_MS =
  Number(process.env.PULSE_MIN_INTERVAL_MS || 2500);

const HEARTBEAT_MS =
  Number(process.env.HEARTBEAT_MS || 15000);

const TRADOVATE_HEARTBEAT_MS = Math.max(
  1000,
  Math.min(
    Number(process.env.TRADOVATE_HEARTBEAT_MS || 2000),
    2400
  )
);

const SESSION_RECONNECT_MS =
  Number(process.env.SESSION_RECONNECT_MS || 55 * 60 * 1000);

const COPIER_EVENT_MAX_AGE_MS =
  Number(process.env.COPIER_EVENT_MAX_AGE_MS || 15 * 60 * 1000);

const COPIER_POSITION_SCAN_MS = Math.max(
  500,
  Number(process.env.COPIER_POSITION_SCAN_MS || 1000)
);

const COPIER_POSITION_FALLBACK_GRACE_MS = Math.max(
  500,
  Number(process.env.COPIER_POSITION_FALLBACK_GRACE_MS || 1500)
);

const COPIER_PRIMARY_MATCH_WINDOW_MS = Math.max(
  750,
  Number(process.env.COPIER_PRIMARY_MATCH_WINDOW_MS || 2500)
);

const COPIER_COMMAND_POLL_MS = Math.max(
  100,
  Number(process.env.COPIER_COMMAND_POLL_MS || 200)
);

const COPIER_FLATTEN_VERIFY_MS = Math.max(
  250,
  Number(process.env.COPIER_FLATTEN_VERIFY_MS || 600)
);

const COPIER_FLATTEN_VERIFY_ATTEMPTS = Math.max(
  2,
  Math.min(
    Number(process.env.COPIER_FLATTEN_VERIFY_ATTEMPTS || 6),
    12
  )
);

const LEADER_FLAT_RECONCILE_DELAY_MS = Math.max(
  750,
  Number(process.env.LEADER_FLAT_RECONCILE_DELAY_MS || 1500)
);

const VERSION = "7.7.20";

const INSTANCE_ID =
  `${os.hostname()}-${process.pid}-${crypto
    .randomBytes(3)
    .toString("hex")}`;

const supabase = createClient(
  SUPABASE_URL,
  SERVICE_ROLE_KEY,
  {
    auth: {
      persistSession: false,
      autoRefreshToken: false,
    },
    realtime: {
      transport: WebSocket,
    },
  }
);

const sessions = new Map();

let stopping = false;

const sleep = ms =>
  new Promise(resolve => setTimeout(resolve, ms));

const nowIso = () =>
  new Date().toISOString();

const asText = value =>
  String(value ?? "");

const normaliseProviderId = value =>
  asText(value).trim();

const num = (value, fallback = 0) => {
  const parsed = Number(value);
  return Number.isFinite(parsed)
    ? parsed
    : fallback;
};

function symbolAllowed(
  symbol,
  allowedSymbols
) {
  const normalized =
    asText(symbol)
      .trim()
      .toUpperCase();

  const allowed =
    Array.isArray(allowedSymbols)
      ? allowedSymbols
          .map(value =>
            asText(value)
              .trim()
              .toUpperCase()
          )
          .filter(Boolean)
      : [];

  if (!allowed.length) return true;

  if (!normalized) return false;

  return allowed.some(
    value =>
      normalized === value ||
      normalized.startsWith(value)
  );
}

function propsEvents(frame) {
  if (
    asText(frame?.e).toLowerCase() !==
    "props"
  ) {
    return [];
  }

  const details =
    Array.isArray(frame?.d)
      ? frame.d
      : [frame?.d];

  return details.filter(
    item =>
      item &&
      typeof item === "object"
  );
}

async function tradovateGet(
  environment,
  path,
  token
) {
  const response = await fetch(
    `https://${environment}.tradovateapi.com/v1${path}`,
    {
      headers: {
        Accept: "application/json",
        Authorization: `Bearer ${token}`,
      },
    }
  );

  const payload =
    await response
      .json()
      .catch(() => ({}));

  if (
    !response.ok ||
    payload?.errorText
  ) {
    throw new Error(
      payload?.errorText ||
      payload?.error ||
      `${path} failed (${response.status})`
    );
  }

  return payload;
}

async function tradovatePost(
  environment,
  path,
  token,
  body
) {
  const response = await fetch(
    `https://${environment}.tradovateapi.com/v1${path}`,
    {
      method: "POST",
      headers: {
        Accept: "application/json",
        "Content-Type":
          "application/json",
        Authorization:
          `Bearer ${token}`,
      },
      body: JSON.stringify(
        body ?? {}
      ),
    }
  );

  const payload =
    await response
      .json()
      .catch(() => ({}));

  const failureReason =
    asText(payload?.failureReason)
      .trim();

  const failureText =
    asText(payload?.failureText)
      .trim();

  const errorText =
    asText(
      payload?.errorText ||
      payload?.error
    ).trim();

  const explicitFailure =
    payload?.ok === false ||
    failureText ||
    errorText ||
    (
      failureReason &&
      failureReason.toLowerCase() !==
        "success"
    );

  if (
    !response.ok ||
    explicitFailure
  ) {
    throw new Error(
      failureText ||
      errorText ||
      failureReason ||
      `${path} failed (${response.status})`
    );
  }

  return payload;
}

async function requestWorkerSession(
  connectionId
) {
  const response = await fetch(
    `${SUPABASE_URL}/functions/v1/tradovate-session`,
    {
      method: "POST",
      headers: {
        "Content-Type":
          "application/json",
        Authorization:
          `Bearer ${SERVICE_ROLE_KEY}`,
        apikey:
          SERVICE_ROLE_KEY,
        "x-fra-worker-secret":
          WORKER_SECRET,
      },
      body: JSON.stringify({
        connection_id:
          connectionId,
      }),
    }
  );

  const payload =
    await response
      .json()
      .catch(() => ({}));

  if (
    !response.ok ||
    payload?.error
  ) {
    throw new Error(
      payload?.error ||
      `Unable to obtain Tradovate live session (${response.status})`
    );
  }

  const token =
    asText(payload.access_token)
      .trim();

  const userIds =
    Array.isArray(payload.user_ids)
      ? payload.user_ids
          .map(Number)
          .filter(Number.isFinite)
      : [];

  if (!token) {
    throw new Error(
      "Supabase did not return a Tradovate access token"
    );
  }

  if (!userIds.length) {
    throw new Error(
      "Tradovate returned no user IDs for live sync"
    );
  }

  const environment =
    payload.environment === "live"
      ? "live"
      : "demo";

  let accountIds = [];

  try {
    const brokerAccounts =
      await tradovateGet(
        environment,
        "/account/list",
        token
      );

    accountIds = [
      ...new Set(
        (
          Array.isArray(
            brokerAccounts
          )
            ? brokerAccounts
            : []
        )
          .map(row =>
            Number(row?.id)
          )
          .filter(
            Number.isFinite
          )
      ),
    ];
  } catch (error) {
    console.error(
      "unable to preload Tradovate account ids",
      connectionId,
      error instanceof Error
        ? error.message
        : String(error)
    );
  }

  if (!accountIds.length) {
    const {
      data: links,
      error: linksError,
    } = await supabase
      .from(
        "provider_account_links"
      )
      .select(
        "provider_account_id"
      )
      .eq(
        "connection_id",
        connectionId
      )
      .not(
        "provider_account_id",
        "is",
        null
      );

    if (linksError) {
      throw linksError;
    }

    accountIds = [
      ...new Set(
        (links || [])
          .map(row =>
            Number(
              row.provider_account_id
            )
          )
          .filter(
            Number.isFinite
          )
      ),
    ];
  }

  let accountSpec = "";

  try {
    const user =
      await tradovateGet(
        environment,
        `/user/item?id=${encodeURIComponent(
          userIds[0]
        )}`,
        token
      );

    accountSpec =
      asText(user?.name)
        .trim();
  } catch (error) {
    console.error(
      "unable to resolve Tradovate accountSpec",
      connectionId,
      error instanceof Error
        ? error.message
        : String(error)
    );
  }

  return {
    token,
    userIds,
    accountIds,
    accountSpec,
    environment,
    wsUrl:
      asText(
        payload.websocket_url
      ).trim(),
  };
}

function isPaidTarget(
  ownerId,
  profileById,
  subscriptionByOwner
) {
  const profile =
    profileById.get(ownerId) || {};

  const subscription =
    subscriptionByOwner.get(
      ownerId
    ) || {};

  const role =
    asText(profile.role)
      .toLowerCase();

  const plan =
    asText(
      profile.plan ||
      subscription.plan
    ).toLowerCase();

  const status =
    asText(
      subscription.status
    ).toLowerCase();

  if (
    role === "admin" ||
    [
      "founder",
      "business",
      "desk",
    ].includes(plan)
  ) {
    return true;
  }

  return (
    [
      "entry",
      "plus",
      "pro",
    ].includes(plan) &&
    [
      "active",
      "trialing",
      "past_due",
    ].includes(status)
  );
}

async function loadTargets() {
  const {
    data: connections,
    error,
  } = await supabase
    .from(
      "provider_connections"
    )
    .select(
      "id,owner_id,provider,environment,status,display_name,metadata,live_sync_enabled"
    )
    .eq(
      "provider",
      "tradovate"
    )
    .in(
      "status",
      [
        "connected",
        "syncing",
      ]
    )
    .eq(
      "live_sync_enabled",
      true
    );

  if (error) throw error;

  const ownerIds = [
    ...new Set(
      (connections || [])
        .map(row =>
          row.owner_id
        )
        .filter(Boolean)
    ),
  ];

  if (!ownerIds.length) {
    return [];
  }

  const [
    {
      data: profiles,
      error: profileError,
    },
    {
      data: subscriptions,
      error: subscriptionError,
    },
  ] = await Promise.all([
    supabase
      .from("profiles")
      .select(
        "id,role,plan"
      )
      .in(
        "id",
        ownerIds
      ),

    supabase
      .from(
        "subscriptions"
      )
      .select(
        "owner_id,plan,status,current_period_end"
      )
      .in(
        "owner_id",
        ownerIds
      ),
  ]);

  if (profileError) {
    throw profileError;
  }

  if (subscriptionError) {
    throw subscriptionError;
  }

  const profileById =
    new Map(
      (profiles || [])
        .map(row => [
          row.id,
          row,
        ])
    );

  const subscriptionByOwner =
    new Map(
      (subscriptions || [])
        .map(row => [
          row.owner_id,
          row,
        ])
    );

  return (
    connections || []
  ).filter(row =>
    isPaidTarget(
      row.owner_id,
      profileById,
      subscriptionByOwner
    )
  );
}

async function writeStatus(
  connection,
  patch
) {
  const environment =
    asText(
      connection?.metadata
        ?.technical_environment ||
      connection.environment
    ) === "live"
      ? "live"
      : "demo";

  const { error } =
    await supabase
      .from(
        "tradovate_live_status"
      )
      .upsert(
        {
          connection_id:
            connection.id,
          owner_id:
            connection.owner_id,
          provider_environment:
            environment,
          worker_instance:
            INSTANCE_ID,
          transport:
            "websocket",
          ...patch,
          updated_at:
            nowIso(),
        },
        {
          onConflict:
            "connection_id",
        }
      );

  if (error) {
    console.error(
      "status update failed",
      connection.id,
      error.message
    );
  }

  await supabase
    .from(
      "provider_connections"
    )
    .update({
      live_sync_state:
        patch.state ||
        "live",
      live_sync_last_event_at:
        patch.last_event_at,
      live_sync_last_heartbeat_at:
        patch.last_heartbeat_at ||
        nowIso(),
    })
    .eq(
      "id",
      connection.id
    );
}

function sendRequest(
  ws,
  endpoint,
  id,
  body
) {
  const payload =
    body === undefined ||
    body === null
      ? `${endpoint}\n${id}\n\n`
      : `${endpoint}\n${id}\n\n${
          typeof body === "string"
            ? body
            : JSON.stringify(body)
        }`;

  ws.send(payload);
}

function frameError(
  frame,
  fallback
) {
  const detail = frame?.d;

  if (
    typeof detail === "string" &&
    detail.trim()
  ) {
    return detail.trim();
  }

  if (
    detail &&
    typeof detail === "object"
  ) {
    return asText(
      detail.errorText ||
      detail.error ||
      detail.message ||
      fallback
    );
  }

  return fallback;
}

function unpackFrame(
  rawValue
) {
  const raw =
    asText(rawValue);

  if (
    !raw ||
    raw === "o" ||
    raw === "h"
  ) {
    return [];
  }

  if (
    !raw.startsWith("a")
  ) {
    return [];
  }

  let outer;

  try {
    outer =
      JSON.parse(
        raw.slice(1)
      );
  } catch {
    return [];
  }

  const items = [];

  for (
    const value of
    Array.isArray(outer)
      ? outer
      : [outer]
  ) {
    if (
      typeof value ===
      "string"
    ) {
      try {
        items.push(
          JSON.parse(value)
        );
      } catch {
        items.push({
          raw: value,
        });
      }
    } else if (
      value &&
      typeof value ===
        "object"
    ) {
      items.push(value);
    }
  }

  return items;
}

/*
  ============================================================
  COPIER HOT CONFIG CACHE
  ============================================================

  No Supabase round-trip is allowed inside the normal follower
  order dispatch critical path.

  Config is refreshed in the background and held in memory.
*/

let copierCache = {
  loadedAt: 0,
  routesByLeader:
    new Map(),
  groupsById:
    new Map(),
};

let copierCacheRefreshInFlight =
  false;

const suppressedGroups =
  new Map();

const leaderCacheKey = (
  ownerId,
  connectionId,
  providerAccountId
) =>
  `${ownerId}:${connectionId}:${normaliseProviderId(
    providerAccountId
  )}`;

function markGroupSuppressed(
  groupId
) {
  const existing =
    suppressedGroups.get(
      groupId
    );

  suppressedGroups.set(
    groupId,
    existing || {
      trippedAt:
        Date.now(),
      sawDisarmed:
        false,
    }
  );
}

function isGroupSuppressed(
  groupId
) {
  return suppressedGroups.has(
    groupId
  );
}

async function refreshCopierCache() {
  if (
    copierCacheRefreshInFlight
  ) {
    return;
  }

  copierCacheRefreshInFlight =
    true;

  try {
    const [
      {
        data: groups,
        error: groupError,
      },
      {
        data: followers,
        error: followerError,
      },
      {
        data: accounts,
        error: accountError,
      },
    ] = await Promise.all([
      supabase
        .from(
          "copier_groups"
        )
        .select(
          "id,owner_id,name,mode,armed,desired_armed,leader_account_id,max_leader_qty,allowed_symbols,updated_at"
        )
        .eq(
          "mode",
          "live"
        ),

      supabase
        .from(
          "copier_followers"
        )
        .select(
          "id,group_id,owner_id,account_id,enabled,multiplier,max_qty,max_daily_loss,allowed_symbols"
        ),

      supabase
        .from("accounts")
        .select(
          "id,owner_id,name,external_id,source_connection_id,status,is_archived,daily_pnl,updated_at"
        ),
    ]);

    if (groupError) {
      throw groupError;
    }

    if (followerError) {
      throw followerError;
    }

    if (accountError) {
      throw accountError;
    }

    const accountById =
      new Map(
        (accounts || [])
          .map(row => [
            String(row.id),
            row,
          ])
      );

    const followersByGroup =
      new Map();

    for (
      const follower of
      followers || []
    ) {
      const account =
        accountById.get(
          String(
            follower.account_id
          )
        );

      if (!account) {
        continue;
      }

      if (
        account.owner_id !==
        follower.owner_id
      ) {
        continue;
      }

      const list =
        followersByGroup.get(
          String(
            follower.group_id
          )
        ) || [];

      list.push({
        follower,
        account,
      });

      followersByGroup.set(
        String(
          follower.group_id
        ),
        list
      );
    }

    const routesByLeader =
      new Map();

    const groupsById =
      new Map();

    for (
      const group of
      groups || []
    ) {
      const leader =
        accountById.get(
          String(
            group.leader_account_id
          )
        );

      if (
        !leader
          ?.source_connection_id ||
        !leader?.external_id
      ) {
        continue;
      }

      const suppression =
        suppressedGroups.get(
          String(group.id)
        );

      if (suppression) {
        if (
          !group.armed ||
          !group.desired_armed
        ) {
          suppression.sawDisarmed =
            true;
        }

        if (
          group.armed &&
          group.desired_armed &&
          suppression.sawDisarmed
        ) {
          suppressedGroups.delete(
            String(group.id)
          );
        }
      }

      const plan = {
        group,
        leader,

        followers:
          (
            followersByGroup.get(
              String(group.id)
            ) || []
          ).filter(
            ({
              follower,
              account,
            }) =>
              follower.enabled ===
                true &&
              account.is_archived !==
                true &&
              asText(
                account.status
              ).toLowerCase() ===
                "active" &&
              account
                .source_connection_id &&
              account.external_id
          ),
      };

      groupsById.set(
        String(group.id),
        plan
      );

      const key =
        leaderCacheKey(
          group.owner_id,
          leader
            .source_connection_id,
          leader.external_id
        );

      const list =
        routesByLeader.get(
          key
        ) || [];

      list.push(plan);

      routesByLeader.set(
        key,
        list
      );
    }

    copierCache = {
      loadedAt:
        Date.now(),
      routesByLeader,
      groupsById,
    };
  } catch (error) {
    console.error(
      "copier config refresh failed",
      error instanceof Error
        ? error.message
        : String(error)
    );
  } finally {
    copierCacheRefreshInFlight =
      false;
  }
}

function copierCacheIsFresh() {
  return (
    copierCache.loadedAt > 0 &&
    Date.now() -
      copierCache.loadedAt <=
      COPIER_CONFIG_MAX_STALE_MS
  );
}

function plansForLeader(
  ownerId,
  connectionId,
  providerAccountId
) {
  return (
    copierCache
      .routesByLeader
      .get(
        leaderCacheKey(
          ownerId,
          connectionId,
          providerAccountId
        )
      ) || []
  );
}

function activePlansForLeader(
  ownerId,
  connectionId,
  providerAccountId
) {
  if (
    !copierCacheIsFresh()
  ) {
    return [];
  }

  return plansForLeader(
    ownerId,
    connectionId,
    providerAccountId
  ).filter(
    plan =>
      plan.group?.armed ===
        true &&
      plan.group
        ?.desired_armed ===
        true &&
      !isGroupSuppressed(
        String(
          plan.group.id
        )
      )
  );
}

function queueAudit(
  row,
  label = "copier audit"
) {
  try {
    supabase
      .from(
        "copier_events"
      )
      .insert(row)
      .then(({ error }) => {
        if (
          error &&
          error.code !== "23505"
        ) {
          console.error(
            label,
            error.message
          );
        }
      })
      .catch(error =>
        console.error(
          label,
          error instanceof Error
            ? error.message
            : String(error)
        )
      );
  } catch (error) {
    console.error(
      label,
      error instanceof Error
        ? error.message
        : String(error)
    );
  }
}

async function tripCopierSafety(
  group,
  ownerId,
  reason,
  details = {}
) {
  const groupId =
    String(
      group?.id || ""
    );

  if (!groupId) {
    return;
  }

  /*
    This happens BEFORE the database call.
    So even if Supabase is slow, this worker immediately stops
    sending new follower orders for the group.
  */
  markGroupSuppressed(
    groupId
  );

  queueAudit({
    owner_id:
      ownerId,

    group_id:
      groupId,

    leader_account_id:
      group?.leader_account_id ||
      null,

    event_type:
      "copier_safety_trip",

    status:
      "error",

    dedupe_key:
      `safety-trip:${groupId}:${Date.now()}:${reason}`,

    message:
      `COPIER SAFETY TRIP · ${reason} · copier disarmed locally and in database`,

    payload: {
      reason,
      worker_version:
        VERSION,
      worker_instance:
        INSTANCE_ID,
      ...details,
    },
  });

  try {
    const { error } =
      await supabase
        .from(
          "copier_groups"
        )
        .update({
          armed: false,
          desired_armed:
            false,
          updated_at:
            nowIso(),
        })
        .eq(
          "id",
          groupId
        )
        .eq(
          "owner_id",
          ownerId
        );

    if (error) {
      throw error;
    }
  } catch (error) {
    console.error(
      "copier safety database disarm failed",
      groupId,
      error instanceof Error
        ? error.message
        : String(error)
    );
  }
}

async function disarmStaleCachePlans(
  ownerId,
  connectionId,
  providerAccountId
) {
  const plans =
    plansForLeader(
      ownerId,
      connectionId,
      providerAccountId
    );

  await Promise.allSettled(
    plans.map(plan =>
      tripCopierSafety(
        plan.group,
        ownerId,
        "copier_config_cache_stale",
        {
          cache_age_ms:
            copierCache.loadedAt
              ? Date.now() -
                copierCache.loadedAt
              : null,
        }
      )
    )
  );
}

function copierClientOrderId(
  groupId,
  followerAccountId,
  dispatchKey
) {
  const digest =
    crypto
      .createHash("sha256")
      .update(
        `${groupId}:${followerAccountId}:${dispatchKey}`
      )
      .digest("hex")
      .slice(0, 28);

  return `FRA-${digest}`;
}

function latencyPayload(
  telemetry = {},
  dispatchStartedAtMs =
    Date.now(),
  responseAtMs = null
) {
  const providerTimestampMs =
    num(
      telemetry
        .providerTimestampMs,
      0
    );

  const workerReceivedAtMs =
    num(
      telemetry
        .workerReceivedAtMs,
      0
    );

  const out = {
    fast_path: true,
    worker_version:
      VERSION,

    provider_timestamp:
      providerTimestampMs
        ? new Date(
            providerTimestampMs
          ).toISOString()
        : null,

    worker_received_at:
      workerReceivedAtMs
        ? new Date(
            workerReceivedAtMs
          ).toISOString()
        : null,

    dispatch_started_at:
      new Date(
        dispatchStartedAtMs
      ).toISOString(),

    provider_to_worker_ms:
      providerTimestampMs &&
      workerReceivedAtMs
        ? Math.max(
            0,
            workerReceivedAtMs -
              providerTimestampMs
          )
        : null,

    worker_to_dispatch_ms:
      workerReceivedAtMs
        ? Math.max(
            0,
            dispatchStartedAtMs -
              workerReceivedAtMs
          )
        : null,

    provider_to_dispatch_ms:
      providerTimestampMs
        ? Math.max(
            0,
            dispatchStartedAtMs -
              providerTimestampMs
          )
        : null,
  };

  if (responseAtMs) {
    out.provider_response_at =
      new Date(
        responseAtMs
      ).toISOString();

    out.order_api_ms =
      Math.max(
        0,
        responseAtMs -
          dispatchStartedAtMs
      );

    out.provider_to_response_ms =
      providerTimestampMs
        ? Math.max(
            0,
            responseAtMs -
              providerTimestampMs
          )
        : null;
  }

  return out;
}

class LiveSession {
  constructor(connection) {
    this.connection =
      connection;

    this.ws = null;

    this.stopped =
      false;

    this.authorized =
      false;

    this.subscribed =
      false;

    this.reconnectCount =
      0;

    this.eventCount =
      0;

    this.pulseCount =
      0;

    this.lastPulseAt =
      0;

    this.pulseTimer =
      null;

    this.pulseInFlight =
      false;

    this.pulseQueued =
      false;

    this.heartbeatTimer =
      null;

    this.socketHeartbeatTimer =
      null;

    this.sessionRefreshTimer =
      null;

    this.lastClientHeartbeatAt =
      0;

    this.lastCloseInfo =
      "";

    this.backoffMs =
      1000;

    this.accessToken =
      "";

    this.accountSpec =
      "";

    this.environment =
      "demo";

    this.contractNameCache =
      new Map();

    this.contractNamePromiseCache =
      new Map();

    this.followerOrderInFlight =
      new Set();

    /*
      Ensures reports for the SAME leader order are processed
      sequentially even if WebSocket reports arrive very fast.
    */
    this.orderChains =
      new Map();

    /*
      Tracks cumulative filled quantity per leader order.

      Example:
      leader stop fills:
        cumQty 1
        cumQty 3
        cumQty 4
        cumQty 5
        cumQty 10

      follower deltas become:
        1
        2
        1
        1
        5
    */
    this.leaderOrderStates =
      new Map();

    this.seenExecutionIds =
      new Map();

    this.recentPrimaryLeaderFills =
      new Map();

    this.pendingPositionFallbacks =
      new Map();

    /*
      Tracks cumulative follower quantity PER leader order and
      follower. This also makes fractional multipliers safe across
      partial fills.
    */
    this.followerOrderProgress =
      new Map();

    this.copierPositionScanInterval =
      null;

    this.copierPositionScanInFlight =
      false;

    this.copierPositionSnapshot =
      new Map();

    this.copierPositionInitialized =
      false;

    this.reconcileLocks =
      new Set();
  }

  updateConnection(
    connection
  ) {
    this.connection =
      connection;
  }

  async start() {
    while (
      !this.stopped &&
      !stopping
    ) {
      try {
        await this.connectOnce();
      } catch (error) {
        if (
          this.stopped ||
          stopping
        ) {
          break;
        }

        const message =
          error instanceof Error
            ? error.message
            : String(error);

        console.error(
          "live connection failed",
          {
            connection_id:
              this.connection.id,

            display_name:
              this.connection
                .display_name ||
              "Tradovate login",

            environment:
              this.connection
                .environment ||
              "demo",

            error:
              message,
          }
        );

        this.reconnectCount +=
          1;

        await writeStatus(
          this.connection,
          {
            state:
              "reconnecting",

            reconnect_count:
              this
                .reconnectCount,

            last_error:
              message,

            last_heartbeat_at:
              nowIso(),
          }
        );

        await sleep(
          this.backoffMs +
            Math.floor(
              Math.random() *
                500
            )
        );

        this.backoffMs =
          Math.min(
            this.backoffMs *
              2,
            30000
          );
      }
    }
  }

  async loadCredential() {
    return requestWorkerSession(
      this.connection.id
    );
  }

  async resolveContractName(
    contractId
  ) {
    const key =
      normaliseProviderId(
        contractId
      );

    if (!key) {
      return "";
    }

    if (
      this.contractNameCache.has(
        key
      )
    ) {
      return this
        .contractNameCache
        .get(key);
    }

    if (
      this
        .contractNamePromiseCache
        .has(key)
    ) {
      return this
        .contractNamePromiseCache
        .get(key);
    }

    const lookup =
      (async () => {
        try {
          const contract =
            await tradovateGet(
              this.environment,
              `/contract/item?id=${encodeURIComponent(
                key
              )}`,
              this.accessToken
            );

          const name =
            asText(
              contract?.name
            ).trim();

          if (name) {
            this
              .contractNameCache
              .set(
                key,
                name
              );
          }

          return name;
        } catch (error) {
          console.error(
            "contract lookup failed",
            this.connection.id,
            key,
            error instanceof Error
              ? error.message
              : String(error)
          );

          return "";
        } finally {
          this
            .contractNamePromiseCache
            .delete(key);
        }
      })();

    this
      .contractNamePromiseCache
      .set(
        key,
        lookup
      );

    return lookup;
  }

  primeContractName(
    contractId
  ) {
    const key =
      normaliseProviderId(
        contractId
      );

    if (
      !key ||
      this.contractNameCache.has(
        key
      ) ||
      this
        .contractNamePromiseCache
        .has(key)
    ) {
      return;
    }

    this
      .resolveContractName(key)
      .catch(() => {});
  }

  async warmContractCache() {
    try {
      const [
        positionsPayload,
        ordersPayload,
      ] = await Promise.all([
        tradovateGet(
          this.environment,
          "/position/list",
          this.accessToken
        ),

        tradovateGet(
          this.environment,
          "/order/list",
          this.accessToken
        ),
      ]);

      const contractIds =
        new Set();

      for (
        const row of
        Array.isArray(
          positionsPayload
        )
          ? positionsPayload
          : []
      ) {
        if (
          row?.contractId
        ) {
          contractIds.add(
            normaliseProviderId(
              row.contractId
            )
          );
        }
      }

      for (
        const row of
        Array.isArray(
          ordersPayload
        )
          ? ordersPayload
          : []
      ) {
        if (
          row?.contractId
        ) {
          contractIds.add(
            normaliseProviderId(
              row.contractId
            )
          );
        }
      }

      await Promise.allSettled(
        [...contractIds]
          .slice(0, 30)
          .map(id =>
            this
              .resolveContractName(
                id
              )
          )
      );
    } catch (error) {
      console.error(
        "contract cache warm failed",
        this.connection.id,
        error instanceof Error
          ? error.message
          : String(error)
      );
    }
  }

  primaryFillBucketKey(
    report
  ) {
    const accountId =
      normaliseProviderId(
        report?.accountId
      );

    const contractId =
      normaliseProviderId(
        report?.contractId
      );

    const action =
      asText(report?.action)
        .trim()
        .toLowerCase();

    if (
      !accountId ||
      !contractId ||
      !action
    ) {
      return "";
    }

    return `${accountId}:${contractId}:${action}`;
  }

  rememberPrimaryLeaderFill(
    report
  ) {
    const key =
      this
        .primaryFillBucketKey(
          report
        );

    if (!key) return;

    const quantity =
      Math.abs(
        num(report?.lastQty)
      );

    if (
      quantity < 1
    ) {
      return;
    }

    const providerTimestampMs =
      new Date(
        asText(
          report?.timestamp
        )
      ).getTime();

    const observedAt =
      Date.now();

    const entries =
      this
        .recentPrimaryLeaderFills
        .get(key) || [];

    entries.push({
      quantity,

      providerTimestampMs:
        Number.isFinite(
          providerTimestampMs
        )
          ? providerTimestampMs
          : observedAt,

      observedAt,

      providerFillId:
        normaliseProviderId(
          report?.execRefId ||
          report?.id
        ),
    });

    const cutoff =
      observedAt -
      Math.max(
        COPIER_PRIMARY_MATCH_WINDOW_MS *
          4,
        10000
      );

    this
      .recentPrimaryLeaderFills
      .set(
        key,
        entries.filter(
          entry =>
            entry.observedAt >=
            cutoff
        )
      );
  }

  hasPrimaryCoverageForPositionDelta(
    report
  ) {
    const key =
      this
        .primaryFillBucketKey(
          report
        );

    if (!key) {
      return false;
    }

    const requiredQuantity =
      Math.abs(
        num(report?.lastQty)
      );

    if (
      requiredQuantity < 1
    ) {
      return false;
    }

    const targetTimestampMs =
      new Date(
        asText(
          report?.rawPosition
            ?.timestamp ||
          report?.timestamp
        )
      ).getTime();

    const now =
      Date.now();

    const entries =
      (
        this
          .recentPrimaryLeaderFills
          .get(key) || []
      ).filter(entry => {
        if (
          now -
            entry.observedAt >
          Math.max(
            COPIER_PRIMARY_MATCH_WINDOW_MS *
              3,
            6000
          )
        ) {
          return false;
        }

        if (
          !Number.isFinite(
            targetTimestampMs
          )
        ) {
          return true;
        }

        return (
          Math.abs(
            entry
              .providerTimestampMs -
            targetTimestampMs
          ) <=
          COPIER_PRIMARY_MATCH_WINDOW_MS
        );
      });

    const coveredQuantity =
      entries.reduce(
        (
          sum,
          entry
        ) =>
          sum +
          Math.abs(
            num(
              entry.quantity
            )
          ),
        0
      );

    return (
      coveredQuantity >=
      requiredQuantity
    );
  }

  queuePositionDeltaFallback(
    report
  ) {
    const accountId =
      normaliseProviderId(
        report?.accountId
      );

    const contractId =
      normaliseProviderId(
        report?.contractId
      );

    const action =
      asText(
        report?.action
      ).trim();

    const quantity =
      Math.abs(
        num(report?.lastQty)
      );

    const before =
      num(
        report?.previousNetPos
      );

    const after =
      num(
        report?.currentNetPos
      );

    if (
      !accountId ||
      !contractId ||
      !action ||
      quantity < 1
    ) {
      return;
    }

    const key =
      `${accountId}:${contractId}:${before}:${after}:${action}:${quantity}`;

    if (
      this
        .pendingPositionFallbacks
        .has(key)
    ) {
      return;
    }

    const timer =
      setTimeout(
        async () => {
          this
            .pendingPositionFallbacks
            .delete(key);

          if (
            this.stopped ||
            stopping
          ) {
            return;
          }

          const plans =
            activePlansForLeader(
              this.connection
                .owner_id,
              this.connection.id,
              accountId
            );

          if (
            !plans.length
          ) {
            return;
          }

          let symbol = "";

          /*
            Even if the primary execution coverage check suppresses
            a fallback order, a leader transition to FLAT always gets
            a follower residual check.

            This is the final defence against:
              Leader flat
              Followers still +/- contracts
          */
          if (
            after === 0
          ) {
            symbol =
              await this
                .resolveContractName(
                  contractId
                );

            this
              .scheduleLeaderFlatReconcile({
                providerAccountId:
                  accountId,

                contractId,

                symbol:
                  symbol ||
                  `Contract ${contractId}`,

                plans,
              });
          }

          if (
            this
              .hasPrimaryCoverageForPositionDelta(
                report
              )
          ) {
            console.log(
              "copier position fallback suppressed by primary fill",
              {
                connection_id:
                  this.connection
                    .id,

                provider_account_id:
                  accountId,

                contract_id:
                  contractId,

                action,

                quantity,

                previous_net_position:
                  before,

                current_net_position:
                  after,
              }
            );

            return;
          }

          if (!symbol) {
            symbol =
              await this
                .resolveContractName(
                  contractId
                );
          }

          await this
            .routeLeaderDelta({
              plans,

              providerAccountId:
                accountId,

              contractId,

              symbol:
                symbol ||
                `Contract ${contractId}`,

              action,

              deltaQuantity:
                quantity,

              leaderCumQty:
                quantity,

              leaderOrderKey:
                `position:${key}`,

              dispatchKey:
                `position:${key}`,

              providerOrderId:
                null,

              providerExecutionId:
                `position:${key}`,

              telemetry: {
                providerTimestampMs:
                  new Date(
                    asText(
                      report
                        ?.timestamp
                    )
                  ).getTime() ||
                  0,

                workerReceivedAtMs:
                  Date.now(),
              },

              source:
                "position-delta-fallback",
            });
        },
        COPIER_POSITION_FALLBACK_GRACE_MS
      );

    timer.unref?.();

    this
      .pendingPositionFallbacks
      .set(
        key,
        timer
      );
  }

  pruneExecutionMemory() {
    const cutoff =
      Date.now() -
      COPIER_EVENT_MAX_AGE_MS;

    for (
      const [
        key,
        timestamp,
      ] of
      this.seenExecutionIds
    ) {
      if (
        timestamp <
        cutoff
      ) {
        this
          .seenExecutionIds
          .delete(key);
      }
    }

    for (
      const [
        key,
        state,
      ] of
      this.leaderOrderStates
    ) {
      if (
        num(
          state.lastSeenAt,
          0
        ) <
        cutoff
      ) {
        this
          .leaderOrderStates
          .delete(key);
      }
    }
  }

  enqueueOrderWork(
    orderKey,
    task
  ) {
    const previous =
      this
        .orderChains
        .get(orderKey) ||
      Promise.resolve();

    const next =
      previous
        .catch(() => {})
        .then(task);

    this
      .orderChains
      .set(
        orderKey,
        next
      );

    next
      .finally(() => {
        if (
          this
            .orderChains
            .get(orderKey) ===
          next
        ) {
          this
            .orderChains
            .delete(
              orderKey
            );
        }
      })
      .catch(() => {});

    return next;
  }

  async executeFollowerOrder({
    plan,
    follower,
    followerAccount,
    symbol,
    action,
    quantity,
    leaderCumQty,
    leaderOrderKey,
    dispatchKey,
    providerOrderId,
    providerExecutionId,
    telemetry,
    source,
  }) {
    const group =
      plan.group;

    const groupId =
      String(group.id);

    if (
      isGroupSuppressed(
        groupId
      )
    ) {
      return {
        ok: false,
        blocked: true,
        reason:
          "group_suppressed",
      };
    }

    const targetConnectionId =
      asText(
        followerAccount
          ?.source_connection_id
      ).trim();

    const providerAccountId =
      normaliseProviderId(
        followerAccount
          ?.external_id
      );

    const targetSession =
      sessions.get(
        targetConnectionId
      );

    if (
      !targetConnectionId ||
      !providerAccountId ||
      !targetSession ||
      !targetSession
        .accessToken ||
      !targetSession
        .subscribed ||
      targetSession.ws
        ?.readyState !==
        WebSocket.OPEN
    ) {
      await tripCopierSafety(
        group,
        this.connection
          .owner_id,
        "follower_session_unavailable",
        {
          follower_account_id:
            follower.account_id,

          target_connection_id:
            targetConnectionId ||
            null,

          target_provider_account_id:
            providerAccountId ||
            null,
        }
      );

      return {
        ok: false,
        blocked: true,
        reason:
          "follower_session_unavailable",
      };
    }

    const accountSpec =
      asText(
        followerAccount?.name ||
        targetSession
          .accountSpec
      ).trim();

    if (!accountSpec) {
      await tripCopierSafety(
        group,
        this.connection
          .owner_id,
        "tradovate_accountspec_missing",
        {
          follower_account_id:
            follower.account_id,

          target_connection_id:
            targetConnectionId,
        }
      );

      return {
        ok: false,
        blocked: true,
        reason:
          "tradovate_accountspec_missing",
      };
    }

    /*
      IMPORTANT:

      Quantity is calculated from the CUMULATIVE leader fill,
      rather than blindly multiplying each partial fill.

      This makes:
        1 + 2 + 1 + 1 + 5

      safe, and also makes fractional multipliers behave properly.
    */

    const multiplier =
      Math.max(
        0,
        num(
          follower.multiplier,
          1
        )
      );

    const maxQty =
      Math.max(
        1,
        Math.floor(
          num(
            follower.max_qty,
            1
          )
        )
      );

    const progressKey =
      `${leaderOrderKey}:${group.id}:${follower.account_id}`;

    const previousFollowerCum =
      Math.max(
        0,
        num(
          this
            .followerOrderProgress
            .get(progressKey),
          0
        )
      );

    const targetFollowerCum =
      Math.floor(
        Math.abs(
          leaderCumQty
        ) *
          multiplier +
        1e-9
      );

    const computedQty =
      Math.max(
        0,
        targetFollowerCum -
          previousFollowerCum
      );

    if (
      computedQty < 1
    ) {
      return {
        ok: true,
        deduped: true,
        reason:
          "no_follower_delta",
      };
    }

    if (
      targetFollowerCum >
      maxQty
    ) {
      queueAudit({
        owner_id:
          this.connection
            .owner_id,

        group_id:
          group.id,

        leader_account_id:
          group
            .leader_account_id,

        follower_account_id:
          follower.account_id,

        event_type:
          "follower_order_blocked",

        status:
          "blocked",

        dedupe_key:
          `route:${group.id}:${dispatchKey}:${follower.account_id}:qty`,

        provider_order_id:
          providerOrderId ||
          null,

        provider_fill_id:
          providerExecutionId ||
          null,

        symbol,

        action,

        quantity:
          computedQty ||
          quantity,

        message:
          `Follower blocked · target cumulative quantity ${targetFollowerCum} exceeds max ${maxQty}`,

        payload: {
          reason:
            "quantity_limit",

          multiplier,

          max_qty:
            maxQty,

          leader_quantity:
            quantity,

          leader_cumulative_quantity:
            leaderCumQty,

          target_follower_cumulative_quantity:
            targetFollowerCum,

          source,

          ...latencyPayload(
            telemetry
          ),
        },
      });

      return {
        ok: false,
        blocked: true,
        reason:
          "quantity_limit",
      };
    }

    if (
      !symbolAllowed(
        symbol,
        follower
          .allowed_symbols
      )
    ) {
      return {
        ok: false,
        blocked: true,
        reason:
          "symbol_not_allowed",
      };
    }

    const dailyLossLimit =
      Math.abs(
        num(
          follower
            .max_daily_loss,
          0
        )
      );

    const dailyPnl =
      num(
        followerAccount
          ?.daily_pnl,
        0
      );

    if (
      dailyLossLimit > 0 &&
      dailyPnl <=
        -dailyLossLimit
    ) {
      return {
        ok: false,
        blocked: true,
        reason:
          "daily_loss_limit",
      };
    }

    const routeKey =
      `${group.id}:${follower.account_id}:${dispatchKey}`;

    if (
      this
        .followerOrderInFlight
        .has(routeKey)
    ) {
      return {
        ok: true,
        deduped: true,
      };
    }

    this
      .followerOrderInFlight
      .add(routeKey);

    const clientOrderId =
      copierClientOrderId(
        group.id,
        follower.account_id,
        dispatchKey
      );

    const dispatchStartedAtMs =
      Date.now();

    /*
      Reserve the target cumulative quantity BEFORE awaiting the
      Tradovate API request.

      This prevents a second fast execution report from sending the
      same follower delta twice.
    */
    this
      .followerOrderProgress
      .set(
        progressKey,
        targetFollowerCum
      );

    try {
      const response =
        await tradovatePost(
          targetSession
            .environment,
          "/order/placeorder",
          targetSession
            .accessToken,
          {
            accountSpec,

            accountId:
              Number(
                providerAccountId
              ),

            clOrdId:
              clientOrderId,

            action,

            symbol,

            orderQty:
              computedQty,

            orderType:
              "Market",

            isAutomated:
              true,
          }
        );

      const responseAtMs =
        Date.now();

      const copiedOrderId =
        normaliseProviderId(
          response?.orderId ||
          response?.commandId
        );

      queueAudit({
        owner_id:
          this.connection
            .owner_id,

        group_id:
          group.id,

        leader_account_id:
          group
            .leader_account_id,

        follower_account_id:
          follower.account_id,

        event_type:
          "follower_order_submitted",

        status:
          "success",

        dedupe_key:
          `route:${group.id}:${dispatchKey}:${follower.account_id}:submitted`,

        provider_order_id:
          copiedOrderId ||
          null,

        provider_fill_id:
          providerExecutionId ||
          null,

        symbol,

        action,

        quantity:
          computedQty,

        message:
          `LIVE COPY → ${followerAccount?.name || providerAccountId}: ${action} ${computedQty} ${symbol}`,

        payload: {
          cl_ord_id:
            clientOrderId,

          leader_provider_order_id:
            providerOrderId ||
            null,

          target_connection_id:
            targetConnectionId,

          target_provider_account_id:
            providerAccountId,

          provider_response:
            response,

          source,

          ...latencyPayload(
            telemetry,
            dispatchStartedAtMs,
            responseAtMs
          ),
        },
      });

      return {
        ok: true,
        providerOrderId:
          copiedOrderId ||
          null,
      };
    } catch (error) {
      const message =
        error instanceof Error
          ? error.message
          : String(error);

      queueAudit({
        owner_id:
          this.connection
            .owner_id,

        group_id:
          group.id,

        leader_account_id:
          group
            .leader_account_id,

        follower_account_id:
          follower.account_id,

        event_type:
          "follower_order_rejected",

        status:
          "error",

        dedupe_key:
          `route:${group.id}:${dispatchKey}:${follower.account_id}:rejected`,

        provider_order_id:
          providerOrderId ||
          null,

        provider_fill_id:
          providerExecutionId ||
          null,

        symbol,

        action,

        quantity:
          computedQty,

        message:
          `Follower order rejected · ${message}`,

        payload: {
          cl_ord_id:
            clientOrderId,

          target_connection_id:
            targetConnectionId,

          target_provider_account_id:
            providerAccountId,

          source,

          ...latencyPayload(
            telemetry,
            dispatchStartedAtMs,
            Date.now()
          ),
        },
      });

      await tripCopierSafety(
        group,
        this.connection
          .owner_id,
        "follower_order_rejected",
        {
          follower_account_id:
            follower.account_id,

          follower_account_name:
            followerAccount?.name ||
            providerAccountId,

          target_connection_id:
            targetConnectionId,

          error:
            message,
        }
      );

      return {
        ok: false,
        rejected: true,
        reason:
          message,
      };
    } finally {
      setTimeout(
        () =>
          this
            .followerOrderInFlight
            .delete(
              routeKey
            ),
        30000
      ).unref?.();
    }
  }

  preflightPlan(
    plan,
    symbol,
    leaderCumQty
  ) {
    const group =
      plan.group;

    if (
      !group?.armed ||
      !group
        ?.desired_armed ||
      isGroupSuppressed(
        String(group.id)
      )
    ) {
      return {
        ok: false,
        reason:
          "group_not_armed",
      };
    }

    if (
      !symbolAllowed(
        symbol,
        group.allowed_symbols
      )
    ) {
      return {
        ok: false,
        reason:
          "group_symbol_not_allowed",
      };
    }

    if (
      leaderCumQty >
      Math.max(
        1,
        num(
          group.max_leader_qty,
          1
        )
      )
    ) {
      return {
        ok: false,
        reason:
          "leader_quantity_limit",
      };
    }

    /*
      PRECHECK ALL FOLLOWER SESSIONS BEFORE sending a single order.

      If one required account is unhealthy, this group does not fan
      out a trade to the other accounts and leave one behind.
    */
    for (
      const {
        follower,
        account,
      } of
      plan.followers
    ) {
      if (
        follower.enabled !==
        true
      ) {
        continue;
      }

      const multiplier =
        Math.max(
          0,
          num(
            follower.multiplier,
            1
          )
        );

      const maxQty =
        Math.max(
          1,
          Math.floor(
            num(
              follower.max_qty,
              1
            )
          )
        );

      const targetFollowerCum =
        Math.floor(
          Math.abs(
            leaderCumQty
          ) *
            multiplier +
          1e-9
        );

      if (
        targetFollowerCum >
        maxQty
      ) {
        return {
          ok: false,

          reason:
            "follower_quantity_limit",

          follower,

          account,

          targetFollowerCum,

          maxQty,
        };
      }

      const targetSession =
        sessions.get(
          asText(
            account
              .source_connection_id
          ).trim()
        );

      if (
        !targetSession ||
        !targetSession
          .accessToken ||
        !targetSession
          .subscribed ||
        targetSession.ws
          ?.readyState !==
          WebSocket.OPEN
      ) {
        return {
          ok: false,

          reason:
            "follower_session_unavailable",

          follower,

          account,
        };
      }

      if (
        !asText(
          account.name ||
          targetSession
            .accountSpec
        ).trim()
      ) {
        return {
          ok: false,

          reason:
            "tradovate_accountspec_missing",

          follower,

          account,
        };
      }
    }

    return {
      ok: true,
    };
  }

  async routeLeaderDelta({
    plans,
    providerAccountId,
    contractId,
    symbol,
    action,
    deltaQuantity,
    leaderCumQty,
    leaderOrderKey,
    dispatchKey,
    providerOrderId,
    providerExecutionId,
    telemetry,
    source,
  }) {
    if (
      this.stopped ||
      stopping ||
      deltaQuantity < 1
    ) {
      return [];
    }

    /*
      If the cache becomes stale we do NOT guess and we do NOT keep
      copying. We locally block and disarm.
    */
    if (
      !copierCacheIsFresh()
    ) {
      await disarmStaleCachePlans(
        this.connection
          .owner_id,
        this.connection.id,
        providerAccountId
      );

      return [];
    }

    const usablePlans = [];

    for (
      const plan of
      plans
    ) {
      const preflight =
        this.preflightPlan(
          plan,
          symbol,
          leaderCumQty
        );

      if (
        !preflight.ok
      ) {
        if (
          [
            "follower_session_unavailable",
            "tradovate_accountspec_missing",
          ].includes(
            preflight.reason
          )
        ) {
          await tripCopierSafety(
            plan.group,
            this.connection
              .owner_id,
            preflight.reason,
            {
              follower_account_id:
                preflight
                  .follower
                  ?.account_id ||
                null,

              target_connection_id:
                preflight
                  .account
                  ?.source_connection_id ||
                null,
            }
          );
        } else if (
          preflight.reason ===
          "leader_quantity_limit"
        ) {
          await tripCopierSafety(
            plan.group,
            this.connection
              .owner_id,
            "leader_quantity_limit",
            {
              leader_cumulative_quantity:
                leaderCumQty,

              max_leader_qty:
                plan.group
                  .max_leader_qty,
            }
          );
        } else if (
          preflight.reason ===
          "follower_quantity_limit"
        ) {
          await tripCopierSafety(
            plan.group,
            this.connection
              .owner_id,
            "follower_quantity_limit",
            {
              follower_account_id:
                preflight
                  .follower
                  ?.account_id ||
                null,

              target_follower_cumulative_quantity:
                preflight
                  .targetFollowerCum,

              max_qty:
                preflight.maxQty,
            }
          );
        }

        continue;
      }

      usablePlans.push(plan);
    }

    const routes = [];

    for (
      const plan of
      usablePlans
    ) {
      /*
        Do not block order dispatch on audit logging.
      */
      setImmediate(() =>
        queueAudit({
          owner_id:
            this.connection
              .owner_id,

          group_id:
            plan.group.id,

          leader_account_id:
            plan.group
              .leader_account_id,

          event_type:
            "live_leader_fill_detected",

          status:
            "success",

          dedupe_key:
            `live:${this.connection.id}:${dispatchKey}:group:${plan.group.id}`,

          provider_order_id:
            providerOrderId ||
            null,

          provider_fill_id:
            providerExecutionId ||
            null,

          symbol,

          action,

          quantity:
            deltaQuantity,

          message:
            `LIVE LEADER DELTA · ${action} ${deltaQuantity} ${symbol} · cumulative ${leaderCumQty}`,

          payload: {
            stage:
              "hot_cache_direct_dispatch",

            source,

            connection_id:
              this.connection.id,

            provider_account_id:
              providerAccountId,

            contract_id:
              contractId,

            leader_cumulative_quantity:
              leaderCumQty,

            execution_enabled:
              true,

            worker_version:
              VERSION,

            ...latencyPayload(
              telemetry
            ),
          },
        })
      );

      for (
        const route of
        plan.followers
      ) {
        routes.push({
          plan,
          ...route,
        });
      }
    }

    if (!routes.length) {
      return [];
    }

    const startedAt =
      Date.now();

    /*
      ALL follower order HTTP calls are kicked off together.
    */
    const results =
      await Promise.allSettled(
        routes.map(
          ({
            plan,
            follower,
            account,
          }) =>
            this
              .executeFollowerOrder({
                plan,

                follower,

                followerAccount:
                  account,

                symbol,

                action,

                quantity:
                  deltaQuantity,

                leaderCumQty,

                leaderOrderKey,

                dispatchKey,

                providerOrderId,

                providerExecutionId,

                telemetry,

                source,
              })
        )
      );

    const rejectedPlans =
      new Set();

    results.forEach(
      (
        result,
        index
      ) => {
        const value =
          result.status ===
          "fulfilled"
            ? result.value
            : null;

        if (
          result.status ===
            "rejected" ||
          value?.rejected
        ) {
          rejectedPlans.add(
            String(
              routes[index]
                .plan.group.id
            )
          );
        }
      }
    );

    /*
      If follower execution fails, any follower positions that DID
      get the trade are flattened as a safety action.
    */
    for (
      const groupId of
      rejectedPlans
    ) {
      const plan =
        usablePlans.find(
          item =>
            String(
              item.group.id
            ) ===
            groupId
        );

      if (plan) {
        this
          .scheduleSafetyFlatten(
            plan,
            `dispatch failure on ${dispatchKey}`
          );
      }
    }

    console.log(
      "copier direct parallel batch complete",
      {
        connection_id:
          this.connection.id,

        dispatch_key:
          dispatchKey,

        action,

        delta_quantity:
          deltaQuantity,

        leader_cumulative_quantity:
          leaderCumQty,

        followers:
          routes.length,

        elapsed_ms:
          Date.now() -
          startedAt,
      }
    );

    return usablePlans;
  }

  async processLeaderExecution(
    report
  ) {
    const workerReceivedAtMs =
      num(
        report
          ?.workerReceivedAtMs,
        Date.now()
      );

    const reportId =
      normaliseProviderId(
        report?.id
      );

    const providerAccountId =
      normaliseProviderId(
        report?.accountId
      );

    const contractId =
      normaliseProviderId(
        report?.contractId
      );

    const providerOrderId =
      normaliseProviderId(
        report?.orderId ||
        report?.commandId ||
        reportId
      );

    const action =
      asText(
        report?.action
      ).trim();

    const lastQty =
      Math.abs(
        num(report?.lastQty)
      );

    const cumQtyRaw =
      Number(
        report?.cumQty
      );

    const cumQty =
      Number.isFinite(
        cumQtyRaw
      )
        ? Math.abs(
            cumQtyRaw
          )
        : null;

    const ordStatus =
      asText(
        report?.ordStatus
      )
        .trim()
        .toLowerCase();

    const execType =
      asText(
        report?.execType
      )
        .trim()
        .toLowerCase();

    const providerTimestamp =
      asText(
        report?.timestamp
      ).trim();

    const providerTimestampMs =
      new Date(
        providerTimestamp
      ).getTime();

    if (
      !reportId ||
      !providerAccountId ||
      !contractId ||
      !action ||
      !providerOrderId
    ) {
      return;
    }

    if (
      providerTimestampMs &&
      Date.now() -
        providerTimestampMs >
        COPIER_EVENT_MAX_AGE_MS
    ) {
      return;
    }

    if (
      this
        .seenExecutionIds
        .has(reportId)
    ) {
      return;
    }

    this
      .seenExecutionIds
      .set(
        reportId,
        Date.now()
      );

    this
      .pruneExecutionMemory();

    const plans =
      activePlansForLeader(
        this.connection
          .owner_id,
        this.connection.id,
        providerAccountId
      );

    if (!plans.length) {
      return;
    }

    const isTrade =
      execType === "trade" ||
      execType ===
        "completed" ||
      ordStatus ===
        "filled" ||
      lastQty > 0;

    if (!isTrade) {
      return;
    }

    this
      .rememberPrimaryLeaderFill(
        report
      );

    /*
      Contract names are normally already warm. If not, the first
      event resolves it and caches it for every following partial.
    */
    const symbol =
      await this
        .resolveContractName(
          contractId
        );

    const orderKey =
      `${providerAccountId}:${providerOrderId}:${contractId}:${action.toLowerCase()}`;

    const state =
      this
        .leaderOrderStates
        .get(orderKey) || {
          observedCumQty:
            0,

          dispatchedCumQty:
            0,

          lastSeenAt:
            Date.now(),
        };

    let observedCumQty;

    /*
      This is the key fix for the 10-lot stop failure.

      If Tradovate reports:
        lastQty = 1, cumQty = 1
        lastQty = 2, cumQty = 3
        lastQty = 1, cumQty = 4
        lastQty = 1, cumQty = 5
        lastQty = 5, cumQty = 10

      we dispatch the difference between the newest cumQty and what
      has already been dispatched.
    */
    if (
      cumQty !== null &&
      cumQty > 0
    ) {
      observedCumQty =
        Math.max(
          state
            .observedCumQty,
          cumQty
        );
    } else if (
      lastQty > 0
    ) {
      observedCumQty =
        state
          .observedCumQty +
        lastQty;
    } else {
      return;
    }

    state.observedCumQty =
      observedCumQty;

    state.lastSeenAt =
      Date.now();

    const deltaQuantity =
      Math.max(
        0,
        observedCumQty -
          state
            .dispatchedCumQty
      );

    console.log(
      "copier leader cumulative execution",
      {
        connection_id:
          this.connection.id,

        provider_account_id:
          providerAccountId,

        provider_order_id:
          providerOrderId,

        report_id:
          reportId,

        action,

        last_qty:
          lastQty,

        cum_qty:
          cumQty,

        observed_cum_qty:
          observedCumQty,

        already_dispatched:
          state
            .dispatchedCumQty,

        delta_to_dispatch:
          deltaQuantity,

        symbol:
          symbol ||
          `Contract ${contractId}`,

        provider_to_worker_ms:
          Number.isFinite(
            providerTimestampMs
          )
            ? Math.max(
                0,
                workerReceivedAtMs -
                  providerTimestampMs
              )
            : null,
      }
    );

    let routedPlans = [];

    if (
      deltaQuantity > 0
    ) {
      /*
        Reserve the cumulative leader quantity locally before any
        follower network request begins.
      */
      state.dispatchedCumQty =
        observedCumQty;

      this
        .leaderOrderStates
        .set(
          orderKey,
          state
        );

      const dispatchKey =
        `${providerOrderId}:cum:${observedCumQty}`;

      routedPlans =
        await this
          .routeLeaderDelta({
            plans,

            providerAccountId,

            contractId,

            symbol:
              symbol ||
              `Contract ${contractId}`,

            action,

            deltaQuantity,

            leaderCumQty:
              observedCumQty,

            leaderOrderKey:
              orderKey,

            dispatchKey,

            providerOrderId,

            providerExecutionId:
              reportId,

            telemetry: {
              providerTimestampMs:
                Number.isFinite(
                  providerTimestampMs
                )
                  ? providerTimestampMs
                  : 0,

              workerReceivedAtMs,
            },

            source:
              "execution-report-cumulative",
          });
    } else {
      this
        .leaderOrderStates
        .set(
          orderKey,
          state
        );
    }

    const finalFill =
      ordStatus ===
        "filled" ||
      execType ===
        "completed" ||
      (
        Number(
          report?.leavesQty
        ) === 0 &&
        observedCumQty > 0
      );

    /*
      A fully-filled ORDER does not necessarily mean FLAT
      (it could be an entry).

      So this schedules a position check. If the actual leader
      position is zero, every follower is checked for residual
      contracts.
    */
    if (finalFill) {
      this
        .scheduleLeaderFlatReconcile({
          providerAccountId,

          contractId,

          symbol:
            symbol ||
            `Contract ${contractId}`,

          plans:
            routedPlans.length
              ? routedPlans
              : plans,
        });
    }
  }

  detectLeaderExecution(
    report
  ) {
    const providerAccountId =
      normaliseProviderId(
        report?.accountId
      );

    if (
      !providerAccountId
    ) {
      return Promise.resolve();
    }

    /*
      Follower execution reports are ignored here.
      Only an account currently configured as a copier leader can
      enter the dispatch pipeline.
    */
    const plans =
      activePlansForLeader(
        this.connection
          .owner_id,
        this.connection.id,
        providerAccountId
      );

    if (!plans.length) {
      return Promise.resolve();
    }

    const providerOrderId =
      normaliseProviderId(
        report?.orderId ||
        report?.commandId ||
        report?.id
      );

    const contractId =
      normaliseProviderId(
        report?.contractId
      );

    const action =
      asText(
        report?.action
      )
        .trim()
        .toLowerCase();

    const orderKey =
      `${providerAccountId}:${providerOrderId}:${contractId}:${action}`;

    /*
      Serialise only messages belonging to the same leader order.
      Different orders/connections are still concurrent.
    */
    return this
      .enqueueOrderWork(
        orderKey,
        () =>
          this
            .processLeaderExecution(
              report
            )
      );
  }

  async scanLeaderPositions(
    reason =
      "periodic-position-scan"
  ) {
    if (
      this
        .copierPositionScanInFlight ||
      !this.accessToken ||
      !this.subscribed
    ) {
      return;
    }

    this
      .copierPositionScanInFlight =
      true;

    try {
      const positionsPayload =
        await tradovateGet(
          this.environment,
          "/position/list",
          this.accessToken
        );

      const positions =
        Array.isArray(
          positionsPayload
        )
          ? positionsPayload
          : [];

      const current =
        new Map();

      /*
        Only snapshot accounts that are actual configured leaders.
        Follower positions do not feed back into the copier.
      */
      for (
        const row of
        positions
      ) {
        const accountId =
          normaliseProviderId(
            row?.accountId
          );

        const contractId =
          normaliseProviderId(
            row?.contractId
          );

        if (
          !accountId ||
          !contractId
        ) {
          continue;
        }

        if (
          !plansForLeader(
            this.connection
              .owner_id,
            this.connection.id,
            accountId
          ).length
        ) {
          continue;
        }

        current.set(
          `${accountId}:${contractId}`,
          {
            accountId,

            contractId,

            netPos:
              num(
                row?.netPos,
                num(
                  row?.netPosition,
                  0
                )
              ),

            raw:
              row,
          }
        );
      }

      if (
        !this
          .copierPositionInitialized
      ) {
        this
          .copierPositionSnapshot =
          current;

        this
          .copierPositionInitialized =
          true;

        console.log(
          "copier leader position baseline ready",
          {
            connection_id:
              this.connection
                .id,

            positions:
              current.size,

            reason,
          }
        );

        return;
      }

      const keys =
        new Set([
          ...this
            .copierPositionSnapshot
            .keys(),

          ...current.keys(),
        ]);

      for (
        const key of
        keys
      ) {
        const before =
          this
            .copierPositionSnapshot
            .get(key) || {
              accountId:
                key.split(
                  ":"
                )[0],

              contractId:
                key.split(
                  ":"
                )[1],

              netPos:
                0,
            };

        const after =
          current.get(key) || {
            accountId:
              before.accountId,

            contractId:
              before.contractId,

            netPos:
              0,
          };

        const delta =
          num(
            after.netPos
          ) -
          num(
            before.netPos
          );

        if (!delta) {
          continue;
        }

        const action =
          delta > 0
            ? "Buy"
            : "Sell";

        const quantity =
          Math.abs(delta);

        const positionTimestamp =
          asText(
            after.raw
              ?.timestamp
          ).trim() ||
          nowIso();

        this
          .queuePositionDeltaFallback({
            id:
              `pos-${after.accountId}-${after.contractId}-${num(before.netPos)}-${num(after.netPos)}-${Date.now()}`,

            accountId:
              after.accountId,

            contractId:
              after.contractId,

            action,

            lastQty:
              quantity,

            timestamp:
              positionTimestamp,

            previousNetPos:
              num(
                before.netPos
              ),

            currentNetPos:
              num(
                after.netPos
              ),

            rawPosition:
              after.raw ||
              null,
          });
      }

      this
        .copierPositionSnapshot =
        current;
    } catch (error) {
      console.error(
        "copier leader position scan failed",
        this.connection.id,
        error instanceof Error
          ? error.message
          : String(error)
      );
    } finally {
      this
        .copierPositionScanInFlight =
        false;
    }
  }

  startCopierPositionScanner() {
    if (
      this
        .copierPositionScanInterval
    ) {
      clearInterval(
        this
          .copierPositionScanInterval
      );
    }

    this
      .copierPositionInitialized =
      false;

    this
      .copierPositionSnapshot =
      new Map();

    this
      .scanLeaderPositions(
        "initial-position-baseline"
      )
      .catch(() => {});

    this
      .copierPositionScanInterval =
      setInterval(
        () => {
          this
            .scanLeaderPositions(
              "periodic-position-scan"
            )
            .catch(
              () => {}
            );
        },
        COPIER_POSITION_SCAN_MS
      );

    this
      .copierPositionScanInterval
      .unref?.();
  }

  handleCopierFrame(
    frame
  ) {
    const frameReceivedAtMs =
      Date.now();

    for (
      const detail of
      propsEvents(frame)
    ) {
      const entityType =
        asText(
          detail?.entityType
        )
          .trim()
          .toLowerCase();

      const entities =
        Array.isArray(
          detail?.entity
        )
          ? detail.entity
          : [
              detail?.entity,
            ];

      for (
        const entity of
        entities
      ) {
        if (
          !entity ||
          typeof entity !==
            "object"
        ) {
          continue;
        }

        if (
          entity?.contractId
        ) {
          this
            .primeContractName(
              entity.contractId
            );
        }

        /*
          v7.7.20 primary copier execution source:
          executionReport only.

          We don't also independently dispatch fill entities because
          execution reports contain the cumulative quantity needed
          for deterministic partial-fill handling.
        */
        if (
          entityType !==
          "executionreport"
        ) {
          continue;
        }

        const providerAccountId =
          normaliseProviderId(
            entity?.accountId
          );

        if (
          !providerAccountId
        ) {
          continue;
        }

        if (
          !plansForLeader(
            this.connection
              .owner_id,
            this.connection.id,
            providerAccountId
          ).length
        ) {
          continue;
        }

        this
          .detectLeaderExecution({
            ...entity,
            workerReceivedAtMs:
              frameReceivedAtMs,
          })
          .catch(error =>
            console.error(
              "copier execution handler failed",
              this.connection
                .id,
              error instanceof
                Error
                ? error.message
                : String(error)
            )
          );
      }
    }
  }

  scheduleLeaderFlatReconcile({
    providerAccountId,
    contractId,
    symbol,
    plans,
  }) {
    const key =
      `${providerAccountId}:${contractId}`;

    if (
      this
        .reconcileLocks
        .has(key)
    ) {
      return;
    }

    this
      .reconcileLocks
      .add(key);

    /*
      Give accepted market follower orders time to actually fill
      before checking for residual contracts.

      This prevents the safety guard from racing the normal copy.
    */
    const timer =
      setTimeout(
        async () => {
          try {
            const positionsPayload =
              await tradovateGet(
                this.environment,
                "/position/list",
                this.accessToken
              );

            const leaderPosition =
              (
                Array.isArray(
                  positionsPayload
                )
                  ? positionsPayload
                  : []
              ).find(
                row =>
                  normaliseProviderId(
                    row
                      ?.accountId
                  ) ===
                    providerAccountId &&
                  normaliseProviderId(
                    row
                      ?.contractId
                  ) ===
                    contractId
              );

            const leaderNet =
              num(
                leaderPosition
                  ?.netPos,
                num(
                  leaderPosition
                    ?.netPosition,
                  0
                )
              );

            /*
              Entry order?
              Leader isn't flat. Do nothing.
            */
            if (
              leaderNet !== 0
            ) {
              return;
            }

            console.log(
              "leader flat confirmed; reconciling follower residuals",
              {
                connection_id:
                  this.connection
                    .id,

                provider_account_id:
                  providerAccountId,

                contract_id:
                  contractId,

                symbol,
              }
            );

            await Promise.allSettled(
              (plans || [])
                .map(plan =>
                  this
                    .reconcilePlanFollowersFlat(
                      plan,
                      contractId,
                      symbol,
                      "leader-flat-guard"
                    )
                )
            );
          } catch (error) {
            console.error(
              "leader flat reconcile check failed",
              this.connection
                .id,
              error instanceof Error
                ? error.message
                : String(error)
            );
          } finally {
            this
              .reconcileLocks
              .delete(key);
          }
        },
        LEADER_FLAT_RECONCILE_DELAY_MS
      );

    timer.unref?.();
  }

  scheduleSafetyFlatten(
    plan,
    reason
  ) {
    const key =
      `safety:${plan.group.id}`;

    if (
      this
        .reconcileLocks
        .has(key)
    ) {
      return;
    }

    this
      .reconcileLocks
      .add(key);

    const timer =
      setTimeout(
        async () => {
          try {
            await this
              .reconcilePlanFollowersFlat(
                plan,
                null,
                null,
                `safety-flatten:${reason}`
              );
          } finally {
            this
              .reconcileLocks
              .delete(key);
          }
        },
        150
      );

    timer.unref?.();
  }

  async reconcilePlanFollowersFlat(
    plan,
    contractId = null,
    symbolHint = null,
    reason = "reconcile"
  ) {
    const grouped =
      new Map();

    for (
      const route of
      plan.followers || []
    ) {
      const connectionId =
        asText(
          route.account
            ?.source_connection_id
        ).trim();

      if (!connectionId) {
        continue;
      }

      const list =
        grouped.get(
          connectionId
        ) || [];

      list.push(
        route.account
      );

      grouped.set(
        connectionId,
        list
      );
    }

    /*
      One position-list request per provider login, not one request
      per follower account.
    */
    for (
      const [
        connectionId,
        accounts,
      ] of
      grouped
    ) {
      const targetSession =
        sessions.get(
          connectionId
        );

      if (
        !targetSession
          ?.accessToken
      ) {
        continue;
      }

      try {
        const positionsPayload =
          await tradovateGet(
            targetSession
              .environment,
            "/position/list",
            targetSession
              .accessToken
          );

        const accountByProvider =
          new Map(
            accounts.map(
              account => [
                normaliseProviderId(
                  account
                    .external_id
                ),
                account,
              ]
            )
          );

        const residuals =
          (
            Array.isArray(
              positionsPayload
            )
              ? positionsPayload
              : []
          ).filter(row => {
            const accountId =
              normaliseProviderId(
                row?.accountId
              );

            const rowContractId =
              normaliseProviderId(
                row?.contractId
              );

            const netPos =
              num(
                row?.netPos,
                num(
                  row
                    ?.netPosition,
                  0
                )
              );

            return (
              accountByProvider.has(
                accountId
              ) &&
              netPos !== 0 &&
              (
                !contractId ||
                rowContractId ===
                  contractId
              )
            );
          });

        await Promise.allSettled(
          residuals.map(
            async row => {
              const accountId =
                normaliseProviderId(
                  row.accountId
                );

              const account =
                accountByProvider.get(
                  accountId
                );

              const netPos =
                num(
                  row?.netPos,
                  num(
                    row
                      ?.netPosition,
                    0
                  )
                );

              const rowContractId =
                normaliseProviderId(
                  row.contractId
                );

              const symbol =
                symbolHint ||
                await targetSession
                  .resolveContractName(
                    rowContractId
                  );

              if (!symbol) {
                throw new Error(
                  `Unable to resolve contract ${rowContractId}`
                );
              }

              /*
                Residual +7 long -> SELL 7
                Residual -7 short -> BUY 7
              */
              const action =
                netPos > 0
                  ? "Sell"
                  : "Buy";

              const qty =
                Math.abs(
                  netPos
                );

              const dispatchKey =
                `flat:${plan.group.id}:${accountId}:${rowContractId}:${Date.now()}`;

              const response =
                await tradovatePost(
                  targetSession
                    .environment,
                  "/order/placeorder",
                  targetSession
                    .accessToken,
                  {
                    accountSpec:
                      asText(
                        account
                          ?.name ||
                        targetSession
                          .accountSpec
                      ).trim(),

                    accountId:
                      Number(
                        accountId
                      ),

                    clOrdId:
                      copierClientOrderId(
                        plan.group
                          .id,
                        account
                          ?.id ||
                          accountId,
                        dispatchKey
                      ),

                    action,

                    symbol,

                    orderQty:
                      qty,

                    orderType:
                      "Market",

                    isAutomated:
                      true,
                  }
                );

              queueAudit({
                owner_id:
                  this.connection
                    .owner_id,

                group_id:
                  plan.group.id,

                leader_account_id:
                  plan.group
                    .leader_account_id,

                follower_account_id:
                  account?.id ||
                  null,

                event_type:
                  "follower_residual_flattened",

                status:
                  "success",

                dedupe_key:
                  dispatchKey,

                provider_order_id:
                  normaliseProviderId(
                    response
                      ?.orderId ||
                    response
                      ?.commandId
                  ) ||
                  null,

                symbol,

                action,

                quantity:
                  qty,

                message:
                  `RESIDUAL FLATTEN → ${account?.name || accountId}: ${action} ${qty} ${symbol}`,

                payload: {
                  reason,

                  prior_net_position:
                    netPos,

                  worker_version:
                    VERSION,
                },
              });
            }
          )
        );
      } catch (error) {
        console.error(
          "follower residual reconcile failed",
          plan.group.id,
          connectionId,
          error instanceof Error
            ? error.message
            : String(error)
        );
      }
    }
  }

  async connectOnce() {
    this.authorized =
      false;

    this.subscribed =
      false;

    this.lastCloseInfo =
      "";

    const {
      token,
      userIds,
      accountIds,
      accountSpec,
      environment,
      wsUrl:
        brokeredWsUrl,
    } =
      await this
        .loadCredential();

    this.accessToken =
      token;

    this.accountSpec =
      accountSpec || "";

    this.environment =
      environment;

    const wsUrl =
      brokeredWsUrl ||
      `wss://${environment}.tradovateapi.com/v1/websocket`;

    await writeStatus(
      this.connection,
      {
        state:
          "connecting",

        last_error:
          null,

        reconnect_count:
          this
            .reconnectCount,

        last_heartbeat_at:
          nowIso(),
      }
    );

    await new Promise(
      (
        resolve,
        reject
      ) => {
        const ws =
          new WebSocket(
            wsUrl,
            {
              handshakeTimeout:
                20000,
            }
          );

        this.ws = ws;

        let settled =
          false;

        const finishReject =
          error => {
            if (
              !settled
            ) {
              settled =
                true;

              try {
                ws.close(
                  1011,
                  "connection-failed"
                );
              } catch {}

              reject(error);
            }
          };

        const sendSocketHeartbeat =
          () => {
            if (
              ws.readyState !==
              WebSocket.OPEN
            ) {
              return;
            }

            try {
              ws.send("[]");

              this
                .lastClientHeartbeatAt =
                Date.now();
            } catch (error) {
              console.error(
                "Tradovate heartbeat send failed",
                this.connection
                  .id,
                error instanceof
                  Error
                  ? error.message
                  : String(error)
              );
            }
          };

        ws.on(
          "open",
          () => {
            sendRequest(
              ws,
              "authorize",
              0,
              token
            );

            this
              .socketHeartbeatTimer =
              setInterval(
                sendSocketHeartbeat,
                TRADOVATE_HEARTBEAT_MS
              );
          }
        );

        ws.on(
          "message",
          async raw => {
            const rawText =
              asText(raw);

            if (
              rawText === "h"
            ) {
              if (
                Date.now() -
                  this
                    .lastClientHeartbeatAt >=
                1000
              ) {
                sendSocketHeartbeat();
              }

              return;
            }

            const frames =
              unpackFrame(
                rawText
              );

            if (
              !frames.length
            ) {
              return;
            }

            for (
              const frame of
              frames
            ) {
              /*
                AUTHORIZE RESPONSE
              */
              if (
                Number(
                  frame.i
                ) === 0
              ) {
                if (
                  Number(
                    frame.s
                  ) !== 200
                ) {
                  return finishReject(
                    new Error(
                      `websocket authorize: ${frameError(
                        frame,
                        "Tradovate WebSocket authorization failed"
                      )}`
                    )
                  );
                }

                this.authorized =
                  true;

                const syncBody =
                  accountIds
                    ?.length
                    ? {
                        accounts:
                          accountIds,

                        splitResponses:
                          true,

                        entityTypes:
                          [
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
                        users:
                          userIds,

                        splitResponses:
                          true,
                      };

                sendRequest(
                  ws,
                  "user/syncrequest",
                  1,
                  syncBody
                );

                continue;
              }

              /*
                SYNC REQUEST FAILED
              */
              if (
                Number(
                  frame.i
                ) === 1 &&
                Number(
                  frame.s
                ) >= 400
              ) {
                return finishReject(
                  new Error(
                    `user/syncrequest: ${frameError(
                      frame,
                      "Tradovate user synchronization failed"
                    )}`
                  )
                );
              }

              /*
                SYNC SUBSCRIBED
              */
              if (
                Number(
                  frame.i
                ) === 1 &&
                Number(
                  frame.s
                ) === 200 &&
                !this
                  .subscribed
              ) {
                this.subscribed =
                  true;

                this.backoffMs =
                  1000;

                await supabase
                  .from(
                    "provider_sync_checkpoints"
                  )
                  .upsert(
                    {
                      connection_id:
                        this
                          .connection
                          .id,

                      checkpoint_key:
                        "copier_worker_version",

                      checkpoint_value:
                        VERSION,

                      metadata: {
                        worker_instance:
                          INSTANCE_ID,

                        detection_mode:
                          "execution-report-cumulative-quantity-with-position-fallback",

                        follower_execution_enabled:
                          true,

                        execution_gate:
                          "hot-cached copier config + local safety suppression",

                        provider_order_mode:
                          "market-isAutomated",

                        follower_dispatch_mode:
                          "parallel-direct-no-db-critical-path",

                        dispatch_preflight:
                          "in-memory",

                        audit_mode:
                          "async-after-dispatch",

                        latency_telemetry:
                          true,

                        partial_fill_mode:
                          "cumQty-delta",

                        emergency_flatten_enabled:
                          true,

                        emergency_flatten_endpoint:
                          "order/placeorder-opposite-residual",

                        emergency_command_poll_ms:
                          COPIER_COMMAND_POLL_MS,

                        leader_flat_residual_guard:
                          true,

                        safety_disarm_on_execution_error:
                          true,

                        config_cache_refresh_ms:
                          COPIER_CONFIG_REFRESH_MS,

                        config_cache_max_stale_ms:
                          COPIER_CONFIG_MAX_STALE_MS,
                      },

                      updated_at:
                        nowIso(),
                    },
                    {
                      onConflict:
                        "connection_id,checkpoint_key",
                    }
                  );

                await writeStatus(
                  this.connection,
                  {
                    state:
                      "live",

                    last_connected_at:
                      nowIso(),

                    last_heartbeat_at:
                      nowIso(),

                    reconnect_count:
                      this
                        .reconnectCount,

                    event_count:
                      this
                        .eventCount,

                    pulse_count:
                      this
                        .pulseCount,

                    last_error:
                      null,
                  }
                );

                this
                  .schedulePulse(
                    "initial-live-snapshot",
                    350
                  );

                this
                  .startCopierPositionScanner();

                this
                  .warmContractCache()
                  .catch(
                    () => {}
                  );

                if (
                  this
                    .sessionRefreshTimer
                ) {
                  clearTimeout(
                    this
                      .sessionRefreshTimer
                  );
                }

                this
                  .sessionRefreshTimer =
                  setTimeout(
                    () => {
                      if (
                        this.ws
                          ?.readyState ===
                        WebSocket.OPEN
                      ) {
                        console.log(
                          "scheduled Tradovate token refresh reconnect",
                          this
                            .connection
                            .id
                        );

                        try {
                          this.ws.close(
                            1000,
                            "scheduled-token-refresh"
                          );
                        } catch {}
                      }
                    },
                    SESSION_RECONNECT_MS
                  );

                this
                  .sessionRefreshTimer
                  .unref?.();

                if (
                  !settled
                ) {
                  settled =
                    true;

                  resolve();
                }

                continue;
              }

              /*
                PROVIDER EVENTS
              */
              if (
                this.authorized
              ) {
                this
                  .handleCopierFrame(
                    frame
                  );

                this.eventCount +=
                  1;

                const eventAt =
                  nowIso();

                writeStatus(
                  this.connection,
                  {
                    state:
                      "live",

                    last_event_at:
                      eventAt,

                    last_heartbeat_at:
                      eventAt,

                    event_count:
                      this
                        .eventCount,

                    reconnect_count:
                      this
                        .reconnectCount,

                    last_error:
                      null,
                  }
                ).catch(
                  () => {}
                );

                this
                  .schedulePulse(
                    "tradovate-user-event"
                  );
              }
            }
          }
        );

        ws.on(
          "error",
          finishReject
        );

        ws.on(
          "close",
          (
            code,
            reason
          ) => {
            this.lastCloseInfo =
              `Tradovate WebSocket closed (${code}) ${asText(
                reason
              )}`.trim();

            this
              .clearHeartbeat();

            this.authorized =
              false;

            this.subscribed =
              false;

            this.ws =
              null;

            if (
              !this.stopped &&
              !stopping
            ) {
              finishReject(
                new Error(
                  this
                    .lastCloseInfo
                )
              );
            } else if (
              !settled
            ) {
              settled =
                true;

              resolve();
            }
          }
        );

        this
          .heartbeatTimer =
          setInterval(
            () => {
              writeStatus(
                this.connection,
                {
                  state:
                    this
                      .subscribed
                      ? "live"
                      : "connecting",

                  last_heartbeat_at:
                    nowIso(),

                  event_count:
                    this
                      .eventCount,

                  pulse_count:
                    this
                      .pulseCount,

                  reconnect_count:
                    this
                      .reconnectCount,
                }
              ).catch(
                error =>
                  console.error(
                    "heartbeat failed",
                    error
                  )
              );
            },
            HEARTBEAT_MS
          );
      }
    );

    while (
      !this.stopped &&
      !stopping &&
      this.ws?.readyState ===
        WebSocket.OPEN
    ) {
      await sleep(1000);
    }

    if (
      !this.stopped &&
      !stopping
    ) {
      throw new Error(
        this.lastCloseInfo ||
        "Tradovate WebSocket disconnected"
      );
    }
  }

  clearHeartbeat() {
    if (
      this
        .heartbeatTimer
    ) {
      clearInterval(
        this
          .heartbeatTimer
      );
    }

    if (
      this
        .socketHeartbeatTimer
    ) {
      clearInterval(
        this
          .socketHeartbeatTimer
      );
    }

    if (
      this
        .sessionRefreshTimer
    ) {
      clearTimeout(
        this
          .sessionRefreshTimer
      );
    }

    if (
      this
        .copierPositionScanInterval
    ) {
      clearInterval(
        this
          .copierPositionScanInterval
      );
    }

    for (
      const timer of
      this
        .pendingPositionFallbacks
        .values()
    ) {
      clearTimeout(
        timer
      );
    }

    this
      .pendingPositionFallbacks
      .clear();

    this
      .recentPrimaryLeaderFills
      .clear();

    this
      .contractNamePromiseCache
      .clear();

    this
      .orderChains
      .clear();

    this
      .leaderOrderStates
      .clear();

    this
      .seenExecutionIds
      .clear();

    this
      .followerOrderProgress
      .clear();

    this.heartbeatTimer =
      null;

    this.socketHeartbeatTimer =
      null;

    this.sessionRefreshTimer =
      null;

    this
      .copierPositionScanInterval =
      null;

    this
      .copierPositionInitialized =
      false;

    this
      .copierPositionSnapshot =
      new Map();
  }

  schedulePulse(
    reason,
    delay =
      PULSE_MIN_INTERVAL_MS
  ) {
    if (
      this.stopped ||
      stopping
    ) {
      return;
    }

    if (
      this.pulseTimer
    ) {
      clearTimeout(
        this.pulseTimer
      );
    }

    const sinceLast =
      Date.now() -
      this.lastPulseAt;

    const wait =
      Math.max(
        delay,

        PULSE_MIN_INTERVAL_MS -
          sinceLast,

        0
      );

    this.pulseTimer =
      setTimeout(
        () =>
          this.runPulse(
            reason
          ),
        wait
      );
  }

  async runPulse(
    reason
  ) {
    this.pulseTimer =
      null;

    if (
      this.pulseInFlight
    ) {
      this.pulseQueued =
        true;

      return;
    }

    this.pulseInFlight =
      true;

    this.lastPulseAt =
      Date.now();

    try {
      const response =
        await fetch(
          `${SUPABASE_URL}/functions/v1/tradovate-live-pulse`,
          {
            method:
              "POST",

            headers: {
              "Content-Type":
                "application/json",

              Authorization:
                `Bearer ${SERVICE_ROLE_KEY}`,

              apikey:
                SERVICE_ROLE_KEY,

              "x-fra-worker-secret":
                WORKER_SECRET,
            },

            body:
              JSON.stringify({
                connection_id:
                  this
                    .connection
                    .id,

                reason,
              }),
          }
        );

      const payload =
        await response
          .json()
          .catch(
            () => ({})
          );

      if (
        !response.ok ||
        payload?.error
      ) {
        throw new Error(
          payload?.error ||
          `Live pulse failed (${response.status})`
        );
      }

      this.pulseCount +=
        1;

      await writeStatus(
        this.connection,
        {
          state:
            payload
              .requires_full_sync
              ? "attention"
              : "live",

          last_pulse_at:
            nowIso(),

          last_success_at:
            nowIso(),

          last_heartbeat_at:
            nowIso(),

          pulse_count:
            this
              .pulseCount,

          event_count:
            this
              .eventCount,

          last_error:
            payload
              .requires_full_sync
              ? "A newly detected account needs one full sync"
              : null,

          metadata: {
            last_pulse_result:
              payload,

            last_reason:
              reason,
          },
        }
      );
    } catch (error) {
      const message =
        error instanceof Error
          ? error.message
          : String(error);

      console.error(
        "live pulse failed",
        this.connection.id,
        message
      );

      await writeStatus(
        this.connection,
        {
          state:
            "attention",

          last_heartbeat_at:
            nowIso(),

          last_error:
            message,

          pulse_count:
            this
              .pulseCount,

          event_count:
            this
              .eventCount,
        }
      );
    } finally {
      this.pulseInFlight =
        false;

      if (
        this.pulseQueued
      ) {
        this.pulseQueued =
          false;

        this
          .schedulePulse(
            "queued-provider-event",
            350
          );
      }
    }
  }

  async stop(
    reason = "stopped"
  ) {
    this.stopped =
      true;

    if (
      this.pulseTimer
    ) {
      clearTimeout(
        this.pulseTimer
      );
    }

    this
      .clearHeartbeat();

    try {
      this.ws?.close(
        1000,
        reason
      );
    } catch {}

    this.ws =
      null;

    await writeStatus(
      this.connection,
      {
        state:
          reason ===
          "not-eligible"
            ? "manual"
            : "offline",

        last_heartbeat_at:
          nowIso(),

        last_error:
          reason ===
          "not-eligible"
            ? "Live sync is available on paid plans"
            : null,
      }
    );
  }
}

/*
  ============================================================
  EMERGENCY FLATTEN
  ============================================================

  v7.7.19 used:
      /order/liquidateposition

  Your actual test proved that endpoint returned 401 across all
  follower accounts.

  v7.7.20 instead reads the actual net position and sends an exact
  opposite MARKET order through /order/placeorder, which is the same
  permission path already proven to work for copied orders.

  Example:
      follower position = -7
      emergency order = Buy 7

      follower position = +7
      emergency order = Sell 7
*/

const OPEN_ORDER_STATUSES =
  new Set([
    "working",
    "pendingnew",
    "pendingreplace",
    "pendingcancel",
    "suspended",
    "unknown",
  ]);

const isOpenTradovateOrder =
  order =>
    OPEN_ORDER_STATUSES.has(
      asText(
        order?.ordStatus
      )
        .trim()
        .toLowerCase()
    );

function normalizeFlattenTargets(
  command
) {
  const rawTargets =
    Array.isArray(
      command?.payload
        ?.targets
    )
      ? command.payload.targets
      : [];

  const seen =
    new Set();

  const targets = [];

  for (
    const row of
    rawTargets
  ) {
    const accountId =
      asText(
        row?.account_id
      ).trim();

    const connectionId =
      asText(
        row
          ?.source_connection_id
      ).trim();

    const providerAccountId =
      normaliseProviderId(
        row
          ?.provider_account_id
      );

    if (
      !accountId ||
      !connectionId ||
      !providerAccountId
    ) {
      continue;
    }

    const key =
      `${connectionId}:${providerAccountId}`;

    if (
      seen.has(key)
    ) {
      continue;
    }

    seen.add(key);

    targets.push({
      account_id:
        accountId,

      name:
        asText(
          row?.name
        ).trim() ||
        providerAccountId,

      source_connection_id:
        connectionId,

      provider_account_id:
        providerAccountId,

      was_enabled:
        row?.was_enabled ===
        true,
    });
  }

  return targets;
}

async function loadFlattenTargetsFromDatabase(
  command
) {
  const ownerId =
    asText(
      command?.owner_id
    ).trim();

  const groupId =
    asText(
      command?.group_id
    ).trim();

  if (
    !ownerId ||
    !groupId
  ) {
    return [];
  }

  const {
    data: followers,
    error: followerError,
  } =
    await supabase
      .from(
        "copier_followers"
      )
      .select(
        "account_id,enabled"
      )
      .eq(
        "group_id",
        groupId
      )
      .eq(
        "owner_id",
        ownerId
      );

  if (followerError) {
    throw followerError;
  }

  const accountIds = [
    ...new Set(
      (followers || [])
        .map(
          row =>
            row.account_id
        )
        .filter(Boolean)
    ),
  ];

  if (
    !accountIds.length
  ) {
    return [];
  }

  const {
    data: accounts,
    error: accountError,
  } =
    await supabase
      .from("accounts")
      .select(
        "id,name,external_id,source_connection_id,is_archived"
      )
      .eq(
        "owner_id",
        ownerId
      )
      .in(
        "id",
        accountIds
      );

  if (accountError) {
    throw accountError;
  }

  const enabledById =
    new Map(
      (followers || [])
        .map(row => [
          String(
            row.account_id
          ),

          row.enabled ===
            true,
        ])
    );

  return (
    accounts || []
  )
    .filter(
      row =>
        !row.is_archived &&
        row
          .source_connection_id &&
        row.external_id
    )
    .map(row => ({
      account_id:
        row.id,

      name:
        asText(
          row.name
        ).trim() ||
        normaliseProviderId(
          row.external_id
        ),

      source_connection_id:
        asText(
          row
            .source_connection_id
        ).trim(),

      provider_account_id:
        normaliseProviderId(
          row.external_id
        ),

      was_enabled:
        enabledById.get(
          String(row.id)
        ) === true,
    }));
}

async function emergencyRestSession(
  connectionId
) {
  const live =
    sessions.get(
      connectionId
    );

  if (
    live?.accessToken &&
    live?.environment
  ) {
    return {
      token:
        live.accessToken,

      environment:
        live.environment,

      accountSpec:
        live.accountSpec ||
        "",

      liveSession:
        live,

      source:
        "live-session",
    };
  }

  const brokered =
    await requestWorkerSession(
      connectionId
    );

  return {
    token:
      brokered.token,

    environment:
      brokered
        .environment,

    accountSpec:
      brokered
        .accountSpec ||
      "",

    liveSession:
      null,

    source:
      "fresh-rest-session",
  };
}

async function writeFlattenEvent(
  command,
  eventType,
  status,
  message,
  payload = {}
) {
  const { error } =
    await supabase
      .from(
        "copier_events"
      )
      .insert({
        owner_id:
          command.owner_id,

        group_id:
          command.group_id,

        event_type:
          eventType,

        status,

        dedupe_key:
          `flatten:${command.id}:${eventType}`,

        message,

        payload: {
          command_id:
            command.id,

          worker_instance:
            INSTANCE_ID,

          worker_version:
            VERSION,

          ...payload,
        },
      });

  if (
    error &&
    error.code !== "23505"
  ) {
    console.error(
      "flatten audit insert failed",
      command.id,
      error.message
    );
  }
}

async function completeCopierCommand(
  command,
  status,
  result = {},
  errorMessage = null
) {
  const { error } =
    await supabase
      .from(
        "copier_commands"
      )
      .update({
        status,

        result,

        error:
          errorMessage,

        completed_at:
          nowIso(),

        worker_instance:
          INSTANCE_ID,
      })
      .eq(
        "id",
        command.id
      )
      .eq(
        "worker_instance",
        INSTANCE_ID
      );

  if (error) {
    throw error;
  }
}

async function forceGroupDisarmed(
  command
) {
  /*
    Local kill switch first.
  */
  markGroupSuppressed(
    String(
      command.group_id
    )
  );

  const { error } =
    await supabase
      .from(
        "copier_groups"
      )
      .update({
        armed: false,
        desired_armed:
          false,
        updated_at:
          nowIso(),
      })
      .eq(
        "id",
        command.group_id
      )
      .eq(
        "owner_id",
        command.owner_id
      );

  if (error) {
    throw error;
  }
}

async function resolveFlattenSymbol(
  rest,
  contractId
) {
  if (
    rest.liveSession
  ) {
    const symbol =
      await rest
        .liveSession
        .resolveContractName(
          contractId
        );

    if (symbol) {
      return symbol;
    }
  }

  const contract =
    await tradovateGet(
      rest.environment,
      `/contract/item?id=${encodeURIComponent(
        contractId
      )}`,
      rest.token
    );

  return asText(
    contract?.name
  ).trim();
}

async function flattenConnectionTargets(
  command,
  connectionId,
  targets
) {
  const rest =
    await emergencyRestSession(
      connectionId
    );

  const providerIds =
    new Set(
      targets
        .map(row =>
          normaliseProviderId(
            row
              .provider_account_id
          )
        )
        .filter(Boolean)
    );

  const numericProviderIds =
    new Set(
      [...providerIds]
        .map(Number)
        .filter(
          Number.isFinite
        )
    );

  const targetByProvider =
    new Map(
      targets.map(row => [
        normaliseProviderId(
          row
            .provider_account_id
        ),
        row,
      ])
    );

  const result = {
    connection_id:
      connectionId,

    session_source:
      rest.source,

    environment:
      rest.environment,

    account_count:
      targets.length,

    cancelled_orders:
      0,

    cancel_errors:
      [],

    flatten_order_requests:
      0,

    flatten_errors:
      [],

    residual_positions:
      [],

    residual_orders:
      [],

    verification_ok:
      false,
  };

  /*
    First try to cancel any working orders.

    This matters because flattening the current net position while an
    old stop/order remains working could allow that old order to open
    a new position afterwards.
  */
  const cancelWorkingOrders =
    async () => {
      const payload =
        await tradovateGet(
          rest.environment,
          "/order/list",
          rest.token
        );

      const rows =
        Array.isArray(
          payload
        )
          ? payload
          : [];

      const openOrders =
        rows.filter(order => {
          const accountId =
            Number(
              order
                ?.accountId
            );

          return (
            Number.isFinite(
              accountId
            ) &&
            numericProviderIds.has(
              accountId
            ) &&
            isOpenTradovateOrder(
              order
            ) &&
            Number(
              order?.id
            ) > 0
          );
        });

      const settled =
        await Promise.allSettled(
          openOrders.map(
            order =>
              tradovatePost(
                rest.environment,
                "/order/cancelorder",
                rest.token,
                {
                  orderId:
                    Number(
                      order.id
                    ),
                }
              )
          )
        );

      settled.forEach(
        (
          entry,
          index
        ) => {
          if (
            entry.status ===
            "fulfilled"
          ) {
            result.cancelled_orders +=
              1;
          } else {
            result
              .cancel_errors
              .push({
                order_id:
                  Number(
                    openOrders[
                      index
                    ]?.id
                  ) ||
                  null,

                error:
                  entry.reason instanceof
                    Error
                    ? entry
                        .reason
                        .message
                    : String(
                        entry.reason
                      ),
              });
          }
        }
      );

      return openOrders;
    };

  try {
    await cancelWorkingOrders();
  } catch (error) {
    result
      .cancel_errors
      .push({
        scope:
          "cancel-scan",

        error:
          error instanceof Error
            ? error.message
            : String(error),
      });
  }

  /*
    Sends exact opposite market orders for all remaining non-zero
    follower positions.
  */
  const flattenResiduals =
    async positions => {
      const residuals =
        (
          Array.isArray(
            positions
          )
            ? positions
            : []
        ).filter(
          position => {
            const accountId =
              Number(
                position
                  ?.accountId
              );

            const netPos =
              num(
                position
                  ?.netPos,
                num(
                  position
                    ?.netPosition,
                  0
                )
              );

            return (
              Number.isFinite(
                accountId
              ) &&
              numericProviderIds.has(
                accountId
              ) &&
              netPos !== 0
            );
          }
        );

      await Promise.allSettled(
        residuals.map(
          async position => {
            const accountId =
              normaliseProviderId(
                position
                  .accountId
              );

            const contractId =
              normaliseProviderId(
                position
                  .contractId
              );

            const netPos =
              num(
                position
                  ?.netPos,
                num(
                  position
                    ?.netPosition,
                  0
                )
              );

            const target =
              targetByProvider.get(
                accountId
              );

            const symbol =
              await resolveFlattenSymbol(
                rest,
                contractId
              );

            if (!symbol) {
              throw new Error(
                `Unable to resolve contract ${contractId}`
              );
            }

            const action =
              netPos > 0
                ? "Sell"
                : "Buy";

            const qty =
              Math.abs(
                netPos
              );

            const dispatchKey =
              `manual-flat:${command.id}:${accountId}:${contractId}:${Date.now()}`;

            try {
              await tradovatePost(
                rest.environment,
                "/order/placeorder",
                rest.token,
                {
                  accountSpec:
                    asText(
                      target
                        ?.name ||
                      rest
                        .accountSpec
                    ).trim(),

                  accountId:
                    Number(
                      accountId
                    ),

                  clOrdId:
                    copierClientOrderId(
                      command
                        .group_id,
                      target
                        ?.account_id ||
                        accountId,
                      dispatchKey
                    ),

                  action,

                  symbol,

                  orderQty:
                    qty,

                  orderType:
                    "Market",

                  isAutomated:
                    true,
                }
              );

              result
                .flatten_order_requests +=
                1;
            } catch (error) {
              result
                .flatten_errors
                .push({
                  account_id:
                    Number(
                      accountId
                    ),

                  contract_id:
                    Number(
                      contractId
                    ),

                  net_position:
                    netPos,

                  action,

                  quantity:
                    qty,

                  error:
                    error instanceof
                      Error
                      ? error.message
                      : String(
                          error
                        ),
                });
            }
          }
        )
      );
    };

  let finalPositions =
    [];

  let finalOrders =
    [];

  /*
    Verification loop:

    1. Read actual positions.
    2. Read working orders.
    3. Flatten residual positions using placeorder.
    4. Cancel remaining working orders.
    5. Repeat.
    6. Only claim success when BOTH are empty.
  */
  for (
    let attempt = 0;
    attempt <
    COPIER_FLATTEN_VERIFY_ATTEMPTS;
    attempt += 1
  ) {
    let positionsPayload;

    try {
      positionsPayload =
        await tradovateGet(
          rest.environment,
          "/position/list",
          rest.token
        );
    } catch (error) {
      result
        .flatten_errors
        .push({
          scope:
            "position-list",

          error:
            error instanceof
              Error
              ? error.message
              : String(error),
        });

      break;
    }

    finalPositions =
      (
        Array.isArray(
          positionsPayload
        )
          ? positionsPayload
          : []
      ).filter(
        position => {
          const accountId =
            Number(
              position
                ?.accountId
            );

          const netPos =
            num(
              position
                ?.netPos,
              num(
                position
                  ?.netPosition,
                0
              )
            );

          return (
            Number.isFinite(
              accountId
            ) &&
            numericProviderIds.has(
              accountId
            ) &&
            netPos !== 0
          );
        }
      );

    try {
      const orderPayload =
        await tradovateGet(
          rest.environment,
          "/order/list",
          rest.token
        );

      finalOrders =
        (
          Array.isArray(
            orderPayload
          )
            ? orderPayload
            : []
        ).filter(
          order => {
            const accountId =
              Number(
                order
                  ?.accountId
              );

            return (
              Number.isFinite(
                accountId
              ) &&
              numericProviderIds.has(
                accountId
              ) &&
              isOpenTradovateOrder(
                order
              ) &&
              Number(
                order?.id
              ) > 0
            );
          }
        );
    } catch (error) {
      result
        .cancel_errors
        .push({
          scope:
            "order-list",

          error:
            error instanceof
              Error
              ? error.message
              : String(error),
        });
    }

    if (
      !finalPositions.length &&
      !finalOrders.length
    ) {
      result.verification_ok =
        true;

      break;
    }

    if (
      finalOrders.length
    ) {
      await Promise.allSettled(
        finalOrders.map(
          order =>
            tradovatePost(
              rest.environment,
              "/order/cancelorder",
              rest.token,
              {
                orderId:
                  Number(
                    order.id
                  ),
              }
            )
        )
      );
    }

    if (
      finalPositions.length
    ) {
      await flattenResiduals(
        finalPositions
      );
    }

    await sleep(
      COPIER_FLATTEN_VERIFY_MS
    );
  }

  result.residual_positions =
    finalPositions.map(
      position => ({
        account_id:
          Number(
            position?.accountId
          ) ||
          null,

        contract_id:
          Number(
            position
              ?.contractId
          ) ||
          null,

        net_position:
          num(
            position?.netPos,
            num(
              position
                ?.netPosition,
              0
            )
          ),
      })
    );

  result.residual_orders =
    finalOrders.map(
      order => ({
        order_id:
          Number(
            order?.id
          ) ||
          null,

        account_id:
          Number(
            order?.accountId
          ) ||
          null,

        contract_id:
          Number(
            order?.contractId
          ) ||
          null,

        status:
          asText(
            order?.ordStatus
          ).trim() ||
          null,
      })
    );

  return result;
}

async function processFlattenCommand(
  command
) {
  const startedAt =
    Date.now();

  /*
    DISARM FIRST.
  */
  await forceGroupDisarmed(
    command
  );

  await writeFlattenEvent(
    command,
    "flatten_started",
    "pending",
    "EMERGENCY FLATTEN STARTED · copier disarmed; follower positions and working orders are being cleared",
    {
      requested_at:
        command.requested_at ||
        null,
    }
  );

  let targets =
    normalizeFlattenTargets(
      command
    );

  if (
    !targets.length
  ) {
    targets =
      await loadFlattenTargetsFromDatabase(
        command
      );
  }

  if (
    !targets.length
  ) {
    const result = {
      ok: true,

      target_count:
        0,

      connection_results:
        [],

      elapsed_ms:
        Date.now() -
        startedAt,
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

  const byConnection =
    new Map();

  for (
    const target of
    targets
  ) {
    const list =
      byConnection.get(
        target
          .source_connection_id
      ) || [];

    list.push(target);

    byConnection.set(
      target
        .source_connection_id,
      list
    );
  }

  const entries = [
    ...byConnection.entries(),
  ];

  /*
    Apex / Lucid / MFFU flatten concurrently.
  */
  const settled =
    await Promise.allSettled(
      entries.map(
        ([
          connectionId,
          connectionTargets,
        ]) =>
          flattenConnectionTargets(
            command,
            connectionId,
            connectionTargets
          )
      )
    );

  const connectionResults =
    settled.map(
      (
        entry,
        index
      ) => {
        const connectionId =
          entries[index][0];

        if (
          entry.status ===
          "fulfilled"
        ) {
          return entry.value;
        }

        return {
          connection_id:
            connectionId,

          account_count:
            byConnection.get(
              connectionId
            )?.length ||
            0,

          fatal_error:
            entry.reason instanceof
              Error
              ? entry.reason
                  .message
              : String(
                  entry.reason
                ),

          residual_positions:
            [],

          residual_orders:
            [],

          verification_ok:
            false,
        };
      }
    );

  const fatalCount =
    connectionResults.filter(
      row =>
        row.fatal_error
    ).length;

  const residualPositionCount =
    connectionResults.reduce(
      (
        sum,
        row
      ) =>
        sum +
        (
          row
            .residual_positions
            ?.length ||
          0
        ),
      0
    );

  const residualOrderCount =
    connectionResults.reduce(
      (
        sum,
        row
      ) =>
        sum +
        (
          row
            .residual_orders
            ?.length ||
          0
        ),
      0
    );

  const actionErrorCount =
    connectionResults.reduce(
      (
        sum,
        row
      ) =>
        sum +
        (
          row
            .cancel_errors
            ?.length ||
          0
        ) +
        (
          row
            .flatten_errors
            ?.length ||
          0
        ),
      0
    );

  const verificationFailureCount =
    connectionResults.filter(
      row =>
        row.verification_ok !==
        true
    ).length;

  const clean =
    fatalCount === 0 &&
    verificationFailureCount ===
      0 &&
    residualPositionCount ===
      0 &&
    residualOrderCount ===
      0;

  const status =
    clean
      ? "success"
      : (
          fatalCount ===
          connectionResults.length
            ? "error"
            : "partial"
        );

  const result = {
    ok:
      clean,

    target_count:
      targets.length,

    connection_count:
      byConnection.size,

    fatal_count:
      fatalCount,

    verification_failure_count:
      verificationFailureCount,

    action_error_count:
      actionErrorCount,

    residual_position_count:
      residualPositionCount,

    residual_order_count:
      residualOrderCount,

    elapsed_ms:
      Date.now() -
      startedAt,

    connection_results:
      connectionResults,
  };

  const errorMessage =
    clean
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

async function failCopierCommand(
  command,
  error
) {
  const message =
    error instanceof Error
      ? error.message
      : String(error);

  try {
    await forceGroupDisarmed(
      command
    );

    await completeCopierCommand(
      command,
      "error",
      {
        ok: false,

        worker_version:
          VERSION,

        worker_instance:
          INSTANCE_ID,
      },
      message
    );

    await writeFlattenEvent(
      command,
      "flatten_failed",
      "error",
      `EMERGENCY FLATTEN FAILED · ${message}`,
      {
        error:
          message,
      }
    );
  } catch (
    secondaryError
  ) {
    console.error(
      "unable to record failed copier command",
      command?.id,
      secondaryError instanceof
        Error
        ? secondaryError.message
        : String(
            secondaryError
          )
    );
  }
}

async function runCopierCommandPump() {
  console.log(
    `copier emergency command pump active · ${COPIER_COMMAND_POLL_MS}ms`
  );

  while (!stopping) {
    try {
      const {
        data,
        error,
      } =
        await supabase.rpc(
          "claim_copier_commands_v1",
          {
            p_worker_instance:
              INSTANCE_ID,

            p_limit:
              4,
          }
        );

      if (error) {
        throw error;
      }

      const commands =
        Array.isArray(data)
          ? data
          : [];

      for (
        const command of
        commands
      ) {
        if (stopping) {
          break;
        }

        if (
          asText(
            command
              ?.command_type
          ).trim() !==
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
          await processFlattenCommand(
            command
          );
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

    await sleep(
      COPIER_COMMAND_POLL_MS
    );
  }
}

async function runCopierConfigPump() {
  console.log(
    `copier hot config cache active · refresh=${COPIER_CONFIG_REFRESH_MS}ms max-stale=${COPIER_CONFIG_MAX_STALE_MS}ms`
  );

  while (!stopping) {
    await sleep(
      COPIER_CONFIG_REFRESH_MS
    );

    await refreshCopierCache();
  }
}

async function reconcileTargets() {
  const targets =
    await loadTargets();

  const targetById =
    new Map(
      targets.map(
        row => [
          row.id,
          row,
        ]
      )
    );

  for (
    const [
      id,
      session,
    ] of
    sessions
  ) {
    const target =
      targetById.get(id);

    if (!target) {
      sessions.delete(id);

      await session.stop(
        "not-eligible"
      );
    } else {
      session
        .updateConnection(
          target
        );
    }
  }

  for (
    const target of
    targets
  ) {
    if (
      sessions.has(
        target.id
      )
    ) {
      continue;
    }

    const session =
      new LiveSession(
        target
      );

    sessions.set(
      target.id,
      session
    );

    session
      .start()
      .catch(error =>
        console.error(
          "session stopped unexpectedly",
          target.id,
          error
        )
      );
  }

  const liveCount =
    [
      ...sessions.values(),
    ].filter(
      session =>
        session.subscribed &&
        session.ws
          ?.readyState ===
          WebSocket.OPEN
    ).length;

  const connectingCount =
    Math.max(
      sessions.size -
        liveCount,
      0
    );

  console.log(
    `[${nowIso()}] live targets=${targets.length} subscribed=${liveCount} connecting=${connectingCount}`
  );
}

async function shutdown(
  signal
) {
  if (stopping) {
    return;
  }

  stopping =
    true;

  console.log(
    `received ${signal}; closing ${sessions.size} live sessions`
  );

  await Promise.allSettled(
    [
      ...sessions.values(),
    ].map(
      session =>
        session.stop(
          "worker-shutdown"
        )
    )
  );

  process.exit(0);
}

process.on(
  "SIGTERM",
  () =>
    shutdown("SIGTERM")
);

process.on(
  "SIGINT",
  () =>
    shutdown("SIGINT")
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

/*
  Load copier configuration BEFORE opening provider sessions.

  This prevents a Render restart from briefly receiving an execution
  report while the worker still has an empty copier config cache.
*/
await refreshCopierCache();

runCopierConfigPump()
  .catch(error =>
    console.error(
      "copier config pump stopped unexpectedly",
      error
    )
  );

runCopierCommandPump()
  .catch(error =>
    console.error(
      "copier command pump stopped unexpectedly",
      error
    )
  );

await reconcileTargets();

while (!stopping) {
  await sleep(
    TARGET_REFRESH_MS
  );

  try {
    await reconcileTargets();
  } catch (error) {
    console.error(
      "target refresh failed",
      error
    );
  }
}
