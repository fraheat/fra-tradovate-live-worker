import { readFile, writeFile } from "node:fs/promises";
import { spawnSync } from "node:child_process";
import { dirname, join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const sourcePath = join(here, "index.mjs");
const generatedPath = join(here, ".index-v7.7.21.generated.mjs");
const checkOnly = process.argv.includes("--check-only");

let source = await readFile(sourcePath, "utf8");

function replaceOnce(input, needle, replacement, label) {
  const first = input.indexOf(needle);
  if (first < 0) {
    throw new Error(`v7.7.21 hotfix anchor missing: ${label}`);
  }
  const second = input.indexOf(needle, first + needle.length);
  if (second >= 0) {
    throw new Error(`v7.7.21 hotfix anchor is not unique: ${label}`);
  }
  return input.slice(0, first) + replacement + input.slice(first + needle.length);
}

function replaceRegexOnce(input, regex, replacement, label) {
  const matches = [...input.matchAll(new RegExp(regex.source, regex.flags.includes("g") ? regex.flags : `${regex.flags}g`))];
  if (matches.length !== 1) {
    throw new Error(`v7.7.21 hotfix regex expected 1 match for ${label}, found ${matches.length}`);
  }
  return input.replace(regex, replacement);
}

if (source.includes('const VERSION = "7.7.21";')) {
  // Source was later folded forward. Do not patch it twice.
} else {
  source = replaceOnce(
    source,
    'const VERSION = "7.7.20";',
    'const VERSION = "7.7.21";',
    "worker version"
  );

  source = replaceOnce(
    source,
    `const COPIER_POSITION_FALLBACK_GRACE_MS = Math.max(\n  500,\n  Number(process.env.COPIER_POSITION_FALLBACK_GRACE_MS || 1500)\n);`,
    `const COPIER_POSITION_FALLBACK_GRACE_MS = Math.max(\n  250,\n  Number(process.env.COPIER_POSITION_FALLBACK_GRACE_MS || 600)\n);`,
    "position fallback grace"
  );

  source = replaceOnce(
    source,
    `const LEADER_FLAT_RECONCILE_DELAY_MS = Math.max(\n  750,\n  Number(process.env.LEADER_FLAT_RECONCILE_DELAY_MS || 1500)\n);`,
    `const LEADER_FLAT_RECONCILE_DELAY_MS = Math.max(\n  350,\n  Number(process.env.LEADER_FLAT_RECONCILE_DELAY_MS || 650)\n);`,
    "leader flat reconcile delay"
  );

  source = replaceOnce(
    source,
    `    const entries =\n      this\n        .recentPrimaryLeaderFills\n        .get(key) || [];\n\n    entries.push({`,
    `    const entries =\n      this\n        .recentPrimaryLeaderFills\n        .get(key) || [];\n\n    const providerFillId =\n      normaliseProviderId(\n        report?.execRefId ||\n        report?.id\n      );\n\n    /*\n      executionReport and fill can describe the SAME provider fill.\n      If they share an execRef/fill id, count it only once so the\n      position fallback coverage cannot be inflated by duplicate\n      event sources.\n    */\n    if (\n      providerFillId &&\n      entries.some(\n        entry =>\n          entry.providerFillId ===\n          providerFillId\n      )\n    ) {\n      return;\n    }\n\n    entries.push({`,
    "primary fill coverage dedupe"
  );

  source = replaceOnce(
    source,
    `      providerFillId:\n        normaliseProviderId(\n          report?.execRefId ||\n          report?.id\n        ),`,
    `      providerFillId,`,
    "primary fill id reuse"
  );

  const insertedMethods = `  async detectLeaderFill(\n    fill,\n    eventType = "fill"\n  ) {\n    const fillId =\n      normaliseProviderId(\n        fill?.id\n      );\n\n    const orderId =\n      normaliseProviderId(\n        fill?.orderId\n      );\n\n    if (\n      !fillId ||\n      !orderId\n    ) {\n      return;\n    }\n\n    const fillTimestamp =\n      asText(\n        fill?.timestamp\n      ).trim();\n\n    const fillTimestampMs =\n      new Date(\n        fillTimestamp\n      ).getTime();\n\n    if (\n      Number.isFinite(\n        fillTimestampMs\n      ) &&\n      Date.now() -\n        fillTimestampMs >\n        COPIER_EVENT_MAX_AGE_MS\n    ) {\n      return;\n    }\n\n    let order = {};\n\n    const needsOrder =\n      !fill?.accountId ||\n      !fill?.contractId ||\n      !fill?.action;\n\n    if (needsOrder) {\n      try {\n        order =\n          await tradovateGet(\n            this.environment,\n            \`/order/item?id=\${encodeURIComponent(\n              orderId\n            )}\`,\n            this.accessToken\n          );\n      } catch (error) {\n        console.error(\n          "copier fill order lookup failed",\n          this.connection.id,\n          orderId,\n          error instanceof Error\n            ? error.message\n            : String(error)\n        );\n\n        return;\n      }\n    }\n\n    const providerAccountId =\n      normaliseProviderId(\n        fill?.accountId ||\n        order?.accountId\n      );\n\n    const contractId =\n      normaliseProviderId(\n        fill?.contractId ||\n        order?.contractId\n      );\n\n    const action =\n      asText(\n        fill?.action ||\n        order?.action\n      ).trim();\n\n    const quantity =\n      Math.abs(\n        num(\n          fill?.qty,\n          num(fill?.quantity)\n        )\n      );\n\n    if (\n      !providerAccountId ||\n      !contractId ||\n      !action ||\n      quantity < 1\n    ) {\n      return;\n    }\n\n    if (\n      !activePlansForLeader(\n        this.connection.owner_id,\n        this.connection.id,\n        providerAccountId\n      ).length\n    ) {\n      return;\n    }\n\n    const orderKey =\n      \`\${providerAccountId}:\${orderId}:\${contractId}:\${action.toLowerCase()}\`;\n\n    return this\n      .enqueueOrderWork(\n        orderKey,\n        () =>\n          this.processLeaderFill({\n            id: fillId,\n            accountId:\n              providerAccountId,\n            contractId,\n            action,\n            qty: quantity,\n            orderId,\n            ordStatus:\n              asText(\n                fill?.ordStatus ||\n                order?.ordStatus\n              ).trim(),\n            timestamp:\n              fillTimestamp ||\n              nowIso(),\n            price:\n              num(\n                fill?.price,\n                null\n              ),\n            workerReceivedAtMs:\n              num(\n                fill?.workerReceivedAtMs,\n                Date.now()\n              ),\n            rawFill:\n              fill,\n          }, eventType)\n      );\n  }\n\n  async processLeaderFill(\n    fill,\n    eventType = "fill"\n  ) {\n    const fillId =\n      normaliseProviderId(\n        fill?.id\n      );\n\n    const providerAccountId =\n      normaliseProviderId(\n        fill?.accountId\n      );\n\n    const contractId =\n      normaliseProviderId(\n        fill?.contractId\n      );\n\n    const providerOrderId =\n      normaliseProviderId(\n        fill?.orderId\n      );\n\n    const action =\n      asText(\n        fill?.action\n      ).trim();\n\n    const quantity =\n      Math.abs(\n        num(fill?.qty)\n      );\n\n    if (\n      !fillId ||\n      !providerAccountId ||\n      !contractId ||\n      !providerOrderId ||\n      !action ||\n      quantity < 1\n    ) {\n      return;\n    }\n\n    const plans =\n      activePlansForLeader(\n        this.connection.owner_id,\n        this.connection.id,\n        providerAccountId\n      );\n\n    if (!plans.length) {\n      return;\n    }\n\n    const symbol =\n      await this\n        .resolveContractName(\n          contractId\n        );\n\n    const orderKey =\n      \`\${providerAccountId}:\${providerOrderId}:\${contractId}:\${action.toLowerCase()}\`;\n\n    const state =\n      this\n        .leaderOrderStates\n        .get(orderKey) || {\n          observedCumQty:\n            0,\n          dispatchedCumQty:\n            0,\n          lastSeenAt:\n            Date.now(),\n        };\n\n    if (!(state.fillQuantities instanceof Map)) {\n      state.fillQuantities =\n        new Map();\n    }\n\n    const previousFillQuantity =\n      Math.max(\n        0,\n        num(\n          state.fillQuantities\n            .get(fillId),\n          0\n        )\n      );\n\n    if (\n      quantity >\n      previousFillQuantity\n    ) {\n      state.fillQuantities\n        .set(\n          fillId,\n          quantity\n        );\n    }\n\n    const fillCumQty =\n      [...state.fillQuantities\n        .values()]\n        .reduce(\n          (sum, value) =>\n            sum +\n            Math.abs(\n              num(value)\n            ),\n          0\n        );\n\n    const observedCumQty =\n      Math.max(\n        Math.max(\n          0,\n          num(\n            state.observedCumQty,\n            0\n          )\n        ),\n        fillCumQty\n      );\n\n    const alreadyDispatched =\n      Math.max(\n        0,\n        num(\n          state.dispatchedCumQty,\n          0\n        )\n      );\n\n    const deltaQuantity =\n      Math.max(\n        0,\n        observedCumQty -\n          alreadyDispatched\n      );\n\n    state.observedCumQty =\n      observedCumQty;\n\n    state.lastSeenAt =\n      Date.now();\n\n    let routedPlans = [];\n\n    if (deltaQuantity > 0) {\n      /*\n        Reserve the SAME cumulative state used by executionReport.\n        Whichever event source wins the race becomes primary; the\n        other source sees zero remaining delta and cannot duplicate\n        the follower order.\n      */\n      state.dispatchedCumQty =\n        observedCumQty;\n\n      this.leaderOrderStates\n        .set(\n          orderKey,\n          state\n        );\n\n      const dispatchKey =\n        \`\${providerOrderId}:cum:\${observedCumQty}\`;\n\n      routedPlans =\n        await this.routeLeaderDelta({\n          plans,\n          providerAccountId,\n          contractId,\n          symbol:\n            symbol ||\n            \`Contract \${contractId}\`,\n          action,\n          deltaQuantity,\n          leaderCumQty:\n            observedCumQty,\n          leaderOrderKey:\n            orderKey,\n          dispatchKey,\n          providerOrderId,\n          providerExecutionId:\n            fillId,\n          telemetry: {\n            providerTimestampMs:\n              new Date(\n                asText(\n                  fill?.timestamp\n                )\n              ).getTime() ||\n              0,\n            workerReceivedAtMs:\n              num(\n                fill?.workerReceivedAtMs,\n                Date.now()\n              ),\n            lastPrice:\n              num(\n                fill?.price,\n                null\n              ),\n          },\n          source:\n            "fill-websocket",\n        });\n\n      if (routedPlans.length) {\n        this.rememberPrimaryLeaderFill({\n          accountId:\n            providerAccountId,\n          contractId,\n          action,\n          lastQty:\n            deltaQuantity,\n          timestamp:\n            fill?.timestamp ||\n            nowIso(),\n          execRefId:\n            fillId,\n          id:\n            fillId,\n        });\n      }\n    } else {\n      this.leaderOrderStates\n        .set(\n          orderKey,\n          state\n        );\n    }\n\n    console.log(\n      "copier leader fill normalized",\n      {\n        connection_id:\n          this.connection.id,\n        provider_account_id:\n          providerAccountId,\n        provider_order_id:\n          providerOrderId,\n        provider_fill_id:\n          fillId,\n        action,\n        fill_qty:\n          quantity,\n        fill_cumulative_qty:\n          fillCumQty,\n        already_dispatched:\n          alreadyDispatched,\n        delta_to_dispatch:\n          deltaQuantity,\n        event_type:\n          eventType ||\n          null,\n      }\n    );\n\n    /*\n      Check flatness after EVERY fill. A stop-loss fill does not need\n      an executionReport in order to trigger the residual guard.\n    */\n    this.scheduleLeaderFlatReconcile({\n      providerAccountId,\n      contractId,\n      symbol:\n        symbol ||\n        \`Contract \${contractId}\`,\n      plans:\n        routedPlans.length\n          ? routedPlans\n          : plans,\n    });\n  }\n\n  handleLeaderPositionEntity(\n    position,\n    workerReceivedAtMs = Date.now()\n  ) {\n    const accountId =\n      normaliseProviderId(\n        position?.accountId\n      );\n\n    const contractId =\n      normaliseProviderId(\n        position?.contractId\n      );\n\n    if (\n      !accountId ||\n      !contractId ||\n      (\n        position?.netPos === undefined &&\n        position?.netPosition === undefined\n      )\n    ) {\n      return;\n    }\n\n    if (\n      !plansForLeader(\n        this.connection.owner_id,\n        this.connection.id,\n        accountId\n      ).length\n    ) {\n      return;\n    }\n\n    const key =\n      \`\${accountId}:\${contractId}\`;\n\n    const afterNet =\n      num(\n        position?.netPos,\n        num(\n          position?.netPosition,\n          0\n        )\n      );\n\n    const nextRow = {\n      accountId,\n      contractId,\n      netPos:\n        afterNet,\n      raw:\n        position,\n    };\n\n    /*\n      During initial Tradovate sync, position entities are snapshots,\n      not new trades. They are baseline only until the REST scanner\n      has initialised the position map.\n    */\n    if (!this.copierPositionInitialized) {\n      this.copierPositionSnapshot\n        .set(\n          key,\n          nextRow\n        );\n      return;\n    }\n\n    const beforeRow =\n      this.copierPositionSnapshot\n        .get(key);\n\n    const beforeNet =\n      beforeRow\n        ? num(\n            beforeRow.netPos,\n            0\n          )\n        : 0;\n\n    this.copierPositionSnapshot\n      .set(\n        key,\n        nextRow\n      );\n\n    const delta =\n      afterNet -\n      beforeNet;\n\n    if (!delta) {\n      return;\n    }\n\n    const action =\n      delta > 0\n        ? "Buy"\n        : "Sell";\n\n    const quantity =\n      Math.abs(delta);\n\n    console.log(\n      "copier websocket position delta",\n      {\n        connection_id:\n          this.connection.id,\n        provider_account_id:\n          accountId,\n        contract_id:\n          contractId,\n        previous_net_position:\n          beforeNet,\n        current_net_position:\n          afterNet,\n        action,\n        quantity,\n      }\n    );\n\n    this.queuePositionDeltaFallback({\n      id:\n        \`ws-pos-\${accountId}-\${contractId}-\${beforeNet}-\${afterNet}-\${workerReceivedAtMs}\`,\n      accountId,\n      contractId,\n      action,\n      lastQty:\n        quantity,\n      timestamp:\n        asText(\n          position?.timestamp\n        ).trim() ||\n        nowIso(),\n      previousNetPos:\n        beforeNet,\n      currentNetPos:\n        afterNet,\n      rawPosition:\n        position,\n    });\n  }\n\n`;

  source = replaceOnce(
    source,
    `  async scanLeaderPositions(\n`,
    `${insertedMethods}  async scanLeaderPositions(\n`,
    "fill and websocket position handlers"
  );

  const newHandleCopierFrame = `  handleCopierFrame(\n    frame\n  ) {\n    const frameReceivedAtMs =\n      Date.now();\n\n    for (\n      const detail of\n      propsEvents(frame)\n    ) {\n      const entityType =\n        asText(\n          detail?.entityType\n        )\n          .trim()\n          .toLowerCase();\n\n      const eventType =\n        asText(\n          detail?.eventType\n        ).trim();\n\n      const entities =\n        Array.isArray(\n          detail?.entity\n        )\n          ? detail.entity\n          : [\n              detail?.entity,\n            ];\n\n      for (\n        const entity of\n        entities\n      ) {\n        if (\n          !entity ||\n          typeof entity !==\n            "object"\n        ) {\n          continue;\n        }\n\n        if (\n          entity?.contractId\n        ) {\n          this.primeContractName(\n            entity.contractId\n          );\n        }\n\n        if (\n          entityType ===\n          "executionreport"\n        ) {\n          const providerAccountId =\n            normaliseProviderId(\n              entity?.accountId\n            );\n\n          if (\n            !providerAccountId ||\n            !plansForLeader(\n              this.connection.owner_id,\n              this.connection.id,\n              providerAccountId\n            ).length\n          ) {\n            continue;\n          }\n\n          this.detectLeaderExecution({\n            ...entity,\n            workerReceivedAtMs:\n              frameReceivedAtMs,\n          }).catch(error =>\n            console.error(\n              "copier execution handler failed",\n              this.connection.id,\n              error instanceof Error\n                ? error.message\n                : String(error)\n            )\n          );\n\n          continue;\n        }\n\n        if (\n          entityType ===\n          "fill"\n        ) {\n          this.detectLeaderFill(\n            {\n              ...entity,\n              workerReceivedAtMs:\n                frameReceivedAtMs,\n            },\n            eventType ||\n              "fill"\n          ).catch(error =>\n            console.error(\n              "copier fill handler failed",\n              this.connection.id,\n              error instanceof Error\n                ? error.message\n                : String(error)\n            )\n          );\n\n          continue;\n        }\n\n        if (\n          entityType ===\n          "position"\n        ) {\n          try {\n            this.handleLeaderPositionEntity(\n              entity,\n              frameReceivedAtMs\n            );\n          } catch (error) {\n            console.error(\n              "copier position handler failed",\n              this.connection.id,\n              error instanceof Error\n                ? error.message\n                : String(error)\n            );\n          }\n        }\n      }\n    }\n  }\n\n  scheduleLeaderFlatReconcile(`;

  source = replaceRegexOnce(
    source,
    /  handleCopierFrame\(\n    frame\n  \) \{[\s\S]*?\n  \}\n\n  scheduleLeaderFlatReconcile\(/,
    newHandleCopierFrame,
    "copier websocket frame router"
  );

  source = replaceOnce(
    source,
    `detection_mode:\n                          "execution-report-cumulative-quantity-with-position-fallback",`,
    `detection_mode:\n                          "dual-source-executionReport+fill-with-websocket-position-fallback",`,
    "checkpoint detection mode"
  );
}

await writeFile(generatedPath, source, "utf8");

const syntax = spawnSync(
  process.execPath,
  ["--check", generatedPath],
  { stdio: "inherit" }
);

if (syntax.status !== 0) {
  throw new Error("Generated v7.7.21 worker failed node --check");
}

console.log("FRA Prop HQ copier v7.7.21 hotfix generated and syntax-checked");

if (!checkOnly) {
  await import(`${pathToFileURL(generatedPath).href}?hotfix=7.7.21`);
}
