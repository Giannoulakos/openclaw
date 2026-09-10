import { AsyncLocalStorage } from "node:async_hooks";
import { createServer } from "node:http";
import { describe, expect, it, vi } from "vitest";
import { writeOpenAiResponsesText } from "../../test/helpers/openai-responses-sse.js";
import { createDeferred } from "../../test/helpers/promise.js";
import { runQaGatewayFixture } from "../../test/helpers/qa-gateway-cleanup.js";
import * as preparedRuntime from "../agents/prepared-model-runtime.js";
import { createSubagentRunRecord } from "../agents/subagent-test-fixtures.test-helpers.js";
import { subagentRuns } from "../agents/subagents/registry/subagent-registry-memory.js";
import * as restartRecovery from "../agents/subagents/registry/subagent-registry-restart-recovery.js";
import {
  expectReleasedCoreSubagentCandidatePersisted,
  settleSubagentRegistryPersistenceWork,
  writeReleasedCoreSubagentCandidateFixture,
  writeSubagentSessionEntry,
} from "../agents/subagents/registry/subagent-registry.persistence.test-support.js";
import { loadSubagentRegistryFromSqlite } from "../agents/subagents/registry/subagent-registry.store.sqlite.js";
import {
  registerSubagentRun,
  releaseSubagentRun,
  resetSubagentRegistryForTests,
} from "../agents/subagents/registry/subagent-registry.test-helpers.js";
import { loadSessionEntry } from "../config/sessions/session-accessor.js";
import type { OpenClawConfig } from "../config/types.openclaw.js";
import { withTimeout } from "../infra/fs-safe.js";
import { executeSqliteQuerySync, getNodeSqliteKysely } from "../infra/kysely-sync.js";
import type { DB } from "../state/openclaw-state-db.generated.js";
import { openOpenClawStateDatabase } from "../state/openclaw-state-db.js";
import { cancelTaskById, findTaskByRunId, getTaskById } from "../tasks/task-registry.js";
import {
  resetTaskFlowRegistryForTests,
  resetTaskRegistryForTests,
} from "../tasks/task-runtime.test-helpers.js";
import { captureEnv } from "../test-utils/env.js";
import { createOpenClawTestState } from "../test-utils/openclaw-test-state.js";
import { cleanupSessionStateForTest } from "../test-utils/session-state-cleanup.js";
import type { GatewayRecoveryRuntime } from "./server-instance-runtime.types.js";
import { disconnectGatewayClient, startGatewayWithClient } from "./test-helpers.e2e.js";
import { GATEWAY_STARTUP_MUTATED_ENV_KEYS } from "./test-helpers.env.js";
import { buildMockOpenAiResponsesProvider } from "./test-openai-responses-model.js";

function readDurableOwnership() {
  const { db } = openOpenClawStateDatabase();
  const query = getNodeSqliteKysely<Pick<DB, "subagent_runs" | "task_runs" | "flow_runs">>(db);
  return {
    runs: executeSqliteQuerySync(
      db,
      query.selectFrom("subagent_runs").selectAll().orderBy("run_id"),
    ).rows,
    tasks: executeSqliteQuerySync(db, query.selectFrom("task_runs").selectAll().orderBy("task_id"))
      .rows,
    flows: executeSqliteQuerySync(db, query.selectFrom("flow_runs").selectAll().orderBy("flow_id"))
      .rows,
  };
}

describe("released subagent ownership through the real Gateway", () => {
  it.each(["allowed", "cancelled", "retained", "completed", "removed"] as const)(
    "revalidates %s after real runtime preparation",
    { timeout: 90_000 },
    async (mode) => {
      const token = "released-subagent-ownership-token";
      const state = await createOpenClawTestState({
        label: "released-subagent-ownership",
        applyEnv: false,
        env: {
          OPENCLAW_GATEWAY_TOKEN: token,
          OPENCLAW_SKIP_CHANNELS: "1",
          OPENCLAW_SKIP_GMAIL_WATCHER: "1",
          OPENCLAW_SKIP_CRON: "1",
          OPENCLAW_SKIP_CANVAS_HOST: "1",
          OPENCLAW_SKIP_BROWSER_CONTROL_SERVER: "1",
          OPENCLAW_SKIP_PROVIDERS: "1",
          OPENCLAW_DISABLE_BUNDLED_PLUGINS: "1",
        },
      });
      const environment = captureEnv([
        ...Object.keys(state.envVars),
        ...GATEWAY_STARTUP_MUTATED_ENV_KEYS,
      ]);
      const runId = "released-predecessor";
      const successorRunId = "released-new-owner";
      const childSessionKey = "agent:main:subagent:released-owner";
      const sessionId = "released-owner-session";
      const lifecycleRevision = "released-owner-revision";
      const task = "Continue RELEASED_OWNER_WORK after the interrupted turn.";
      const marker = "RELEASED_OWNER_COMPLETED";
      const recoveryPosts: string[] = [];
      const successorPosts: string[] = [];
      const providerWork = new Set<Promise<void>>();
      const providerErrors: unknown[] = [];
      const provider = createServer((request, response) => {
        const work = (async () => {
          if (request.method !== "POST" || request.url !== "/v1/responses") {
            response.writeHead(404).end();
            return;
          }
          const chunks: Buffer[] = [];
          for await (const chunk of request) {
            chunks.push(Buffer.from(chunk));
          }
          const body = Buffer.concat(chunks).toString("utf8");
          if (body.includes("RELEASED_OWNER_WORK")) {
            recoveryPosts.push(body);
          }
          if (body.includes("NEW_OWNER_WORK")) {
            successorPosts.push(body);
          }
          writeOpenAiResponsesText(response, {
            text: marker,
            messageId: "released-owner-message",
            responseId: "released-owner-response",
          });
        })();
        providerWork.add(work);
        void work
          .catch((error: unknown) => {
            providerErrors.push(error);
            if (!response.headersSent) {
              response.writeHead(500);
            }
            response.end();
          })
          .finally(() => providerWork.delete(work));
      });
      const entered = createDeferred<preparedRuntime.PreparedModelRuntimeLease>();
      const release = createDeferred();
      const recoveryScope = new AsyncLocalStorage<string>();
      const originalAcquire = preparedRuntime.acquireAgentRunPreparedModelRuntime;
      const originalRecover = restartRecovery.recoverInterruptedSubagentRow;
      let heldLease: preparedRuntime.PreparedModelRuntimeLease | undefined;
      let heldSignal: AbortSignal | undefined;
      let returnedLease: preparedRuntime.PreparedModelRuntimeLease | undefined;
      let recoveryWork: ReturnType<typeof originalRecover> | undefined;
      let dispatchWork: Promise<unknown> | undefined;
      let runtime: GatewayRecoveryRuntime | undefined;
      let recoveryRunId: string | undefined;
      let terminalWork: Promise<{ status: string }> | undefined;
      let successorTerminal: Promise<{ status: string }> | undefined;
      let cancellation: ReturnType<typeof cancelTaskById> | undefined;
      let starting: ReturnType<typeof startGatewayWithClient> | undefined;
      let gateway: Awaited<ReturnType<typeof startGatewayWithClient>> | undefined;
      let gatewayClosed = false;
      let workJoined = false;
      let providerClosed = false;
      let stateReleased = false;
      const restoreSpies: Array<() => void> = [];
      const joinDispatch = async () => {
        if (!dispatchWork) {
          return;
        }
        const [dispatch] = await Promise.allSettled([dispatchWork]);
        if (
          dispatch.status === "fulfilled" &&
          dispatch.value !== null &&
          typeof dispatch.value === "object" &&
          "status" in dispatch.value &&
          (dispatch.value.status === "accepted" || dispatch.value.status === "in_flight") &&
          runtime &&
          recoveryRunId
        ) {
          terminalWork ??= runtime.waitForAgent<{ status: string }>(
            { runId: recoveryRunId, timeoutMs: 30_000 },
            35_000,
          );
          return await terminalWork;
        }
      };

      await runQaGatewayFixture(
        async () => {
          state.applyEnv();
          resetSubagentRegistryForTests({ persist: false });
          resetTaskRegistryForTests({ persist: false });
          resetTaskFlowRegistryForTests({ persist: false });
          await new Promise<void>((resolve, reject) => {
            provider.once("error", reject);
            provider.listen(0, "127.0.0.1", resolve);
          });
          const address = provider.address();
          if (!address || typeof address === "string") {
            throw new Error("released ownership provider did not bind");
          }
          const model = buildMockOpenAiResponsesProvider(
            `http://127.0.0.1:${address.port}/v1`,
            "gpt-released-owner",
          );
          const cfg = {
            agents: {
              defaults: {
                workspace: state.workspaceDir,
                skipBootstrap: true,
                model: { primary: model.modelRef },
                models: {
                  [model.modelRef]: { params: { transport: "sse", openaiWsWarmup: false } },
                },
              },
              entries: { main: { default: true } },
            },
            models: { mode: "replace", providers: { [model.providerId]: model.config } },
            gateway: { auth: { mode: "token", token } },
            plugins: { slots: { memory: "none" } },
          } satisfies OpenClawConfig;
          await state.writeConfig(cfg);
          const now = Date.now();
          const storePath = await writeSubagentSessionEntry({
            stateDir: state.stateDir,
            agentId: "main",
            sessionKey: childSessionKey,
            sessionId,
            defaultSessionId: sessionId,
            lifecycleRevision,
            updatedAt: now,
            abortedLastRun: true,
          });
          const released = writeReleasedCoreSubagentCandidateFixture(
            createSubagentRunRecord({
              runId,
              childSessionKey,
              task,
              taskRunId: runId,
              generation: 1,
              createdAt: now - 60_000,
              startedAt: now - 55_000,
              spawnMode: "session",
              expectsCompletionMessage: false,
            }),
          );
          expectReleasedCoreSubagentCandidatePersisted(released);
          resetSubagentRegistryForTests({ persist: false });
          resetTaskRegistryForTests({ persist: false });
          resetTaskFlowRegistryForTests({ persist: false });
          await cleanupSessionStateForTest({ stateDir: state.stateDir });

          const acquireSpy = vi
            .spyOn(preparedRuntime, "acquireAgentRunPreparedModelRuntime")
            .mockImplementation(async (...args) => {
              const lease = await originalAcquire(...args);
              if (
                recoveryScope.getStore() === runId &&
                args[0].agentId === "main" &&
                args[0].allowGatewaySubagentBinding === true &&
                !heldLease
              ) {
                // Delay the real producer result only. Production owns every
                // authority check, including the revalidation after this await.
                heldLease = lease;
                heldSignal = args[1]?.abortSignal;
                entered.resolve(lease);
                await release.promise;
                returnedLease = lease;
              }
              return lease;
            });
          restoreSpies.push(() => acquireSpy.mockRestore());
          const recoverySpy = vi
            .spyOn(restartRecovery, "recoverInterruptedSubagentRow")
            .mockImplementation((params) => {
              if (params.runId !== runId || recoveryWork) {
                return originalRecover(params);
              }
              runtime = params.gatewayRuntime;
              if (!runtime) {
                return originalRecover(params);
              }
              const originalDispatch = runtime.dispatchAgent;
              const dispatchSpy = vi.spyOn(runtime, "dispatchAgent").mockImplementation(function <
                T,
              >(...args: Parameters<GatewayRecoveryRuntime["dispatchAgent"]>): Promise<T> {
                const work = originalDispatch<T>(...args);
                if (
                  !dispatchWork &&
                  recoveryScope.getStore() === runId &&
                  args[0].sessionKey === childSessionKey &&
                  args[0].inputProvenance?.sourceTool === "subagent_interrupted_resume"
                ) {
                  recoveryRunId = args[0].idempotencyKey;
                  dispatchWork = work;
                  void work.catch(() => {});
                }
                return work;
              });
              restoreSpies.push(() => dispatchSpy.mockRestore());
              const noticeSpy = vi.spyOn(runtime, "sendRecoveryNotice");
              restoreSpies.push(() => noticeSpy.mockRestore());
              recoveryWork = recoveryScope.run(runId, () => originalRecover(params));
              return recoveryWork;
            });
          restoreSpies.push(() => recoverySpy.mockRestore());
          starting = startGatewayWithClient({
            cfg,
            configPath: state.configPath,
            token,
            clientDisplayName: "released-subagent-ownership",
          });
          void starting.catch(() => {});
          expect(await withTimeout(entered.promise, 30_000, "recovery preparation")).toBe(
            heldLease,
          );
          const predecessor = subagentRuns.get(runId);
          if (!predecessor) {
            throw new Error("Gateway did not hydrate the released predecessor");
          }
          expect(predecessor).not.toBe(released.run);
          expect(predecessor).toMatchObject({
            taskOwnershipPolicy: "core_required",
            requesterAgentId: "main",
            generation: 1,
          });
          expect(loadSubagentRegistryFromSqlite().get(runId)?.taskOwnershipPolicy).toBe(
            "core_required",
          );
          let protectedRows: ReturnType<typeof readDurableOwnership> | undefined;
          let protectedSession: ReturnType<typeof loadSessionEntry>;
          let successorTask: ReturnType<typeof findTaskByRunId>;
          if (!runtime || !heldSignal) {
            throw new Error("real recovery did not publish its Gateway admission");
          }
          if (mode === "cancelled") {
            // The operator is outside recovery's admission context. Public cancel
            // interrupts first, then waits for this preparation before claiming.
            expect(recoveryScope.getStore()).toBeUndefined();
            const interrupted = createDeferred();
            const signal = heldSignal;
            const onInterrupt = () => interrupted.resolve();
            signal.addEventListener("abort", onInterrupt, { once: true });
            restoreSpies.push(() => signal.removeEventListener("abort", onInterrupt));
            cancellation = cancelTaskById({ cfg, taskId: released.task.taskId });
            void cancellation.catch(() => {});
            await withTimeout(interrupted.promise, 10_000, "public cancellation interruption");
            expect(signal.aborted).toBe(true);
          } else if (mode !== "allowed") {
            expect(
              registerSubagentRun({
                runId: successorRunId,
                childSessionKey,
                requesterSessionKey: "agent:main:main",
                requesterDisplayKey: "main",
                task: "Complete NEW_OWNER_WORK.",
                cleanup: "keep",
                spawnMode: "session",
                queued: mode === "retained",
                expectsCompletionMessage: false,
                taskRowOwnership: "required",
              }),
            ).toBeDefined();
            expect(subagentRuns.get(successorRunId)?.generation).toBe(2);
            if (mode !== "retained") {
              await expect(
                runtime.dispatchAgent({
                  sessionKey: childSessionKey,
                  expectedExistingSessionId: sessionId,
                  idempotencyKey: successorRunId,
                  message: "Complete NEW_OWNER_WORK.",
                  deliver: false,
                  lane: "subagent",
                  sessionEffects: "internal",
                  inputProvenance: {
                    kind: "inter_session",
                    sourceSessionKey: "agent:main:main",
                    sourceChannel: "internal",
                    sourceTool: "sessions_spawn",
                  },
                }),
              ).resolves.toMatchObject({ status: "accepted", runId: successorRunId });
              successorTerminal = runtime.waitForAgent<{ status: string }>(
                { runId: successorRunId, timeoutMs: 30_000 },
                35_000,
              );
              await expect(successorTerminal).resolves.toMatchObject({ status: "ok" });
              await vi.waitFor(
                () => {
                  expect(subagentRuns.get(successorRunId)?.cleanupCompletedAt).toBeTypeOf("number");
                  expect(findTaskByRunId(successorRunId)?.status).toBe("succeeded");
                },
                { timeout: 10_000 },
              );
              expect(successorPosts).toHaveLength(1);
              expect(loadSessionEntry({ storePath, sessionKey: childSessionKey })).toMatchObject({
                sessionId,
                status: "done",
                lifecycleRunId: successorRunId,
              });
              if (mode === "removed") {
                releaseSubagentRun(successorRunId);
                expect(loadSubagentRegistryFromSqlite().has(successorRunId)).toBe(false);
              }
            }
            successorTask = findTaskByRunId(successorRunId);
            expect(successorTask?.parentFlowId).toBeDefined();
            protectedRows = readDurableOwnership();
            protectedSession = structuredClone(
              loadSessionEntry({ storePath, sessionKey: childSessionKey }),
            );
          }
          release.resolve();
          gateway = await starting;
          await gateway.server.startupSettled;
          if (!recoveryWork || !dispatchWork || !recoveryRunId) {
            throw new Error("real recovery did not reach Gateway dispatch");
          }
          await recoveryWork;
          const terminal = await joinDispatch();
          if (cancellation) {
            await expect(cancellation).resolves.toMatchObject({ found: true, cancelled: true });
            expect(getTaskById(released.task.taskId)?.status).toBe("cancelled");
            expect(loadSubagentRegistryFromSqlite().get(runId)?.killReconciliation).toMatchObject({
              taskCancellationAccepted: true,
            });
          }
          await settleSubagentRegistryPersistenceWork();
          expect(returnedLease).toBe(heldLease);
          expect(providerErrors).toEqual([]);
          if (mode === "allowed") {
            expect(terminal).toMatchObject({ status: "ok" });
            expect(recoveryPosts).toHaveLength(1);
            const runs = loadSubagentRegistryFromSqlite();
            expect(runs.has(runId)).toBe(false);
            expect(runs.get(recoveryRunId)).toMatchObject({
              generation: 2,
              taskRunId: runId,
              taskOwnershipPolicy: "core_required",
            });
            expect(getTaskById(released.task.taskId)).toMatchObject({
              runId,
              detail: { runtime: "subagent", generation: 2 },
            });
            expect(readDurableOwnership().tasks).toHaveLength(1);
            expect(readDurableOwnership().flows).toHaveLength(1);
          } else {
            // An accepted dispatch must finish before zero POST can prove rejection.
            const [dispatch] = await Promise.allSettled([dispatchWork]);
            if (mode === "cancelled" && dispatch.status === "fulfilled") {
              expect(dispatch.value).toMatchObject({
                status: "timeout",
                summary: "aborted",
                providerStarted: false,
              });
            } else {
              expect(dispatch.status).toBe("rejected");
            }
            expect(recoveryPosts).toHaveLength(0);
            expect(runtime.sendRecoveryNotice).not.toHaveBeenCalled();
            expect(loadSubagentRegistryFromSqlite().has(recoveryRunId)).toBe(false);
            const after = readDurableOwnership();
            if (successorTask && protectedRows) {
              expect(after.tasks.find((row) => row.task_id === successorTask.taskId)).toEqual(
                protectedRows.tasks.find((row) => row.task_id === successorTask.taskId),
              );
              expect(after.flows.find((row) => row.flow_id === successorTask.parentFlowId)).toEqual(
                protectedRows.flows.find((row) => row.flow_id === successorTask.parentFlowId),
              );
              expect(after.runs.find((row) => row.run_id === successorRunId)).toEqual(
                protectedRows.runs.find((row) => row.run_id === successorRunId),
              );
              expect(after.tasks).toHaveLength(protectedRows.tasks.length);
              expect(after.flows).toHaveLength(protectedRows.flows.length);
              expect(loadSessionEntry({ storePath, sessionKey: childSessionKey })).toEqual(
                protectedSession,
              );
            }
          }
        },
        async () => {
          release.resolve();
          if (starting) {
            gateway = await starting;
          }
        },
        () =>
          runQaGatewayFixture(
            async () => {
              await gateway?.server.startupSettled;
            },
            () => recoveryWork,
            () => joinDispatch(),
            () => successorTerminal,
            () => cancellation,
            () => {
              workJoined = true;
            },
          ),
        async () => {
          if (gateway) {
            await disconnectGatewayClient(gateway.client);
          }
        },
        async () => {
          if (gateway) {
            await gateway.server.close();
            gatewayClosed = true;
          } else {
            // Failed startup may retain an unreturned server. No handle means
            // no proof that its producers closed, so keep that fixture state.
            gatewayClosed = starting === undefined;
          }
        },
        () => runQaGatewayFixture(() => Promise.resolve(), ...restoreSpies.toReversed()),
        async () => {
          await Promise.allSettled(providerWork);
          if (provider.listening) {
            provider.closeAllConnections();
            await new Promise<void>((resolve, reject) =>
              provider.close((error) => (error ? reject(error) : resolve())),
            );
          }
          providerClosed = true;
        },
        async () => {
          if (gatewayClosed && workJoined && providerClosed) {
            await settleSubagentRegistryPersistenceWork();
            // This release drains writers/reconciles before closing databases.
            // A failed drain retains state instead of retiring live producers.
            await state.restoreEnv();
            stateReleased = true;
          }
        },
        () => {
          if (stateReleased) {
            resetSubagentRegistryForTests({ persist: false });
            resetTaskRegistryForTests({ persist: false });
            resetTaskFlowRegistryForTests({ persist: false });
          }
        },
        async () => {
          if (stateReleased) {
            await state.cleanup();
          }
        },
        // Restoring selectors does not close databases or remove retained state.
        () => environment.restore(),
      );
    },
  );
});
