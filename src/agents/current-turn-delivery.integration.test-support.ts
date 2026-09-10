import { randomUUID } from "node:crypto";
import path from "node:path";
import { assert, expect, vi } from "vitest";
import type * as SlackApi from "../../extensions/slack/api.js";
import { createDeferred, withTestTimeout } from "../../test/helpers/promise.js";
import type { OpenClawConfig } from "../config/config.js";
import {
  loadSessionEntryReadOnly,
  replaceSessionEntrySync,
} from "../config/sessions/session-accessor.js";
import {
  mintMessageActionTurnCapability,
  revokeMessageActionTurnCapability,
} from "../gateway/message-action-turn-capability.js";
import * as guardedFetch from "../infra/net/fetch-guard.js";
import type { Model } from "../llm/types.js";
import { resetPluginRuntimeStateForTest, setActivePluginRegistry } from "../plugins/runtime.js";
import { resolveRelativeBundledPluginPublicModuleId } from "../test-utils/bundled-plugin-public-surface.js";
import { createTestRegistry } from "../test-utils/channel-plugins.js";
import {
  withOpenClawTestState,
  type OpenClawTestState,
} from "../test-utils/openclaw-test-state.js";
import {
  closeAdmittedRunDelegatedAuthority,
  createOperationalRunInstanceRef,
  prepareAgentRunAdmission,
} from "./admitted-run-context.js";
import * as toolExecutionState from "./agent-tools.before-tool-call.state.js";
import { createAssistantErrorTranscript } from "./assistant-error-transcript.js";
import {
  closeCurrentTurnReplyCompletionOwner,
  copyCurrentTurnReplyCompletion,
  createCurrentTurnReplyCompletionOwner,
  readCurrentTurnReplyCompletion,
} from "./current-turn-reply-completion.js";
import type { EmbeddedRunAttemptParams } from "./embedded-agent-runner/run/types.js";
import { createAdmittedHostCapabilityTestFixture } from "./harness/host-capability.test-support.js";
import { resolveAgentHarnessCurrentTurnDeliveryTool } from "./harness/host-private-capabilities.js";
import { AuthStorage, ModelRegistry } from "./sessions/index.js";

type SlackClient = ReturnType<typeof SlackApi.getSlackWriteClient>;
type Stage = "blocker" | "route" | "post" | "upload-url" | "dns" | "upload" | "complete";
type Lookup = NonNullable<Parameters<typeof guardedFetch.fetchWithSsrFGuard>[0]["lookupFn"]>;
type Gate = ReturnType<typeof createDeferred<void>>;
type Hold = { entered: Gate; release: Gate };
type SdkQueue = { add<T>(operation: () => Promise<T>): Promise<T> };
type SdkClient = { requestQueue: SdkQueue };
type SdkPrototype = {
  makeRequest(
    this: SlackClient,
    url: URL,
    body: Record<string, unknown>,
    headers?: Record<string, string>,
  ): Promise<unknown>;
};

const model: Model = {
  id: "current-reply-test",
  name: "Current reply test",
  api: "openai-responses",
  provider: "openai",
  baseUrl: "https://model.example.test",
  reasoning: false,
  input: ["text"],
  cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
  contextWindow: 32_768,
  maxTokens: 1024,
};

function response(value: unknown): Response {
  return new Response(JSON.stringify(value), {
    headers: { "content-type": "application/json" },
  });
}

function createSlackHttpEdge() {
  const counts: Record<Stage, number> = {
    blocker: 0,
    route: 0,
    post: 0,
    "upload-url": 0,
    dns: 0,
    upload: 0,
    complete: 0,
  };
  const holds = new Map<Stage, Hold>();
  const requests: Promise<unknown>[] = [];
  let loseFirstPostResponse = false;
  let maxPosts = 1;
  const hold = (stage: Stage) => {
    const gate = { entered: createDeferred<void>(), release: createDeferred<void>() };
    holds.set(stage, gate);
    return {
      ...gate,
      wait: () => withTestTimeout(gate.entered.promise, 5000, `Slack ${stage} was not reached`),
    };
  };
  const enterStage = async (stage: Stage) => {
    counts[stage] += 1;
    if (stage === "post" && counts.post > maxPosts) {
      throw new Error("Unexpected duplicate physical current reply");
    }
    const gate = holds.get(stage);
    // Only the first response is held, so a regression fails instead of parking
    // its second physical request behind the same acknowledgement latch.
    if (gate && (counts[stage] === 1 || stage === "blocker")) {
      gate.entered.resolve();
      await gate.release.promise;
    }
  };
  const lookup: Lookup = (hostname) => {
    const operation = (async () => {
      assert(hostname === "files.slack.com", "unexpected synthetic upload DNS lookup");
      await enterStage("dns");
      return [{ address: "93.184.216.34", family: 4 }];
    })();
    requests.push(operation);
    return operation;
  };
  const fetch: typeof globalThis.fetch = (input, init) => {
    const operation = (async () => {
      const url = new URL(String(input));
      const stage: Stage = url.pathname.endsWith("/api.test")
        ? "blocker"
        : url.pathname.endsWith("/conversations.info")
          ? "route"
          : url.pathname.endsWith("/chat.postMessage")
            ? "post"
            : url.pathname.endsWith("/files.getUploadURLExternal")
              ? "upload-url"
              : url.pathname.endsWith("/files.completeUploadExternal")
                ? "complete"
                : url.hostname === "files.slack.com"
                  ? "upload"
                  : (() => {
                      throw new Error(`Unexpected synthetic Slack HTTP stage: ${url.pathname}`);
                    })();
      if (stage === "upload") {
        assert(init && Reflect.get(init, "dispatcher"), "expected real pinned upload dispatcher");
      }
      await enterStage(stage);
      if (stage === "post" && loseFirstPostResponse && counts.post === 1) {
        throw new TypeError("Synthetic accepted write with lost response");
      }
      if (stage === "route") {
        return response({
          ok: true,
          channel: { id: "D12345678", is_im: true, user: "U12345678" },
        });
      }
      if (stage === "upload-url") {
        return response({
          ok: true,
          upload_url: "https://files.slack.com/upload/current-reply",
          file_id: "F12345678",
        });
      }
      if (stage === "upload") {
        return new Response("ok");
      }
      return response({
        ok: true,
        ts: `171234.${counts.post}`,
        channel: "C12345678",
        files: [{ id: "F12345678" }],
      });
    })();
    requests.push(operation);
    return operation;
  };
  return {
    counts,
    fetch,
    lookup,
    hold,
    allowNextTurn: () => {
      maxPosts += 1;
    },
    loseFirstPostResponse: () => {
      loseFirstPostResponse = true;
    },
    releaseAll: () => {
      for (const gate of holds.values()) {
        gate.release.resolve();
      }
    },
    join: async () => {
      await Promise.allSettled(requests);
    },
  };
}

export type CurrentReplyIntegration = Awaited<ReturnType<typeof createIntegration>>;

async function createIntegration(
  state: OpenClawTestState,
  target: string,
  restoreObservers: Array<() => void>,
) {
  const network = createSlackHttpEdge();
  vi.stubGlobal("fetch", network.fetch);
  const fetchWithSsrFGuard = guardedFetch.fetchWithSsrFGuard;
  const dnsSubstitution = vi
    .spyOn(guardedFetch, "fetchWithSsrFGuard")
    .mockImplementation((params) =>
      fetchWithSsrFGuard({
        ...params,
        lookupFn: network.lookup,
        // A distinct, explicit plain function honors the real guard's dispatcher.
        // A global vi.fn would instead select its hermetic no-DNS test shortcut.
        fetchImpl: (input, init) => network.fetch(input, init),
      }),
    );
  restoreObservers.push(() => dnsSubstitution.mockRestore());
  const slack = await vi.importActual<typeof SlackApi>(
    resolveRelativeBundledPluginPublicModuleId({
      fromModuleUrl: import.meta.url,
      pluginId: "slack",
      artifactBasename: "api.js",
    }),
  );
  const token = `xoxb-test-${randomUUID()}`;
  const config: OpenClawConfig = {
    agents: { defaults: { workspace: state.workspaceDir } },
    channels: { slack: { enabled: true, botToken: token } },
    plugins: { allow: ["slack"], entries: { slack: { enabled: true } } },
    tools: { codeMode: { enabled: true } },
  };
  await state.writeConfig(config);
  setActivePluginRegistry(
    createTestRegistry([{ pluginId: "slack", source: "test", plugin: slack.slackPlugin }]),
  );
  const client = slack.getSlackWriteClient(token);
  const sdkRequests: Promise<unknown>[] = [];
  const admissions = new Map<string, Gate>();
  const prototype = Object.getPrototypeOf(client) as SdkPrototype;
  const original = Reflect.get(prototype, "makeRequest") as SdkPrototype["makeRequest"];
  // Observe the real SDK queue without replacing its admission or transport.
  const observation = vi
    .spyOn(prototype, "makeRequest")
    .mockImplementation(function (this: SlackClient, url, body, headers) {
      const admitted = typeof body.text === "string" ? admissions.get(body.text) : undefined;
      if (admitted) {
        const queue = (this as unknown as SdkClient).requestQueue;
        const add = queue.add.bind(queue);
        queue.add = <T>(operation: () => Promise<T>) => {
          const pending = add(operation);
          admitted.resolve();
          return pending;
        };
      }
      const pending = original.call(this, url, body, headers);
      sdkRequests.push(pending);
      return pending;
    });
  restoreObservers.push(() => observation.mockRestore());
  const sessionId = `current-reply-${randomUUID()}`;
  const sessionKey = `agent:main:slack:channel:${target.toLowerCase()}`;
  const scope = {
    agentId: "main",
    env: state.env,
    sessionId,
    sessionKey,
    storePath: path.join(state.sessionsDir(), "sessions.json"),
  };
  const runs: Array<{
    close: () => void;
    closeAdmission: () => void;
    owner: object;
    token: string;
  }> = [];
  const extraAdmissions: Array<ReturnType<typeof prepareAgentRunAdmission>> = [];
  const executions: Promise<unknown>[] = [];
  const runIds = new Set<string>();
  const producers = new Map<string, { active: number; settled: Gate }>();
  let producerStarts = 0;
  let producerSettlements = 0;
  const recordTracked = toolExecutionState.recordToolExecutionTracked;
  const clearTracked = toolExecutionState.clearTrackedToolExecution;
  // Observe the real wrapper's start/finally pair, inside its outer abort race.
  // Its finally follows source settlement, SDK parsing, and outcome bookkeeping.
  const producerStartObservation = vi
    .spyOn(toolExecutionState, "recordToolExecutionTracked")
    .mockImplementation((toolCallId, runId) => {
      recordTracked(toolCallId, runId);
      if (!runId || !runIds.has(runId)) {
        return;
      }
      const key = `${runId}:${toolCallId}`;
      const pending = producers.get(key);
      producerStarts += 1;
      if (pending) {
        pending.active += 1;
      } else {
        producers.set(key, { active: 1, settled: createDeferred<void>() });
      }
    });
  restoreObservers.push(() => producerStartObservation.mockRestore());
  const producerSettlementObservation = vi
    .spyOn(toolExecutionState, "clearTrackedToolExecution")
    .mockImplementation((toolCallId, runId) => {
      clearTracked(toolCallId, runId);
      const key = `${runId}:${toolCallId}`;
      const pending = producers.get(key);
      if (!pending) {
        return;
      }
      producerSettlements += 1;
      pending.active -= 1;
      if (pending.active === 0) {
        producers.delete(key);
        pending.settled.resolve();
      }
    });
  restoreObservers.push(() => producerSettlementObservation.mockRestore());
  const join = async () => {
    await Promise.allSettled(executions);
    while (producers.size > 0) {
      await Promise.all([...producers.values()].map((producer) => producer.settled.promise));
    }
    await Promise.allSettled(sdkRequests);
    await network.join();
    expect(producerSettlements).toBe(producerStarts);
  };
  let currentRun = 0;
  const readRow = () => loadSessionEntryReadOnly({ ...scope, readConsistency: "latest" });
  const createTurn = async () => {
    const runId = `reply-run-${++currentRun}-${randomUUID()}`;
    runIds.add(runId);
    const lifecycleRevision = "current-reply-lifecycle";
    replaceSessionEntrySync(scope, {
      sessionId,
      updatedAt: currentRun,
      lifecycleRevision,
      activeWriterRunId: runId,
    });
    const turnToken = mintMessageActionTurnCapability({
      agentId: "main",
      runId,
      sessionId,
      sessionKey,
    });
    const assistantErrorTranscript = createAssistantErrorTranscript({ runId, config });
    const owner = createCurrentTurnReplyCompletionOwner();
    // Both native and embedded construction copy this private record, not public
    // tool-result fields. The transcript object remains the actual runtime owner.
    copyCurrentTurnReplyCompletion(owner, assistantErrorTranscript);
    const abortController = new AbortController();
    const sessionTarget = {
      ...scope,
      expectedLifecycleRevision: lifecycleRevision,
      expectedWriterRunId: runId,
    };
    const common = {
      agentId: "main",
      config,
      sessionId,
      sessionKey,
      runId,
      workspaceDir: state.workspaceDir,
      cwd: state.workspaceDir,
      sessionTarget,
      model,
      modelId: model.id,
      provider: model.provider,
      messageChannel: "slack",
      messageProvider: "slack",
      currentChannelId: target,
      currentMessagingTarget: `channel:${target}`,
      messageTo: `channel:${target}`,
      messageActionTurnCapability: turnToken,
      assistantErrorTranscript,
      abortSignal: abortController.signal,
      toolsAllow: ["read", "write", "edit", "send_current_reply", "exec", "wait"],
    };
    const host = await createAdmittedHostCapabilityTestFixture(common);
    runs.push({
      close: host.closeHost,
      closeAdmission: host.closeAdmission,
      owner,
      token: turnToken,
    });
    const authStorage = AuthStorage.inMemory();
    const attempt: EmbeddedRunAttemptParams = {
      ...common,
      admittedRunContext: host.admittedRunContext,
      authStorage,
      authProfileStore: { version: 1, profiles: {} },
      modelRegistry: ModelRegistry.inMemory(authStorage),
      sessionFile: path.join(state.sessionsDir(), `${sessionId}.jsonl`),
      permissionMode: "full",
      thinkLevel: "off",
      timeoutMs: 30_000,
      prompt: "Reply once.",
    };
    const createHostTool = () => {
      const tools = host.hostCapabilities.createToolSurface?.(
        {
          config,
          workspaceDir: state.workspaceDir,
          sessionKey,
          runSessionKey: sessionKey,
          sessionId,
          runId,
          messageProvider: "slack",
          messageTo: `channel:${target}`,
          currentChannelId: target,
          messageActionTurnCapability: turnToken,
          runtimeToolAllowlist: common.toolsAllow,
        },
        undefined,
        { terminalCompletion: "per-result" },
      );
      assert(tools, "expected a real admitted host tool surface");
      const tool = resolveAgentHarnessCurrentTurnDeliveryTool(tools);
      assert(tool, "expected the exact registered host current-reply tool");
      return tool;
    };
    return {
      ...host,
      attempt,
      owner,
      abortController,
      createHostTool,
      completion: () => readCurrentTurnReplyCompletion(owner),
      releaseAuthority: () => {
        expect(closeAdmittedRunDelegatedAuthority(host.admittedRunContext)).toBe(true);
      },
      replaceAdmission: async () => {
        const admission = prepareAgentRunAdmission({
          cfg: config,
          facts: {
            runId,
            agentId: "main",
            ingress: { kind: "system", boundary: "replacement-test", state: "present" },
          },
          operationalRunInstance: createOperationalRunInstanceRef(runId),
        });
        extraAdmissions.push(admission);
        await admission.admit("plugin-harness", `replacement-${runId}`);
      },
      replaceWriter: () => {
        const row = readRow();
        assert(row);
        replaceSessionEntrySync(scope, {
          ...row,
          activeWriterRunId: "successor-writer",
        });
        return readRow();
      },
      replaceLifecycle: () => {
        const row = readRow();
        assert(row);
        replaceSessionEntrySync(scope, {
          ...row,
          lifecycleRevision: "successor-lifecycle",
        });
        return readRow();
      },
    };
  };
  return {
    state,
    config,
    network,
    scope,
    readRow,
    createTurn,
    pendingToolExecutions: () =>
      [...producers.values()].reduce((count, producer) => count + producer.active, 0),
    track: <T>(operation: Promise<T>) => {
      executions.push(operation);
      void operation.catch(() => {});
      return operation;
    },
    observeSdkAdmission: (text: string) => {
      const gate = createDeferred<void>();
      admissions.set(text, gate);
      return () => withTestTimeout(gate.promise, 5000, "Slack SDK admission was not reached");
    },
    occupyTransport: async () => {
      const held = network.hold("blocker");
      // getSlackWriteClient is the cache owner used by the real adapter. Its
      // production default is 100; every slot must be held on this same owner.
      const blockers = Array.from({ length: 100 }, () => client.apiCall("api.test"));
      executions.push(...blockers);
      await expect.poll(() => network.counts.blocker).toBe(100);
      return held.release;
    },
    join,
    dispose: async () => {
      for (const run of runs) {
        run.close();
      }
      network.releaseAll();
      await join();
      for (const run of runs) {
        run.closeAdmission();
        closeCurrentTurnReplyCompletionOwner(run.owner);
        revokeMessageActionTurnCapability(run.token);
      }
      for (const admission of extraAdmissions) {
        admission.close();
      }
    },
  };
}

export async function withCurrentReplyIntegration(
  run: (fixture: CurrentReplyIntegration) => Promise<void>,
  target = "C12345678",
) {
  await withOpenClawTestState(
    {
      label: "current-reply-connected",
      env: {
        HTTP_PROXY: undefined,
        HTTPS_PROXY: undefined,
        ALL_PROXY: undefined,
        http_proxy: undefined,
        https_proxy: undefined,
        all_proxy: undefined,
      },
    },
    async (state) => {
      let fixture: CurrentReplyIntegration | undefined;
      const restoreObservers: Array<() => void> = [];
      try {
        fixture = await createIntegration(state, target, restoreObservers);
        await run(fixture);
      } finally {
        await fixture?.dispose();
        for (const restore of restoreObservers.reverse()) {
          restore();
        }
        resetPluginRuntimeStateForTest();
        vi.unstubAllGlobals();
      }
    },
  );
}
