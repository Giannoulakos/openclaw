import fs from "node:fs/promises";
import path from "node:path";
import { getGlobalDispatcher, setGlobalDispatcher } from "undici";
import { assert, describe, expect, it } from "vitest";
import { createDiagnosticTraceContext } from "../../../infra/diagnostic-trace-context.js";
import { createDiagnosticEmbeddedRunOwner } from "../../../logging/diagnostic-run-activity.js";
import {
  withCurrentReplyIntegration,
  type CurrentReplyIntegration,
} from "../../current-turn-delivery.integration.test-support.js";
import { readCurrentTurnReplyCompletion } from "../../current-turn-reply-completion.js";
import { guardSessionManager } from "../../session-tool-result-guard-wrapper.js";
import { SessionManager, type AgentSession } from "../../sessions/index.js";
import { isAgentToolReplaySafe } from "../../tool-replay-safety.js";
import { clearToolSearchCatalog, type ToolSearchCatalogToolExecutor } from "../../tool-search.js";
import { clearActiveEmbeddedRun } from "../runs.js";
import { prepareEmbeddedAttemptBundleTools } from "./attempt-bundle-tools.js";
import { createPromptBuildToolPolicy } from "./attempt-prompt-support.js";
import { prepareEmbeddedAttemptAgentSession } from "./attempt-session-prepare.js";
import { prepareEmbeddedAttemptSetup } from "./attempt-setup.js";
import { prepareEmbeddedAttemptStream } from "./attempt-stream-prepare.js";
import { prepareEmbeddedAttemptToolCatalog } from "./attempt-tool-catalog.js";
import { prepareEmbeddedAttemptToolBase } from "./attempt-tool-prepare.js";
import { createEmbeddedAttemptTranscriptLifecycle } from "./attempt-transcript-lifecycle.js";

type PreparedTurn = Awaited<ReturnType<typeof prepareTurn>>;

async function prepareTurn(fixture: CurrentReplyIntegration) {
  const turn = await fixture.createTurn();
  const attempt = turn.attempt;
  const outcomes: string[] = [];
  attempt.onToolOutcome = (outcome) => outcomes.push(outcome.toolName);
  const trace = createDiagnosticTraceContext();
  const setup = await prepareEmbeddedAttemptSetup(attempt);
  const runAbortController = new AbortController();
  const transcriptLifecycle = createEmbeddedAttemptTranscriptLifecycle(attempt);
  let catalogExecutor: ToolSearchCatalogToolExecutor | undefined;
  // Match the attempt's late binding: controls are built before the real session
  // subscription owns nested execution and durable transcript acceptance.
  const executeTool: ToolSearchCatalogToolExecutor = (params) => {
    assert(catalogExecutor, "session subscription must own catalog execution");
    return catalogExecutor(params);
  };
  const base = await prepareEmbeddedAttemptToolBase({
    agentDir: fixture.state.agentDir(),
    attempt,
    setup,
    markCoreToolStage: () => {},
    onYield: () => {},
    runAbortController,
    runTrace: trace,
    skillUsagePaths: undefined,
    skillsSnapshot: undefined,
    codeModeSkills: [],
    toolSearchCatalogExecutor: executeTool,
  });
  let bundle: Awaited<ReturnType<typeof prepareEmbeddedAttemptBundleTools>> | undefined;
  let activeSession: AgentSession | undefined;
  let stream: ReturnType<typeof prepareEmbeddedAttemptStream> | undefined;
  const dispose = async () => {
    fixture.network.releaseAll();
    runAbortController.abort();
    for (const cleanup of base.runCleanups) {
      await cleanup("cancel");
    }
    await fixture.join();
    if (stream) {
      stream.subscription.unsubscribe();
      clearActiveEmbeddedRun(attempt.sessionId, stream.queueHandle, attempt.sessionKey);
    }
    await transcriptLifecycle.beginCleanup();
    activeSession?.dispose();
    await Promise.all([bundle?.bundleMcpRuntime?.dispose(), bundle?.bundleLspRuntime?.dispose()]);
    clearToolSearchCatalog({
      sessionId: attempt.sessionId,
      sessionKey: attempt.sessionKey,
      runId: attempt.runId,
      catalogRef: base.toolSearchCatalogRef,
    });
    await transcriptLifecycle.dispose();
    turn.closeHost();
  };
  try {
    bundle = await prepareEmbeddedAttemptBundleTools({
      agentDir: fixture.state.agentDir(),
      attempt,
      setup,
      isRawModelRun: false,
      preparedToolBase: base,
    });
    const catalog = prepareEmbeddedAttemptToolCatalog({
      attempt,
      setup,
      preparedToolBase: base,
      bundleTools: bundle,
      runTrace: trace,
      abortSignal: runAbortController.signal,
      executeCodeModeTool: executeTool,
    });
    const sessionManager = guardSessionManager(
      SessionManager.open(attempt.sessionTarget!, fixture.state.workspaceDir),
      { runId: attempt.runId },
    );
    sessionManager.appendMessage({ role: "user", content: attempt.prompt, timestamp: 1 });
    const session = await prepareEmbeddedAttemptAgentSession({
      attempt,
      agentCoreThinkingLevel: setup.agentCoreThinkingLevel,
      agentDir: fixture.state.agentDir(),
      clientToolPreparation: {
        catalogToolHookContext: catalog.catalogToolHookContext,
        codeModeControlsEnabledForRun: base.codeModeControlsEnabledForRun,
        deferredDirectoryToolsCallable: catalog.deferredDirectoryToolsCallable,
        effectiveTools: catalog.effectiveTools,
        replaySafetyOptions: base.replaySafetyOptions,
        sandboxEnabled: Boolean(setup.sandbox?.enabled),
        sandboxSessionKey: setup.sandboxSessionKey,
        sessionAgentId: setup.sessionAgentId,
        toolSearchCatalogRef: base.toolSearchCatalogRef,
        toolSearchRuntimeConfig: base.toolSearchRuntimeConfig,
        uncompactedEffectiveTools: bundle.uncompactedEffectiveTools,
        clientTools: bundle.clientTools,
        getToolAbortSignal: () => base.toolAbortSignal,
      },
      effectiveCwd: setup.effectiveCwd,
      getCurrentAttemptPluginMetadataSnapshot: setup.getCurrentAttemptPluginMetadataSnapshot,
      initialSystemPrompt: "Complete the requested work and reply once.",
      markStage: () => {},
      onSessionCreated: (created) => {
        activeSession = created;
      },
      onSystemPromptChanged: () => {},
      runAbortSignal: runAbortController.signal,
      sessionAgentId: setup.sessionAgentId,
      transcriptLifecycle,
      sessionManager,
    });
    const promptPolicy = createPromptBuildToolPolicy({
      session: session.activeSession,
      effectiveTools: catalog.effectiveTools,
      uncompactedEffectiveTools: bundle.uncompactedEffectiveTools,
      tools: bundle.tools,
      catalogRef: base.toolSearchCatalogRef,
      codeModeControlsEnabled: base.codeModeControlsEnabledForRun,
      onApplied: (surface) =>
        catalog.applyPromptToolPolicy(
          new Set([
            ...surface.activeToolNames,
            ...surface.uncompactedEffectiveTools.map((tool) => tool.name),
          ]),
        ),
    });
    stream = prepareEmbeddedAttemptStream({
      attempt,
      applyPermissionMode: (mode, revokeApprovals) => {
        base.refreshPermissionMode(mode, revokeApprovals);
        bundle!.refreshTools();
        catalog.refreshTools();
        session.refreshTools();
        promptPolicy.refresh();
        session.setPermissionPromptPreparation(undefined);
      },
      activeSession: session.activeSession,
      hookRunner: session.hookRunner,
      hookAgentId: setup.sessionAgentId,
      diagnosticTrace: trace,
      clientToolCallSlots: session.clientToolCallSlots,
      nestedToolActivities: base.nestedToolActivities,
      currentTurnReplyCompletion: base.currentTurnReplyCompletion,
      isReplaySafeTool: (tool) => isAgentToolReplaySafe(tool, base.replaySafetyOptions),
      runAbortController,
      abortRun: () => runAbortController.abort(),
      markExternalAbort: () => {},
      getRunState: () => ({
        aborted: runAbortController.signal.aborted,
        promptError: undefined,
        timedOut: false,
        yieldDetected: false,
      }),
      hasDeliveredSourceReply: session.hasDeliveredSourceReply,
      markSourceReplyDelivered: session.markSourceReplyDelivered,
      onBlockReply: undefined,
      onBlockReplyFlush: undefined,
      sandboxSessionKey: setup.sandboxSessionKey,
      builtinToolNames: session.builtinToolNames,
      coreBuiltinToolNames: session.coreBuiltinToolNames,
      replaySafeToolNames: session.replaySafeToolNames,
      codeModeExecToolNames: session.codeModeExecToolNames,
      sideEffectToolOwners: session.sideEffectToolOwners,
      diagnosticOwner: createDiagnosticEmbeddedRunOwner(attempt),
    });
    catalogExecutor = stream.toolSearchCatalogExecutor;
    const refresh = async (mode: "full" | "workspace") => {
      const handle = stream?.queueHandle;
      assert(handle?.applyPermissionMode, "expected the live permission-change entry point");
      expect(attempt.permissionMode).not.toBe(mode);
      const previousSignal = base.toolAbortSignal;
      let approvalsRevoked = false;
      const accepted = await handle.applyPermissionMode(mode, () => {
        expect(previousSignal.aborted).toBe(true);
        approvalsRevoked = true;
      });
      expect(accepted).toBe(true);
      expect(approvalsRevoked).toBe(true);
      expect(attempt.permissionMode).toBe(mode);
      expect(base.toolAbortSignal).not.toBe(previousSignal);
      expect(base.toolAbortSignal.aborted).toBe(false);
    };
    return {
      turn,
      base,
      outcomes,
      refresh,
      dispose,
      control: () => {
        const tool = session.activeSession.agent.state.tools.find((tool) => tool.name === "exec");
        assert(tool, "expected the real session's Code Mode control");
        return tool;
      },
      execute: (id: string, code: string) => {
        const tool = session.activeSession.agent.state.tools.find((tool) => tool.name === "exec");
        assert(tool, "expected the real session's Code Mode control");
        return fixture.track(tool.execute(id, { code }));
      },
    };
  } catch (error) {
    await dispose();
    throw error;
  }
}

async function withPreparedTurn(
  fixture: CurrentReplyIntegration,
  run: (prepared: PreparedTurn) => Promise<void>,
) {
  const dispatcher = getGlobalDispatcher();
  let prepared: PreparedTurn | undefined;
  try {
    prepared = await prepareTurn(fixture);
    await run(prepared);
  } finally {
    await prepared?.dispose();
    const createdDispatcher = getGlobalDispatcher();
    setGlobalDispatcher(dispatcher);
    if (createdDispatcher !== dispatcher) {
      await createdDispatcher.close();
    }
  }
}

describe("embedded current reply across real permission preparation", () => {
  it("allocates a new completion owner for the next genuine turn in the same session", async () => {
    await withCurrentReplyIntegration(async (fixture) => {
      let firstOwner: object | undefined;
      await withPreparedTurn(fixture, async (prepared) => {
        firstOwner = prepared.turn.owner;
        const result = await prepared.execute(
          "first-turn",
          'return await send_current_reply({text:"first turn"});',
        );
        expect(result).toMatchObject({ terminate: true });
        expect(prepared.turn.completion()).toBe("confirmed");
      });
      fixture.network.allowNextTurn();
      await withPreparedTurn(fixture, async (prepared) => {
        expect(prepared.turn.owner).not.toBe(firstOwner);
        const result = await prepared.execute(
          "second-turn",
          'return await send_current_reply({text:"second turn"});',
        );
        expect(result).toMatchObject({ terminate: true });
        expect(prepared.turn.completion()).toBe("confirmed");
        expect(readCurrentTurnReplyCompletion(firstOwner)).toBe("confirmed");
        expect(fixture.network.counts.post).toBe(2);
      });
    });
  });

  it.each(["accepted", "lost response"] as const)(
    "continues a refreshed catalog after %s without replaying the earlier mutation or reply",
    async (acknowledgement) => {
      await withCurrentReplyIntegration(async (fixture) => {
        await withPreparedTurn(fixture, async (prepared) => {
          const held = fixture.network.hold("post");
          if (acknowledgement === "lost response") {
            fixture.network.loseFirstPostResponse();
          }
          const file = path.join(fixture.state.workspaceDir, "mutation.txt");
          const before = prepared.control();
          const pending = prepared.execute(
            "before-refresh",
            `await write({path:${JSON.stringify(file)},content:"once"});
             return await send_current_reply({text:"held reply"});`,
          );
          await held.wait();
          expect(await fs.readFile(file, "utf8")).toBe("once");
          expect(prepared.turn.completion()).toBe("pending");
          const previousSignal = prepared.base.toolAbortSignal;
          await prepared.refresh("workspace");
          expect(previousSignal.aborted).toBe(true);
          expect(prepared.base.toolAbortSignal.aborted).toBe(false);
          expect(prepared.control()).not.toBe(before);
          await pending;

          const second = await prepared.execute(
            "after-refresh",
            'return await send_current_reply({text:"must not send twice"});',
          );
          expect(JSON.stringify(second)).toContain("already been consumed");
          expect(fixture.network.counts.post).toBe(1);
          held.release.resolve();
          await expect.poll(prepared.turn.completion).toBe("ambiguous");

          const continuation = await prepared.execute(
            "ordinary-continuation",
            `return await edit({path:${JSON.stringify(file)},oldText:"once",newText:"continued"});`,
          );
          expect(continuation.details).toMatchObject({ status: "completed" });
          expect(await fs.readFile(file, "utf8")).toBe("continued");
          expect(prepared.outcomes.filter((name) => name === "write")).toHaveLength(1);
          expect(fixture.network.counts.post).toBe(1);
          await expect(before.execute("retained", { code: "return 1;" })).rejects.toThrow();
        });
      });
    },
  );

  it("keeps pre-handoff admission reserved until the old real Slack route lookup settles", async () => {
    await withCurrentReplyIntegration(async (fixture) => {
      await withPreparedTurn(fixture, async (prepared) => {
        const held = fixture.network.hold("route");
        const pending = prepared.execute(
          "held-route",
          'return await send_current_reply({text:"old generation"});',
        );
        await held.wait();
        expect(prepared.turn.completion()).toBeUndefined();
        await prepared.refresh("workspace");
        await pending;
        const refused = await prepared.execute(
          "before-settlement",
          'return await send_current_reply({text:"still reserved"});',
        );
        expect(JSON.stringify(refused)).toContain("already been consumed");
        expect(fixture.network.counts).toMatchObject({ route: 1, post: 0 });

        // The existing inner tool-outcome observer runs after producer settlement,
        // unlike the outer abort race, which already returned during refresh.
        const settledBefore = prepared.outcomes.filter(
          (name) => name === "send_current_reply",
        ).length;
        held.release.resolve();
        await expect
          .poll(() => prepared.outcomes.filter((name) => name === "send_current_reply").length)
          .toBeGreaterThan(settledBefore);
        expect(prepared.turn.completion()).toBeUndefined();
        await prepared.refresh("full");
        const retried = await prepared.execute(
          "authoritative-retry",
          'return await send_current_reply({text:"new generation"});',
        );
        expect(retried).toMatchObject({ terminate: true });
        expect(prepared.turn.completion()).toBe("confirmed");
        expect(fixture.network.counts.post).toBe(1);
      });
    }, "D12345678");
  });
});
