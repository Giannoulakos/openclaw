import { createHash } from "node:crypto";
import path from "node:path";
import { isRecord } from "@openclaw/normalization-core/record-coerce";
import { stableStringify } from "@openclaw/normalization-core/stable-stringify";
import { createAssistantMessageEventStream } from "openclaw/plugin-sdk/llm";
import type { Static } from "typebox";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { ChatSendParamsSchema } from "../../../packages/gateway-protocol/src/index.js";
import { createDeferred } from "../../../test/helpers/promise.js";
import { useAutoCleanupTempDirTracker } from "../../../test/helpers/temp-dir.js";
import { steerActiveSessionWithOptionalDeliveryWait } from "../../agents/embedded-agent-runner/run/attempt-queue-message.js";
import { resolveAgentQuestionGatewayCall } from "../../agents/harness/gateway-question-dispatch.js";
import { guardSessionManager } from "../../agents/session-tool-result-guard-wrapper.js";
import {
  createAssistant,
  createAssistantResultStream,
  createTestSession,
  registerAgentSessionLoopTestLifecycle,
  streamMocks,
  testModel,
} from "../../agents/sessions/agent-session-loop-correctness.test-support.js";
import type { AgentSessionEvent } from "../../agents/sessions/agent-session-types.js";
import { SessionManager } from "../../agents/sessions/session-manager.js";
import type { dispatchInboundMessage } from "../../auto-reply/dispatch.js";
import {
  beginReplyMessageInjectionTarget,
  createReplyOperation,
  replyRunRegistry,
} from "../../auto-reply/reply/reply-run-registry.js";
import { getRuntimeConfig } from "../../config/config.js";
import {
  appendTranscriptMessage,
  listSessionPendingInputs,
  loadSessionEntry,
  loadTranscriptEventsSync,
  patchSessionEntryCore,
  replaceSessionEntrySync,
} from "../../config/sessions/session-accessor.js";
import {
  resolveSqliteScope,
  toDatabaseOptions,
} from "../../config/sessions/session-accessor.sqlite-scope.js";
import { SessionTranscriptProjectionUnavailableError } from "../../config/sessions/session-transcript-projection-error.js";
import { runExclusiveSessionStoreWrite } from "../../config/sessions/store-writer.js";
import { rotateAgentEventLifecycleGeneration } from "../../infra/agent-events.js";
import { initializeGlobalHookRunner } from "../../plugins/hook-runner-global.js";
import type { PluginHookBeforeMessageWriteEvent } from "../../plugins/types.js";
import { getSessionWorkAdmissionRelease } from "../../sessions/session-lifecycle-admission.js";
import {
  createUserTurnTranscriptRecorder,
  type UserTurnTranscriptRecorder,
} from "../../sessions/user-turn-transcript.js";
import { openOpenClawAgentDatabase } from "../../state/openclaw-agent-db.js";
import { ensureSessionPendingInputsSchema } from "../../state/openclaw-agent-pending-inputs-schema.js";
import { ensureProfileForEmail, linkEmail, setDisplayName } from "../../state/user-profiles.js";
import { createExpectedProfileBinding } from "../expected-profile.js";
import { createMentionInbox } from "../mention-inbox.js";
import { createDirectChatContext } from "../server-chat.agent-events.test-helpers.js";
import { handleGatewayRequest } from "../server-methods.js";
import { PENDING_CHAT_SEND_DEDUPE_PREFIX } from "../server-shared.js";
import {
  dispatchInboundMessageMock,
  installGatewayTestHooks,
  testState,
  writeSessionStore,
} from "../test-helpers.js";
import { getTestPluginRegistry } from "../test-helpers.plugin-registry.js";
import { createWorkerSessionPlacementStore } from "../worker-environments/placement-store.js";
import { admitChatSend } from "./chat-send-admission.js";
import { handleChatSend } from "./chat-send-handler.js";
import { normalizeChatSendRequest } from "./chat-send-request.js";
import { prepareChatSendSession } from "./chat-send-session.js";
import type { GatewayClient, RespondFn } from "./types.js";

installGatewayTestHooks();
registerAgentSessionLoopTestLifecycle();
const temporaryDirs = useAutoCleanupTempDirTracker(afterEach);

describe("ordinary browser input admission", () => {
  async function createBrowserFollowupFixture(
    options: {
      active?: boolean;
      preserveContent?: boolean;
      transientProjectionFailures?: number;
      persistDuringDispatch?: boolean;
    } = {},
  ) {
    const active = options.active !== false;
    const storePath = path.join(temporaryDirs.make("openclaw-chat-custody-"), "sessions.json");
    testState.sessionStorePath = storePath;
    const scope = {
      agentId: "main",
      sessionKey: "agent:main:main",
      sessionId: "cloud-session",
      storePath,
    };
    await writeSessionStore({
      entries: {
        main: {
          sessionId: scope.sessionId,
          updatedAt: Date.now(),
          status: active ? "running" : "done",
        },
        unrelated: {
          sessionId: "unrelated-browser-session",
          updatedAt: Date.now(),
          skillsSnapshot: { prompt: "Unrelated session context. ".repeat(128), skills: [] },
        },
      },
    });
    await appendTranscriptMessage(scope, {
      message: { role: "user", content: "Keep working on the current task.", timestamp: 1 },
    });
    const activeTranscript = loadTranscriptEventsSync(scope);
    const activeRun = active
      ? createReplyOperation({ ...scope, resetTriggered: false })
      : undefined;
    // Cloud workers expose a running owner but explicitly reject message injection.
    activeRun?.attachBackend({
      kind: "embedded",
      runId: "active-cloud-run",
      cancel: vi.fn(),
      messageInjection: { isAvailable: () => false, queueMessage: vi.fn() },
    });
    const approvedContent = "Review the follow-up after the current task.";
    const beforeApprove = vi.fn<(message: PluginHookBeforeMessageWriteEvent["message"]) => void>();
    const registry = getTestPluginRegistry();
    // Hooks disable restart-safe admission, so the idle sibling needs an unhooked fixture.
    if (active) {
      registry.typedHooks.push({
        pluginId: "approved-input-fixture",
        hookName: "before_message_write",
        source: "test",
        handler: ({ message }: PluginHookBeforeMessageWriteEvent) => {
          if (message.role !== "user") {
            return undefined;
          }
          beforeApprove(message);
          return {
            message: options.preserveContent ? message : { ...message, content: approvedContent },
          };
        },
      });
    }
    initializeGlobalHookRunner(registry);
    const dispatchRelease = createDeferred();
    const dispatchedRecorder = createDeferred<UserTurnTranscriptRecorder>();
    // Admission, approval, and SQLite remain real; pause only execution after ACK.
    let dispatchAttempts = 0;
    dispatchInboundMessageMock.mockImplementation(async (dispatchParams: unknown) => {
      const { replyOptions } = dispatchParams as Parameters<typeof dispatchInboundMessage>[0];
      if (replyOptions?.userTurnTranscriptRecorder) {
        dispatchedRecorder.resolve(replyOptions.userTurnTranscriptRecorder);
      }
      dispatchAttempts += 1;
      if (dispatchAttempts <= (options.transientProjectionFailures ?? 0)) {
        throw new SessionTranscriptProjectionUnavailableError(scope.sessionId);
      }
      await dispatchRelease.promise;
      if (options.persistDuringDispatch) {
        if (!replyOptions?.userTurnTranscriptRecorder) {
          throw new Error("Expected dispatch to own the admitted user input");
        }
        await replyOptions.userTurnTranscriptRecorder.persistApproved();
      }
      return {};
    });
    const context = createDirectChatContext({ getRuntimeConfig, chatQueuedTurns: new Map() });
    const client: GatewayClient = {
      connId: "browser-custody-client",
      connect: {
        minProtocol: 1,
        maxProtocol: 1,
        role: "operator",
        scopes: ["operator.read", "operator.write", "operator.admin"],
        client: { id: "openclaw-control-ui", version: "test", platform: "web", mode: "webchat" },
      },
    };
    const params: Static<typeof ChatSendParamsSchema> = {
      sessionKey: scope.sessionKey,
      sessionId: scope.sessionId,
      message: "Raw follow-up awaiting approval.",
      idempotencyKey: "browser-follow-up",
    };
    const send = async (
      respond = vi.fn<RespondFn>(),
      binding?: {
        expectedProfileId?: string;
        signal?: AbortSignal;
        sessionMutationCommitGuard?: () => void;
      },
    ) => {
      const request = {
        req: { type: "req", id: params.idempotencyKey, method: "chat.send", params },
        params,
        client,
        context,
        respond,
        isWebchatConnect: () => true,
      } as const;
      if (binding) {
        await handleGatewayRequest({
          ...request,
          req: { ...request.req, expectedProfileId: binding.expectedProfileId },
          signal: binding.signal,
          sessionMutationCommitGuard: binding.sessionMutationCommitGuard,
          extraHandlers: { "chat.send": handleChatSend },
        });
      } else {
        await handleChatSend(request);
      }
      return respond;
    };
    const finishDispatch = async () => {
      dispatchRelease.resolve();
      activeRun?.complete();
      let settled = false;
      const completion = getSessionWorkAdmissionRelease({
        scope: storePath,
        identities: [scope.sessionKey, scope.sessionId],
      });
      void Promise.resolve(completion).then(() => {
        settled = true;
      });
      await vi.waitFor(() => expect(settled).toBe(true), { timeout: 5_000 });
    };
    return {
      scope,
      context,
      client,
      params,
      approvedContent,
      beforeApprove,
      activeRun,
      activeTranscript,
      send,
      dispatchedRecorder: dispatchedRecorder.promise,
      finishDispatch,
      cleanup: async () => {
        await finishDispatch();
        dispatchInboundMessageMock.mockReset();
      },
    };
  }

  async function createMentionFixture(
    options: { active?: boolean; preserveContent?: boolean } = {},
  ) {
    const fixture = await createBrowserFollowupFixture({ preserveContent: true, ...options });
    const profiles = ["Alice", "Bob", "Carol"].map((name) => {
      const profile = ensureProfileForEmail(`${name.toLowerCase()}@mentions.example.test`);
      setDisplayName(profile.id, name);
      return { profileId: profile.id, displayName: name, hasAvatar: false, updatedAt: 1 };
    });
    const [alice, bob, carol] = profiles;
    if (!alice || !bob || !carol) {
      throw new Error("Mention test profiles were not created");
    }
    fixture.client.authenticatedUserProfile = alice;
    const bobClient = { ...fixture.client, connId: "bob-one", authenticatedUserProfile: bob };
    const carolClient = { ...fixture.client, connId: "carol", authenticatedUserProfile: carol };
    const inbox = createMentionInbox({
      gatewayInstanceId: "chat-mention-commit-test",
      getRuntimeConfig,
      getClients: () => [fixture.client, bobClient, carolClient],
      broadcastToConnIds: vi.fn(),
    });
    fixture.context.mentionInbox = inbox;
    fixture.params.message = "@Bob could you review this?";
    fixture.params.mentions = [{ profileId: bob.profileId, start: 0, end: 4 }];
    const read = (client: GatewayClient = bobClient) => {
      const result = inbox.list(client);
      if (!result.ok) {
        throw new Error(result.error.message);
      }
      return result.value.items;
    };
    return {
      ...fixture,
      bobClient,
      carolClient,
      inbox,
      read,
      cleanup: async () => {
        inbox.dispose();
        await fixture.cleanup();
      },
    };
  }

  it.each(["reservation", "writer"] as const)(
    "rejects a native account merge at %s without accepting or terminalizing input",
    async (boundary) => {
      const fixture = await createBrowserFollowupFixture();
      const email = "native-source@example.test";
      const source = ensureProfileForEmail(email);
      const target = ensureProfileForEmail("native-target@example.test");
      fixture.client.connect.client = {
        id: "openclaw-ios",
        version: "test",
        platform: "ios",
        mode: "ui",
      };
      fixture.client.authenticatedUserProfile = {
        profileId: source.id,
        displayName: null,
        hasAvatar: false,
        updatedAt: source.updatedAt,
      };
      const before = loadSessionEntry(fixture.scope);
      const release = createDeferred();
      let writer: Promise<void> | undefined;
      let request: ReturnType<typeof fixture.send> | undefined;
      try {
        if (boundary === "reservation") {
          const normalized = normalizeChatSendRequest({
            params: fixture.params,
            client: fixture.client,
          });
          if (!normalized.ok) {
            throw new Error(normalized.error);
          }
          const prepared = prepareChatSendSession({
            request: normalized.value,
            client: fixture.client,
            context: fixture.context,
          });
          if (!prepared.ok) {
            throw new Error("Native session preparation failed");
          }
          const binding = createExpectedProfileBinding(source.id, fixture.client)!;
          binding.markInvoked();
          linkEmail(email, target.id);
          await expect(
            admitChatSend({
              request: normalized.value,
              session: prepared.value,
              client: fixture.client,
              context: fixture.context,
              respond: vi.fn(),
              assertCurrent: binding.assertCurrent,
            }),
          ).rejects.toMatchObject({
            error: {
              details: { reason: "EXPECTED_PROFILE_MISMATCH", execution: "may_have_executed" },
            },
          });
          expect(fixture.context.dedupe.size).toBe(0);
        } else {
          const entered = createDeferred();
          writer = runExclusiveSessionStoreWrite(fixture.scope.storePath, async () => {
            entered.resolve();
            await release.promise;
          });
          await entered.promise;
          request = fixture.send(undefined, { expectedProfileId: source.id });
          await vi.waitFor(() =>
            expect(
              fixture.context.dedupe.has(
                `${PENDING_CHAT_SEND_DEDUPE_PREFIX}${fixture.params.idempotencyKey}`,
              ),
            ).toBe(true),
          );
          linkEmail(email, target.id);
          release.resolve();
          await writer;
          const respond = await request;
          expect(respond).toHaveBeenCalledExactlyOnceWith(
            false,
            undefined,
            expect.objectContaining({
              details: { reason: "EXPECTED_PROFILE_MISMATCH", execution: "may_have_executed" },
            }),
          );
        }
        expect(loadSessionEntry(fixture.scope)).toEqual(before);
        expect(listSessionPendingInputs(fixture.scope)).toEqual({ items: [], total: 0 });
        expect(loadTranscriptEventsSync(fixture.scope)).toEqual(fixture.activeTranscript);
        expect(fixture.context.chatAbortControllers.size).toBe(0);
        expect(fixture.context.chatQueuedTurns.size).toBe(0);
        expect(dispatchInboundMessageMock).not.toHaveBeenCalled();
        const cached = fixture.context.dedupe.get(`chat:${fixture.params.idempotencyKey}`);
        expect(cached?.payload).toBeUndefined();
        expect(cached?.error).toBeUndefined();
      } finally {
        release.resolve();
        await writer;
        await request;
        await fixture.cleanup();
      }
    },
  );

  it("terminalizes a native account merge during dispatch transcript approval without rewriting the ACK", async () => {
    const fixture = await createBrowserFollowupFixture({ persistDuringDispatch: true });
    const email = "native-approval@example.test";
    const source = ensureProfileForEmail(email);
    const target = ensureProfileForEmail("native-approval-target@example.test");
    fixture.client.connect.client = {
      id: "openclaw-ios",
      version: "test",
      platform: "ios",
      mode: "ui",
    };
    fixture.client.authenticatedUserProfile = {
      profileId: source.id,
      displayName: null,
      hasAvatar: false,
      updatedAt: source.updatedAt,
    };
    try {
      const ack = await fixture.send(undefined, { expectedProfileId: source.id });
      expect(ack.mock.calls[0]?.[1]).toMatchObject({ status: "started" });
      expect(fixture.beforeApprove).not.toHaveBeenCalled();
      const recorder = await fixture.dispatchedRecorder;
      const acceptedAck = structuredClone(ack.mock.calls);
      fixture.beforeApprove.mockImplementation(() => linkEmail(email, target.id));
      await fixture.finishDispatch();
      expect(fixture.beforeApprove).toHaveBeenCalledOnce();
      expect.soft(recorder.getAdmissionReceipt()).toBeUndefined();
      expect.soft(ack.mock.calls).toEqual(acceptedAck);
      expect.soft(ack).toHaveBeenCalledOnce();
      expect.soft(dispatchInboundMessageMock).toHaveBeenCalledOnce();
      const transcript = loadTranscriptEventsSync(fixture.scope);
      expect
        .soft(transcript.filter((entry) => isRecord(entry) && entry.type === "message"))
        .toEqual(
          fixture.activeTranscript.filter((entry) => isRecord(entry) && entry.type === "message"),
        );
      expect.soft(transcript).toContainEqual(
        expect.objectContaining({
          type: "custom_message",
          customType: "run-failed-before-reply",
          display: true,
          details: expect.objectContaining({ runId: fixture.params.idempotencyKey }),
        }),
      );
      expect.soft(loadSessionEntry(fixture.scope)).toMatchObject({
        status: "failed",
        lastRunId: fixture.params.idempotencyKey,
      });
      expect
        .soft(fixture.context.dedupe.get(`chat:${fixture.params.idempotencyKey}`))
        .toMatchObject({
          ok: false,
          payload: { runId: fixture.params.idempotencyKey, status: "error" },
          error: { message: expect.stringContaining("Selected account changed") },
        });
      expect.soft(fixture.context.broadcast).toHaveBeenCalledWith(
        "chat",
        expect.objectContaining({
          runId: fixture.params.idempotencyKey,
          state: "error",
          errorMessage: expect.stringContaining("Selected account changed"),
        }),
        expect.anything(),
      );
      expect.soft(fixture.context.chatAbortControllers.size).toBe(0);
      expect.soft(fixture.context.chatQueuedTurns.size).toBe(0);
      expect.soft(listSessionPendingInputs(fixture.scope)).toEqual({ items: [], total: 0 });
    } finally {
      await fixture.cleanup();
    }
  });

  it.each(["request-signal abort", "profile merge"] as const)(
    "preserves accepted native input across %s without adopting the retry socket",
    async (change) => {
      const fixture = await createBrowserFollowupFixture();
      const profile = ensureProfileForEmail("native-accepted@example.test");
      fixture.client.connect.client = {
        id: "openclaw-macos",
        version: "test",
        platform: "darwin",
        mode: "ui",
      };
      fixture.client.authenticatedUserProfile = {
        profileId: profile.id,
        displayName: null,
        hasAvatar: false,
        updatedAt: profile.updatedAt,
      };
      const requestAbort = new AbortController();
      try {
        const ack = await fixture.send(undefined, {
          expectedProfileId: profile.id,
          signal: requestAbort.signal,
        });
        expect(ack.mock.calls[0]?.[1]).toMatchObject({ status: "started" });
        const recorder = await fixture.dispatchedRecorder;
        const committed = await recorder.persistApproved();
        expect(committed).toMatchObject({ appended: true });
        const receipt = recorder.getAdmissionReceipt();
        expect(receipt).toBeDefined();
        const accepted = loadTranscriptEventsSync(fixture.scope);
        expect(accepted).toHaveLength(fixture.activeTranscript.length + 1);
        expect(accepted.at(-1)).toMatchObject({
          message: { content: fixture.approvedContent },
        });
        const owner = fixture.context.chatAbortControllers.get(fixture.params.idempotencyKey);
        expect(owner).toBeDefined();
        const ownerConnId = owner?.ownerConnId;
        const cached = structuredClone(
          fixture.context.dedupe.get(`chat:${fixture.params.idempotencyKey}`),
        );
        if (change === "request-signal abort") {
          requestAbort.abort();
        }
        fixture.client.connId = "native-reconnected";
        if (change === "profile merge") {
          const target = ensureProfileForEmail("native-accepted-target@example.test");
          linkEmail("native-accepted@example.test", target.id);
        }
        const retry = await fixture.send(undefined, { expectedProfileId: profile.id });
        if (change === "profile merge") {
          expect(retry).toHaveBeenCalledExactlyOnceWith(
            false,
            undefined,
            expect.objectContaining({
              details: { reason: "EXPECTED_PROFILE_MISMATCH", execution: "not_started" },
            }),
          );
        } else {
          expect(retry.mock.calls[0]?.[1]).toMatchObject({ status: "in_flight" });
        }
        expect(fixture.context.chatAbortControllers.get(fixture.params.idempotencyKey)).toBe(owner);
        expect(owner?.ownerConnId).toBe(ownerConnId);
        expect(owner?.controller.signal.aborted).toBe(false);
        expect(loadTranscriptEventsSync(fixture.scope)).toEqual(accepted);
        expect(recorder.getAdmissionReceipt()).toEqual(receipt);
        expect(fixture.context.dedupe.get(`chat:${fixture.params.idempotencyKey}`)).toEqual(cached);
        expect(await recorder.persistApproved()).toEqual(committed);
        expect(loadTranscriptEventsSync(fixture.scope)).toEqual(accepted);
        expect(recorder.getAdmissionReceipt()).toEqual(receipt);
        expect(listSessionPendingInputs(fixture.scope)).toEqual({ items: [], total: 0 });
        expect(dispatchInboundMessageMock).toHaveBeenCalledOnce();
      } finally {
        await fixture.cleanup();
      }
    },
  );

  it.each(["same profile", "profile merge", "target closed"] as const)(
    "keeps bound native V2 steering on its captured owner after preparation (%s)",
    async (change) => {
      const fixture = await createBrowserFollowupFixture();
      fixture.params.queueMode = "steer";
      const operation = fixture.activeRun;
      if (!operation) {
        throw new Error("Expected the steering fixture to own an active run");
      }
      const email = "native-steering@example.test";
      const profile = ensureProfileForEmail(email);
      const target = ensureProfileForEmail("native-steering-target@example.test");
      fixture.client.connect.client = {
        id: "openclaw-ios",
        version: "test",
        platform: "ios",
        mode: "ui",
      };
      fixture.client.authenticatedUserProfile = {
        profileId: profile.id,
        displayName: null,
        hasAvatar: false,
        updatedAt: profile.updatedAt,
      };
      const fingerprint = "native-steering-tools";
      operation.bindToolAuthoritySnapshot({
        fingerprint: () => fingerprint,
        project: () => fingerprint,
      });
      operation.bindToolAuthorityRoute({ provider: "test-provider", model: "test-model" });
      operation.setPhase("running");
      const entered = createDeferred();
      const release = createDeferred();
      const enqueued = vi.fn();
      const cancel = vi.fn();
      let preparing = false;
      operation.attachBackend({
        kind: "embedded",
        runId: "native-steering-owner",
        toolAuthorityFingerprint: fingerprint,
        cancel,
        messageInjectionV2: {
          version: 2,
          isAvailable: () => true,
          queueMessage: async (text, options, assertCurrent) => {
            preparing = true;
            entered.resolve();
            await release.promise;
            assertCurrent();
            enqueued(text);
            options?.onQueueAccepted?.(true);
          },
        },
      });
      const request = fixture.send(undefined, { expectedProfileId: profile.id });
      try {
        await Promise.race([entered.promise, request]);
        expect(preparing).toBe(true);
        expect(enqueued).not.toHaveBeenCalled();
        expect(dispatchInboundMessageMock).not.toHaveBeenCalled();
        if (change === "profile merge") {
          linkEmail(email, target.id);
        } else if (change === "target closed") {
          operation.complete();
        }
        release.resolve();
        const respond = await request;
        if (change === "same profile") {
          expect(enqueued).toHaveBeenCalledExactlyOnceWith(fixture.params.message);
          expect(respond.mock.calls[0]?.[1]).toMatchObject({ status: "started" });
        } else {
          expect.soft(enqueued).not.toHaveBeenCalled();
          expect.soft(respond.mock.calls[0]?.[0]).toBe(false);
          expect
            .soft(respond.mock.calls[0]?.[1])
            .not.toEqual(expect.objectContaining({ status: "started" }));
          if (change === "profile merge") {
            expect.soft(respond.mock.calls[0]?.[2]).toMatchObject({
              details: { reason: "EXPECTED_PROFILE_MISMATCH", execution: "may_have_executed" },
            });
          }
        }
        expect.soft(dispatchInboundMessageMock).not.toHaveBeenCalled();
        expect.soft(cancel).not.toHaveBeenCalled();
        expect.soft(fixture.client).not.toHaveProperty("invalidated", true);
        if (change !== "target closed") {
          expect.soft(operation.result).toBeNull();
        }
        await fixture.finishDispatch();
        expect.soft(respond).toHaveBeenCalledOnce();
        if (change !== "same profile") {
          expect.soft(loadTranscriptEventsSync(fixture.scope)).toEqual(fixture.activeTranscript);
        }
      } finally {
        release.resolve();
        await request;
        await fixture.cleanup();
      }
    },
  );

  it.each([
    "native fresh",
    "native committed",
    "browser custody",
    "browser custody lifecycle",
  ] as const)(
    "owns the real backing-run outcome when authority changes at steering commit (%s)",
    async (inputState) => {
      const fixture = await createBrowserFollowupFixture({ preserveContent: true });
      const failures = new Set<unknown>();
      let releaseProviders = () => {};
      let backingRun: Promise<void> | undefined;
      try {
        const browserCustody =
          inputState === "browser custody" || inputState === "browser custody lifecycle";
        fixture.params.queueMode = "steer";
        const operation = fixture.activeRun;
        if (!operation) {
          throw new Error("Expected a captured backing run");
        }
        const email = "steering-commit@example.test";
        const profile = ensureProfileForEmail(email);
        const target = ensureProfileForEmail("steering-commit-target@example.test");
        if (!browserCustody) {
          fixture.client.connect.client = {
            id: "openclaw-ios",
            version: "test",
            platform: "ios",
            mode: "ui",
          };
        }
        fixture.client.authenticatedUserProfile = {
          profileId: profile.id,
          displayName: null,
          hasAvatar: false,
          updatedAt: profile.updatedAt,
        };
        const sessionManager = SessionManager.open(
          fixture.scope,
          path.dirname(fixture.scope.storePath),
        );
        const inputKey = `${fixture.params.idempotencyKey}:user`;
        let recorder: UserTurnTranscriptRecorder | undefined;
        let initialRuntimeReceipt: ReturnType<UserTurnTranscriptRecorder["getAdmissionReceipt"]>;
        guardSessionManager(sessionManager, {
          agentId: fixture.scope.agentId,
          sessionKey: fixture.scope.sessionKey,
          runId: "native-backing-run",
          onUserMessagePersisted: (message) => {
            if ("idempotencyKey" in message && message.idempotencyKey === inputKey) {
              initialRuntimeReceipt = structuredClone(recorder?.getAdmissionReceipt());
            }
          },
        });
        const { session } = await createTestSession({ sessionManager });
        const terminals: AgentSessionEvent[] = [];
        session.subscribe((event) => {
          if (event.type === "agent_end" || event.type === "agent_settled") {
            terminals.push(event);
          }
        });
        const firstResponse = createAssistantMessageEventStream();
        const secondResponse = createAssistantMessageEventStream();
        streamMocks.streamSimple
          .mockImplementationOnce(() => firstResponse)
          .mockImplementation((model) =>
            inputState === "native committed"
              ? secondResponse
              : createAssistantResultStream(
                  createAssistant(model, [{ type: "text", text: "accepted steering completed" }]),
                ),
          );
        let released = false;
        const finishProvider = () => {
          if (released) {
            return;
          }
          released = true;
          firstResponse.push({
            type: "done",
            reason: "stop",
            message: createAssistant(testModel, [{ type: "text", text: "backing answer" }]),
          });
          firstResponse.end();
        };
        let secondReleased = false;
        const finishSteeringProvider = () => {
          if (secondReleased) {
            return;
          }
          secondReleased = true;
          secondResponse.push({
            type: "done",
            reason: "stop",
            message: createAssistant(testModel, [
              { type: "text", text: "accepted steering completed" },
            ]),
          });
          secondResponse.end();
        };
        releaseProviders = () => {
          finishProvider();
          finishSteeringProvider();
        };
        backingRun = session.prompt("Continue the original backing work.");
        const cancel = vi.fn();
        const fingerprint = "steering-commit-tools";
        operation.bindToolAuthoritySnapshot({
          fingerprint: () => fingerprint,
          project: () => fingerprint,
        });
        operation.bindToolAuthorityRoute({ provider: "test-provider", model: "test-model" });
        operation.setPhase("running");
        operation.attachBackend({
          kind: "embedded",
          runId: "native-backing-run",
          toolAuthorityFingerprint: fingerprint,
          cancel,
          messageInjectionV2: {
            version: 2,
            isAvailable: () => true,
            queueMessage: async (text, options, assertCurrent) => {
              recorder = options?.userTurnTranscriptRecorder;
              return await steerActiveSessionWithOptionalDeliveryWait(
                session,
                text,
                options,
                fixture.scope.sessionKey,
                () => {
                  assertCurrent();
                  return true;
                },
              );
            },
          },
        });
        await vi.waitFor(() => expect(streamMocks.streamSimple).toHaveBeenCalledOnce());
        const ack = await fixture.send(undefined, { expectedProfileId: profile.id });
        expect(ack.mock.calls[0]?.[1]).toMatchObject({ status: "started" });
        expect(ack).toHaveBeenCalledOnce();
        const originalAck = structuredClone(ack.mock.calls);
        expect(recorder).toBeDefined();
        if (!recorder) {
          throw new Error("The backing runtime did not capture its input recorder");
        }
        expect(recorder.getAdmissionReceipt()).toBeUndefined();
        const persistFallback = vi.spyOn(recorder, "persistFallback");
        const pending = listSessionPendingInputs(fixture.scope);
        expect(pending.total).toBe(browserCustody ? 1 : 0);
        fixture.beforeApprove.mockClear();
        if (inputState === "browser custody") {
          linkEmail(email, target.id);
        } else if (inputState === "browser custody lifecycle") {
          expect(session.getSteeringMessages()).toEqual([fixture.params.message]);
          expect(session.isStreaming).toBe(true);
          expect(released).toBe(false);
          expect(pending.items[0]).toMatchObject({
            runId: fixture.params.idempotencyKey,
            state: "queued",
            message: { idempotencyKey: inputKey, content: fixture.params.message },
          });
          expect(dispatchInboundMessageMock).not.toHaveBeenCalled();
          rotateAgentEventLifecycleGeneration();
          expect(cancel).toHaveBeenCalledExactlyOnceWith("restart");
          expect(operation.result).toMatchObject({
            kind: "aborted",
            code: "aborted_for_restart",
          });
        } else if (inputState === "native fresh") {
          fixture.beforeApprove.mockImplementation(() => linkEmail(email, target.id));
        }
        finishProvider();
        if (inputState === "native committed") {
          await vi.waitFor(() => expect(initialRuntimeReceipt).toBeDefined());
          // Source finalization confirms steering metadata after the runtime commit.
          // Keep the backing provider held until that final receipt is observable.
          await vi.waitFor(() =>
            expect(
              fixture.context.dedupe.get(`chat:${fixture.params.idempotencyKey}`),
            ).toMatchObject({
              ok: true,
              payload: { runId: fixture.params.idempotencyKey, status: "ok" },
            }),
          );
          const finalReceipt = structuredClone(recorder.getAdmissionReceipt());
          const committedTranscript = loadTranscriptEventsSync(fixture.scope);
          expect(finalReceipt).toBeDefined();
          expect(operation.result).toBeNull();
          expect(session.isStreaming).toBe(true);
          expect(fixture.beforeApprove).toHaveBeenCalledOnce();
          linkEmail(email, target.id);
          expect(await recorder.persistApproved()).toBeUndefined();
          expect(recorder.getAdmissionReceipt()).toEqual(finalReceipt);
          expect(loadTranscriptEventsSync(fixture.scope)).toEqual(committedTranscript);
          expect(fixture.beforeApprove).toHaveBeenCalledOnce();
          finishSteeringProvider();
        }
        await backingRun;
        await fixture.finishDispatch();
        if (inputState === "browser custody lifecycle") {
          const transcript = loadTranscriptEventsSync(fixture.scope);
          const sourceErrors = vi
            .mocked(fixture.context.broadcast)
            .mock.calls.filter(
              ([event, payload]) =>
                event === "chat" &&
                isRecord(payload) &&
                payload.runId === fixture.params.idempotencyKey &&
                payload.state === "error",
            )
            .map(([, payload]) => payload);
          // Observe the full terminal result even when fallback wrongly dispatches:
          // the original ACK, backing runtime, custody, and source cleanup stay distinct.
          expect({
            ack: ack.mock.calls,
            freshDispatchCalls: dispatchInboundMessageMock.mock.calls.length,
            cancellationCalls: cancel.mock.calls,
            inputHooks: fixture.beforeApprove.mock.calls.length,
            providerCalls: streamMocks.streamSimple.mock.calls.length,
            streaming: session.isStreaming,
            steering: session.getSteeringMessages(),
            terminalEvents: terminals.map((event) => event.type),
            backingTerminal: session.messages.at(-1),
            originalInputs: transcript.filter(
              (entry) =>
                isRecord(entry) &&
                entry.type === "message" &&
                isRecord(entry.message) &&
                entry.message.idempotencyKey === inputKey,
            ),
            receipt: recorder.getAdmissionReceipt(),
            sourceTerminal: fixture.context.dedupe.get(`chat:${fixture.params.idempotencyKey}`),
            sourceErrors,
            pendingInputs: listSessionPendingInputs(fixture.scope),
            abortOwners: fixture.context.chatAbortControllers.size,
            queuedTurns: fixture.context.chatQueuedTurns.size,
          }).toMatchObject({
            ack: originalAck,
            freshDispatchCalls: 0,
            cancellationCalls: [["restart"]],
            inputHooks: 0,
            providerCalls: 1,
            streaming: false,
            steering: [],
            terminalEvents: ["agent_end", "agent_settled"],
            backingTerminal: {
              role: "assistant",
              stopReason: "error",
              errorMessage: expect.stringContaining("Pending input ownership ended"),
            },
            originalInputs: [],
            receipt: undefined,
            sourceTerminal: {
              ok: false,
              payload: { runId: fixture.params.idempotencyKey, status: "error" },
              error: { message: expect.stringContaining("Pending input ownership ended") },
            },
            sourceErrors: [
              {
                runId: fixture.params.idempotencyKey,
                state: "error",
                errorMessage: expect.stringContaining("Pending input ownership ended"),
              },
            ],
            pendingInputs: {
              total: 1,
              items: [{ ...pending.items[0], state: "interrupted" }],
            },
            abortOwners: 0,
            queuedTurns: 0,
          });
          expect(transcript).toContainEqual(
            expect.objectContaining({
              type: "message",
              message: expect.objectContaining({
                role: "assistant",
                stopReason: "error",
                __openclaw: expect.objectContaining({ runId: "native-backing-run" }),
              }),
            }),
          );
        } else {
          const refused = inputState === "native fresh";
          expect(
            fixture.beforeApprove.mock.calls.map(([message]) =>
              "idempotencyKey" in message ? message.idempotencyKey : undefined,
            ),
          ).toEqual(
            refused ? [inputKey, inputKey] : inputState === "browser custody" ? [] : [inputKey],
          );
          if (refused) {
            expect(persistFallback).toHaveBeenCalledOnce();
          }
          expect(ack.mock.calls).toEqual(originalAck);
          expect(dispatchInboundMessageMock).not.toHaveBeenCalled();
          expect(cancel).not.toHaveBeenCalled();
          expect(session.isStreaming).toBe(false);
          expect(session.getSteeringMessages()).toEqual([]);
          expect(terminals.filter((event) => event.type === "agent_end")).toHaveLength(1);
          expect(terminals.filter((event) => event.type === "agent_settled")).toHaveLength(1);
          expect(streamMocks.streamSimple).toHaveBeenCalledTimes(refused ? 1 : 2);
          const transcript = loadTranscriptEventsSync(fixture.scope);
          const originalInputs = transcript.filter(
            (entry) =>
              isRecord(entry) &&
              entry.type === "message" &&
              isRecord(entry.message) &&
              entry.message.idempotencyKey === `${fixture.params.idempotencyKey}:user`,
          );
          expect(originalInputs).toHaveLength(refused ? 0 : 1);
          if (refused) {
            // Agent-core commits its memory queue before persistence listeners.
            // Refusal must produce the existing backing-run error, not pretend it continued.
            expect(session.messages).toContainEqual(
              expect.objectContaining({
                role: "user",
                content: expect.arrayContaining([
                  expect.objectContaining({ type: "text", text: fixture.params.message }),
                ]),
              }),
            );
            expect(session.messages.at(-1)).toMatchObject({
              role: "assistant",
              stopReason: "error",
              errorMessage: expect.any(String),
            });
            expect(transcript).toContainEqual(
              expect.objectContaining({
                type: "message",
                message: expect.objectContaining({
                  role: "assistant",
                  stopReason: "error",
                  __openclaw: expect.objectContaining({ runId: "native-backing-run" }),
                }),
              }),
            );
            expect(recorder.getAdmissionReceipt()).toBeUndefined();
            expect(fixture.context.broadcast).toHaveBeenCalledWith(
              "chat",
              expect.objectContaining({
                runId: fixture.params.idempotencyKey,
                state: "error",
                errorMessage: expect.stringContaining("Selected account changed"),
              }),
              expect.anything(),
            );
          } else {
            expect(session.messages.at(-1)).toMatchObject({
              role: "assistant",
              stopReason: "stop",
            });
            expect(recorder.getAdmissionReceipt()).toBeDefined();
          }
          const cached = structuredClone(
            fixture.context.dedupe.get(`chat:${fixture.params.idempotencyKey}`),
          );
          expect(cached).toMatchObject({
            ok: !refused,
            payload: { runId: fixture.params.idempotencyKey, status: refused ? "error" : "ok" },
          });
          const staleRetry = await fixture.send(undefined, { expectedProfileId: profile.id });
          expect(staleRetry).toHaveBeenCalledExactlyOnceWith(
            false,
            undefined,
            expect.objectContaining({
              details: { reason: "EXPECTED_PROFILE_MISMATCH", execution: "not_started" },
            }),
          );
          expect(fixture.context.dedupe.get(`chat:${fixture.params.idempotencyKey}`)).toEqual(
            cached,
          );
          expect(loadTranscriptEventsSync(fixture.scope)).toEqual(transcript);
          expect(fixture.context.chatAbortControllers.size).toBe(0);
          expect(fixture.context.chatQueuedTurns.size).toBe(0);
          expect(listSessionPendingInputs(fixture.scope)).toEqual({ items: [], total: 0 });
        }
      } catch (error) {
        failures.add(error);
      } finally {
        releaseProviders();
        try {
          await backingRun;
        } catch (error) {
          failures.add(error);
        } finally {
          try {
            await fixture.cleanup();
          } catch (error) {
            failures.add(error);
          }
        }
      }
      if (failures.size === 1) {
        throw failures.values().next().value;
      }
      if (failures.size > 1) {
        throw new AggregateError(failures, "Backing-run fixture and cleanup failed", {
          cause: failures.values().next().value,
        });
      }
    },
  );

  it.each(
    (["backend", "question dispatcher"] as const).flatMap((sink) =>
      [false, true].map((bound) => ({ sink, bound })),
    ),
  )("keeps V1 steering opt-in at the $sink boundary (bound: $bound)", async ({ sink, bound }) => {
    const fixture = await createBrowserFollowupFixture({ preserveContent: true });
    try {
      fixture.params.queueMode = "steer";
      fixture.client.connect.client = {
        id: "openclaw-ios",
        version: "test",
        platform: "ios",
        mode: "ui",
      };
      const profile = ensureProfileForEmail("v1-steering@example.test");
      fixture.client.authenticatedUserProfile = {
        profileId: profile.id,
        displayName: null,
        hasAvatar: false,
        updatedAt: profile.updatedAt,
      };
      const operation = fixture.activeRun!;
      const fingerprint = "v1-steering-tools";
      operation.bindToolAuthoritySnapshot({
        fingerprint: () => fingerprint,
        project: () => fingerprint,
      });
      operation.bindToolAuthorityRoute({ provider: "test-provider", model: "test-model" });
      operation.setPhase("running");
      const write = vi.fn(async () => undefined);
      const questionCall = resolveAgentQuestionGatewayCall(write);
      const cancel = vi.fn();
      operation.attachBackend({
        kind: "embedded",
        runId: "v1-backing-run",
        toolAuthorityFingerprint: fingerprint,
        cancel,
        ...(sink === "backend"
          ? {
              messageInjection: {
                isAvailable: () => true,
                queueMessage: async (_text, options) => {
                  await write();
                  await options?.userTurnTranscriptRecorder?.persistApproved();
                },
              },
            }
          : {
              messageInjectionV2: {
                version: 2,
                isAvailable: () => true,
                queueMessage: async (_text, options, assertCurrent, kind) => {
                  await questionCall(
                    "question.resolve",
                    {},
                    {},
                    {
                      dispatchAuthority: { version: 2, kind, assertCurrent },
                    },
                  );
                  await options?.userTurnTranscriptRecorder?.persistApproved();
                },
              },
            }),
      });
      const respond = await fixture.send(undefined, {
        expectedProfileId: bound ? profile.id : undefined,
      });
      expect(operation.result).toBeNull();
      await fixture.finishDispatch();
      expect(write).toHaveBeenCalledTimes(bound ? 0 : 1);
      expect(respond).toHaveBeenCalledOnce();
      expect(respond.mock.calls[0]?.[0]).toBe(!bound);
      expect(dispatchInboundMessageMock).not.toHaveBeenCalled();
      expect(cancel).not.toHaveBeenCalled();
      if (bound) {
        expect(loadTranscriptEventsSync(fixture.scope)).toEqual(fixture.activeTranscript);
      } else {
        expect(respond.mock.calls[0]?.[1]).toMatchObject({ status: "started" });
      }
    } finally {
      await fixture.cleanup();
    }
  });

  it.each(
    (["queue", "claim", "cancel"] as const).flatMap((sink) =>
      [false, true].map((bound) => ({ sink, bound })),
    ),
  )(
    "fences deferred custom question I/O through registry $sink (bound: $bound)",
    async ({ sink, bound }) => {
      const fixture = await createBrowserFollowupFixture();
      const entered = createDeferred();
      const release = createDeferred();
      let outcome: Promise<unknown> | undefined;
      try {
        const email = "deferred-source@example.test";
        const profile = ensureProfileForEmail(email);
        const successor = ensureProfileForEmail("deferred-successor@example.test");
        fixture.client.authenticatedUserProfile = {
          profileId: profile.id,
          displayName: null,
          hasAvatar: false,
          updatedAt: profile.updatedAt,
        };
        const binding = createExpectedProfileBinding(profile.id, fixture.client)!;
        const write = vi.fn();
        const questionCall = resolveAgentQuestionGatewayCall({
          version: 2,
          call: async ({ authority }) => {
            entered.resolve();
            await release.promise;
            if (authority.kind === "source-bound") {
              authority.assertCurrent();
            }
            write();
            return {};
          },
        });
        const dispatch = async (assertCurrent: () => void, kind: "run" | "source-bound") => {
          await questionCall(
            "question.resolve",
            {},
            {},
            {
              dispatchAuthority: { version: 2, kind, assertCurrent },
            },
          );
          return true;
        };
        const cancelBacking = vi.fn();
        const operation = fixture.activeRun!;
        operation.setPhase("running");
        operation.attachBackend({
          kind: "embedded",
          runId: "deferred-backing-run",
          toolAuthorityFingerprint: "active-tools",
          cancel: cancelBacking,
          messageInjectionV2: {
            version: 2,
            isAvailable: () => true,
            queueMessage: async (_text, _options, assertCurrent, kind) => {
              await dispatch(assertCurrent, kind);
            },
            claimPendingUserInputAnswer: async (_text, _options, assertCurrent, kind) =>
              dispatch(assertCurrent, kind),
            cancelPendingUserInput: async (_resolvedBy, assertCurrent, kind) =>
              dispatch(assertCurrent, kind),
          },
        });
        const target = replyRunRegistry.resolveCurrentMessageInjectionTarget(
          fixture.scope.sessionKey,
        )!;
        const attempt = beginReplyMessageInjectionTarget(target, "answer", {
          isInboundUserMessage: true,
          toolAuthorityFingerprint: sink === "claim" ? "other-tools" : "active-tools",
          pendingInputAuthorityFingerprint: "active-tools",
          ...(sink === "cancel"
            ? { images: [{ type: "image", data: "aW1hZ2U=", mimeType: "image/png" }] }
            : {}),
          ...(bound ? { assertCurrent: binding.assertCurrent } : {}),
        });
        outcome = attempt.outcome.catch((error: unknown) => error);
        await Promise.race([entered.promise, outcome]);
        expect(write).not.toHaveBeenCalled();
        linkEmail(email, successor.id);
        release.resolve();
        const result = await outcome;
        expect(write).toHaveBeenCalledTimes(bound ? 0 : 1);
        if (bound) {
          if (sink === "cancel") {
            expect(result).toMatchObject({ name: "MessageInjectionAuthorityError" });
          } else {
            expect(result).toMatchObject({
              status: "failed",
              error: expect.objectContaining({
                error: expect.objectContaining({
                  details: { reason: "EXPECTED_PROFILE_MISMATCH", execution: "not_started" },
                }),
              }),
            });
          }
        } else {
          expect(result).toMatchObject({ status: sink === "cancel" ? "rejected" : "accepted" });
        }
        expect(cancelBacking).not.toHaveBeenCalled();
        expect(operation.result).toBeNull();
        expect(loadTranscriptEventsSync(fixture.scope)).toEqual(fixture.activeTranscript);
      } finally {
        release.resolve();
        try {
          await outcome;
        } finally {
          await fixture.cleanup();
        }
      }
    },
  );

  it.each(["host", "session ACL", "lifecycle"] as const)(
    "retains original %s authority after committed browser custody and profile merge",
    async (boundary) => {
      const fixture = await createBrowserFollowupFixture({
        preserveContent: true,
        persistDuringDispatch: true,
      });
      let hostCurrent = true;
      try {
        const email = "retained-authority@example.test";
        const profile = ensureProfileForEmail(email);
        const target = ensureProfileForEmail("retained-authority-target@example.test");
        fixture.client.authenticatedUserProfile = {
          profileId: profile.id,
          displayName: null,
          hasAvatar: false,
          updatedAt: profile.updatedAt,
        };
        if (boundary === "session ACL") {
          fixture.client.connect.scopes = ["operator.read", "operator.write"];
        }
        const ack = await fixture.send(undefined, {
          expectedProfileId: profile.id,
          sessionMutationCommitGuard: () => {
            if (!hostCurrent) {
              throw new Error("The original input host has closed.");
            }
          },
        });
        expect(ack).toHaveBeenCalledOnce();
        expect(ack.mock.calls[0]?.[1]).toMatchObject({ status: "started" });
        const originalAck = structuredClone(ack.mock.calls);
        const pending = listSessionPendingInputs(fixture.scope);
        expect(pending).toMatchObject({
          total: 1,
          items: [{ state: "queued", message: { content: fixture.params.message } }],
        });
        await fixture.dispatchedRecorder;
        linkEmail(email, target.id);
        if (boundary === "host") {
          hostCurrent = false;
        } else if (boundary === "session ACL") {
          await patchSessionEntryCore(fixture.scope, () => ({ visibility: "draft" }));
        } else {
          rotateAgentEventLifecycleGeneration();
        }
        await fixture.finishDispatch();
        expect(ack.mock.calls).toEqual(originalAck);
        expect(dispatchInboundMessageMock).toHaveBeenCalledOnce();
        expect(
          loadTranscriptEventsSync(fixture.scope).filter(
            (entry) =>
              isRecord(entry) &&
              entry.type === "message" &&
              isRecord(entry.message) &&
              entry.message.idempotencyKey === `${fixture.params.idempotencyKey}:user`,
          ),
        ).toEqual([]);
        expect(listSessionPendingInputs(fixture.scope)).toMatchObject({
          total: 1,
          items: [
            {
              id: pending.items[0]?.id,
              state: "interrupted",
              message: pending.items[0]?.message,
            },
          ],
        });
        const cached = structuredClone(
          fixture.context.dedupe.get(`chat:${fixture.params.idempotencyKey}`),
        );
        expect(cached).toMatchObject({
          ok: false,
          payload: { runId: fixture.params.idempotencyKey, status: "error" },
        });
        expect(fixture.context.broadcast).toHaveBeenCalledWith(
          "chat",
          expect.objectContaining({
            runId: fixture.params.idempotencyKey,
            state: "error",
            errorMessage: expect.any(String),
          }),
          expect.anything(),
        );
        const retry = await fixture.send(undefined, { expectedProfileId: profile.id });
        expect(retry).toHaveBeenCalledExactlyOnceWith(
          false,
          undefined,
          expect.objectContaining({
            details: { reason: "EXPECTED_PROFILE_MISMATCH", execution: "not_started" },
          }),
        );
        expect(fixture.context.dedupe.get(`chat:${fixture.params.idempotencyKey}`)).toEqual(cached);
        expect(fixture.context.chatAbortControllers.size).toBe(0);
        expect(fixture.context.chatQueuedTurns.size).toBe(0);
      } finally {
        await fixture.cleanup();
      }
    },
  );

  it("creates recipient-only mentions at original message commit, never at the queued ACK", async () => {
    const fixture = await createMentionFixture();
    try {
      const ack = await fixture.send();
      expect(ack).toHaveBeenCalledWith(
        true,
        expect.objectContaining({ status: "started" }),
        undefined,
        expect.anything(),
      );
      expect(fixture.read()).toEqual([]);
      const recorder = await fixture.dispatchedRecorder;
      const committed = await recorder.persistApproved();
      expect(committed?.appended).toBe(true);
      expect(fixture.read()).toMatchObject([
        {
          messageId: committed?.messageId,
          senderProfileId: fixture.client.authenticatedUserProfile?.profileId,
          excerpt: fixture.params.message,
        },
      ]);
      expect(fixture.read(fixture.client)).toEqual([]);
      expect(fixture.read(fixture.carolClient)).toEqual([]);
      const id = fixture.read()[0]?.id;
      expect(id).toBeDefined();
      fixture.inbox.dismiss(fixture.bobClient, id ? [id] : []);
      await recorder.persistApproved();
      await fixture.send();
      expect(fixture.read()).toEqual([]);
    } finally {
      await fixture.cleanup();
    }
  });

  it("includes an idle first commit in the Inbox before ACK without waiting for the agent", async () => {
    const fixture = await createMentionFixture({ active: false });
    let atAck = 0;
    try {
      const ack = await fixture.send(
        vi.fn((ok) => {
          if (ok) {
            atAck = fixture.read().length;
          }
        }),
      );
      expect(ack).toHaveBeenCalledWith(
        true,
        expect.objectContaining({ status: "started", messageSeq: 2 }),
        undefined,
        expect.anything(),
      );
      expect(atAck).toBe(1);
      await fixture.finishDispatch();
      expect(fixture.read()).toHaveLength(1);
    } finally {
      await fixture.cleanup();
    }
  });

  it("does not notify when approval replaces the selected token", async () => {
    const fixture = await createMentionFixture({ preserveContent: false });
    try {
      await fixture.send();
      const recorder = await fixture.dispatchedRecorder;
      const committed = await recorder.persistApproved();
      expect(committed?.message.content).toBe(fixture.approvedContent);
      expect(committed?.message["__openclaw"]?.humanMentions).toBeUndefined();
      expect(fixture.read()).toEqual([]);
    } finally {
      await fixture.cleanup();
    }
  });

  it("rejects changed recipients on a same-ID retry while preserving the queued original", async () => {
    const fixture = await createMentionFixture();
    try {
      await fixture.send();
      fixture.params.mentions = [
        { profileId: fixture.carolClient.authenticatedUserProfile.profileId, start: 0, end: 4 },
      ];
      const replay = await fixture.send();
      expect(replay).toHaveBeenCalledWith(
        false,
        undefined,
        expect.objectContaining({ message: expect.stringMatching(/different|conflict|reused/i) }),
      );
      const recorder = await fixture.dispatchedRecorder;
      await recorder.persistApproved();
      expect(fixture.read()).toHaveLength(1);
      expect(fixture.read(fixture.carolClient)).toEqual([]);
    } finally {
      await fixture.cleanup();
    }
  });

  it("retains pending-input custody while retrying a transient post-ACK projection failure", async () => {
    const fixture = await createBrowserFollowupFixture({ transientProjectionFailures: 1 });
    try {
      const ack = await fixture.send();
      expect(ack).toHaveBeenCalledWith(
        true,
        expect.objectContaining({ status: "started" }),
        undefined,
        expect.anything(),
      );
      await vi.waitFor(() => expect(dispatchInboundMessageMock).toHaveBeenCalledTimes(2));
      const reconnect = await fixture.send();
      expect(reconnect).toHaveBeenCalledWith(
        true,
        expect.objectContaining({ runId: fixture.params.idempotencyKey, status: "in_flight" }),
        undefined,
        expect.objectContaining({ cached: true }),
      );
      expect(dispatchInboundMessageMock).toHaveBeenCalledTimes(2);
      expect(listSessionPendingInputs(fixture.scope)).toMatchObject({
        total: 1,
        items: [{ state: "queued", runId: fixture.params.idempotencyKey }],
      });
      expect(fixture.context.removeChatRun).not.toHaveBeenCalled();
      expect(fixture.context.broadcast).not.toHaveBeenCalledWith(
        "chat",
        expect.objectContaining({ runId: fixture.params.idempotencyKey, state: "error" }),
        expect.anything(),
      );
    } finally {
      await fixture.cleanup();
    }
  });

  it("durably stages the approved cloud follow-up before ACK without changing the active transcript", async () => {
    const fixture = await createBrowserFollowupFixture();
    const clone = vi.spyOn(globalThis, "structuredClone");
    const { scope, params, approvedContent, activeTranscript } = fixture;
    let transcriptAtAck: ReturnType<typeof loadTranscriptEventsSync> | undefined;
    let pendingAtAck: ReturnType<typeof listSessionPendingInputs> | undefined;
    const respond = vi.fn<RespondFn>((ok) => {
      if (ok) {
        transcriptAtAck = loadTranscriptEventsSync(scope);
        pendingAtAck = listSessionPendingInputs(scope);
      }
    });
    try {
      expect(replyRunRegistry.isActive(scope.sessionKey)).toBe(true);
      expect(
        replyRunRegistry.resolveCurrentMessageInjectionTarget(scope.sessionKey),
      ).toBeUndefined();
      await fixture.send(respond);
      expect(respond).toHaveBeenCalledWith(
        true,
        expect.objectContaining({ runId: params.idempotencyKey, status: "started" }),
        undefined,
        expect.anything(),
      );
      expect(respond.mock.calls[0]?.[1]).not.toHaveProperty("messageSeq");
      expect(transcriptAtAck).toEqual(activeTranscript);
      expect(pendingAtAck).toMatchObject({
        total: 1,
        items: [
          {
            state: "queued",
            runId: params.idempotencyKey,
            message: {
              role: "user",
              content: approvedContent,
              idempotencyKey: `${params.idempotencyKey}:user`,
            },
          },
        ],
      });
      // Initial resolution detaches the store; custody needs only the current target binding.
      expect(
        clone.mock.calls.filter(
          ([entry]) => isRecord(entry) && entry.sessionId === "unrelated-browser-session",
        ).length,
      ).toBeLessThanOrEqual(1);
    } finally {
      clone.mockRestore();
      await fixture.cleanup();
    }
  });

  it("commits an existing idle session input before ACK through restart-safe admission", async () => {
    const fixture = await createBrowserFollowupFixture({ active: false });
    const clone = vi.spyOn(globalThis, "structuredClone");
    let transcriptAtAck: ReturnType<typeof loadTranscriptEventsSync> | undefined;
    const respond = vi.fn<RespondFn>((ok) => {
      if (ok) {
        transcriptAtAck = loadTranscriptEventsSync(fixture.scope);
      }
    });
    try {
      await fixture.send(respond);
      expect(respond).toHaveBeenCalledWith(
        true,
        expect.objectContaining({ status: "started", messageSeq: 2 }),
        undefined,
        expect.anything(),
      );
      expect(transcriptAtAck).toHaveLength(fixture.activeTranscript.length + 1);
      expect(transcriptAtAck?.at(-1)).toMatchObject({
        message: {
          role: "user",
          content: fixture.params.message,
          idempotencyKey: `${fixture.params.idempotencyKey}:user`,
        },
      });
      expect(listSessionPendingInputs(fixture.scope)).toEqual({ items: [], total: 0 });
      expect(
        clone.mock.calls.filter(
          ([entry]) => isRecord(entry) && entry.sessionId === "unrelated-browser-session",
        ).length,
      ).toBeLessThanOrEqual(1);
    } finally {
      clone.mockRestore();
      await fixture.cleanup();
    }
  });

  it.each(["worker-turn", "remote-exec"] as const)(
    "holds an idle %s browser input in custody while its workspace is syncing",
    async (executionMode) => {
      const fixture = await createBrowserFollowupFixture({ active: false });
      const placements = createWorkerSessionPlacementStore();
      const requested = placements.startDispatch({ ...fixture.scope, executionMode });
      const provisioning = placements.transition({
        sessionId: fixture.scope.sessionId,
        from: "requested",
        to: "provisioning",
        expectedGeneration: requested.generation,
        patch: { environmentId: "setup-environment" },
      });
      placements.transition({
        sessionId: fixture.scope.sessionId,
        from: "provisioning",
        to: "syncing",
        expectedGeneration: provisioning.generation,
        patch: { workerBundleHash: "a".repeat(64) },
      });
      fixture.context.workerSessionPlacementService = placements;
      try {
        const respond = await fixture.send();
        expect(respond).toHaveBeenCalledWith(
          true,
          expect.objectContaining({ status: "started" }),
          undefined,
          expect.anything(),
        );
        expect(respond.mock.calls[0]?.[1]).not.toHaveProperty("messageSeq");
        expect(loadTranscriptEventsSync(fixture.scope)).toEqual(fixture.activeTranscript);
        expect(listSessionPendingInputs(fixture.scope)).toMatchObject({
          total: 1,
          items: [{ state: "queued", runId: fixture.params.idempotencyKey }],
        });
      } finally {
        await fixture.cleanup();
      }
    },
  );

  it("retries a failed custody write with the same request identity without acknowledging lost input", async () => {
    const fixture = await createBrowserFollowupFixture();
    const database = openOpenClawAgentDatabase(
      toDatabaseOptions(resolveSqliteScope(fixture.scope)),
    ).db;
    ensureSessionPendingInputsSchema(database);
    database.exec(
      "CREATE TRIGGER reject_browser_custody BEFORE INSERT ON session_pending_inputs BEGIN SELECT RAISE(ABORT, 'custody unavailable'); END",
    );
    try {
      const rejected = await fixture.send();
      expect(rejected).toHaveBeenCalledWith(
        false,
        expect.objectContaining({ status: "error" }),
        expect.objectContaining({ message: expect.stringContaining("custody unavailable") }),
        expect.anything(),
      );
      expect(dispatchInboundMessageMock).not.toHaveBeenCalled();
      expect(loadTranscriptEventsSync(fixture.scope)).toEqual(fixture.activeTranscript);
      expect(listSessionPendingInputs(fixture.scope)).toEqual({ items: [], total: 0 });
      expect(fixture.context.chatAbortControllers.has(fixture.params.idempotencyKey)).toBe(false);
      await getSessionWorkAdmissionRelease({
        scope: fixture.scope.storePath,
        identities: [fixture.scope.sessionKey, fixture.scope.sessionId],
      });

      database.exec("DROP TRIGGER reject_browser_custody");
      const retried = await fixture.send();
      expect(retried).toHaveBeenCalledWith(
        true,
        expect.objectContaining({ runId: fixture.params.idempotencyKey, status: "started" }),
        undefined,
        expect.anything(),
      );
      expect(listSessionPendingInputs(fixture.scope)).toMatchObject({
        total: 1,
        items: [{ state: "queued", message: { content: fixture.approvedContent } }],
      });
      expect(loadTranscriptEventsSync(fixture.scope)).toEqual(fixture.activeTranscript);
    } finally {
      database.exec("DROP TRIGGER IF EXISTS reject_browser_custody");
      await fixture.cleanup();
    }
  });

  it.each(["cancellation", "lifecycle rotation", "session replacement"] as const)(
    "revalidates %s after message approval before committing custody",
    async (change) => {
      const fixture = await createBrowserFollowupFixture();
      fixture.beforeApprove.mockImplementation(() => {
        if (change === "lifecycle rotation") {
          rotateAgentEventLifecycleGeneration();
          return;
        }
        if (change === "session replacement") {
          replaceSessionEntrySync(fixture.scope, {
            sessionId: "successor-session",
            updatedAt: Date.now(),
          });
          return;
        }
        const active = fixture.context.chatAbortControllers.get(fixture.params.idempotencyKey);
        if (!active) {
          throw new Error("Expected the browser admission to own its cancellation controller");
        }
        active.abortStopReason = "rpc";
        active.controller.abort();
      });
      try {
        const respond = await fixture.send();
        expect(fixture.beforeApprove).toHaveBeenCalledOnce();
        expect(respond).toHaveBeenCalledOnce();
        expect(respond).not.toHaveBeenCalledWith(
          true,
          expect.objectContaining({ status: "started" }),
          undefined,
          expect.anything(),
        );
        expect(dispatchInboundMessageMock).not.toHaveBeenCalled();
        expect(listSessionPendingInputs(fixture.scope)).toEqual({ items: [], total: 0 });
        expect(loadTranscriptEventsSync(fixture.scope)).toEqual(fixture.activeTranscript);
        if (change === "session replacement") {
          expect(loadSessionEntry(fixture.scope)?.sessionId).toBe("successor-session");
          expect(
            loadTranscriptEventsSync({ ...fixture.scope, sessionId: "successor-session" }),
          ).toEqual([]);
        }
        expect(fixture.context.chatAbortControllers.has(fixture.params.idempotencyKey)).toBe(false);
      } finally {
        await fixture.cleanup();
      }
    },
  );

  it("keeps one approved source when an accepted browser request is retried", async () => {
    const fixture = await createBrowserFollowupFixture();
    try {
      await fixture.send();
      const accepted = listSessionPendingInputs(fixture.scope);
      expect(accepted.total).toBe(1);
      const retried = await fixture.send();
      expect(retried).toHaveBeenCalledWith(
        true,
        expect.objectContaining({ runId: fixture.params.idempotencyKey, status: "in_flight" }),
        undefined,
        expect.objectContaining({ cached: true }),
      );
      expect(listSessionPendingInputs(fixture.scope)).toEqual(accepted);
      expect(fixture.beforeApprove).toHaveBeenCalledOnce();
      expect(loadTranscriptEventsSync(fixture.scope)).toEqual(fixture.activeTranscript);
    } finally {
      await fixture.cleanup();
    }
  });

  it("does not execute a consumed collected source when retried after the session becomes idle", async () => {
    const fixture = await createBrowserFollowupFixture();
    try {
      await fixture.send();
      expect(listSessionPendingInputs(fixture.scope).total).toBe(1);
      const source = await fixture.dispatchedRecorder;
      const aggregate = createUserTurnTranscriptRecorder({
        input: {
          text: "Collected follow-up already accepted for execution.",
          idempotencyKey: "collected-follow-up:user",
          timestamp: Date.now(),
        },
        pendingInputSources: [source],
        target: () => ({
          ...fixture.scope,
          sessionEntry: loadSessionEntry(fixture.scope),
          expectedSessionId: fixture.scope.sessionId,
        }),
      });
      await aggregate.persistApproved();
      const consumedTranscript = loadTranscriptEventsSync(fixture.scope);
      expect(consumedTranscript).toHaveLength(fixture.activeTranscript.length + 1);
      expect(listSessionPendingInputs(fixture.scope)).toEqual({ items: [], total: 0 });
      await fixture.finishDispatch();
      await patchSessionEntryCore(fixture.scope, () => ({ status: "done" }));
      const registry = getTestPluginRegistry();
      registry.typedHooks = registry.typedHooks.filter(
        (hook) => hook.pluginId !== "approved-input-fixture",
      );
      initializeGlobalHookRunner(registry);
      // Exercise durable replay detection after the transient ACK cache is gone.
      fixture.context.dedupe.clear();
      dispatchInboundMessageMock.mockClear();
      const retried = await fixture.send();
      expect(retried).toHaveBeenCalledWith(
        true,
        { runId: fixture.params.idempotencyKey, status: "ok" },
        undefined,
        expect.objectContaining({ cached: true }),
      );
      expect(dispatchInboundMessageMock).not.toHaveBeenCalled();
      expect(fixture.beforeApprove).toHaveBeenCalledOnce();
      expect(loadTranscriptEventsSync(fixture.scope)).toEqual(consumedTranscript);
      expect(listSessionPendingInputs(fixture.scope)).toEqual({ items: [], total: 0 });
    } finally {
      await fixture.cleanup();
    }
  });

  it.each(["consumed", "changed-payload", "interrupted"] as const)(
    "preserves legacy collected-input replay without adopting old custody (%s)",
    async (disposition) => {
      const fixture = await createBrowserFollowupFixture({ preserveContent: true });
      const profile = ensureProfileForEmail("legacy-input@example.test");
      fixture.client.authenticatedUserProfile = {
        profileId: profile.id,
        displayName: "Legacy input author",
        hasAvatar: false,
        updatedAt: 1,
      };
      try {
        const originalAck = await fixture.send();
        expect(originalAck.mock.calls[0]?.[0]).toBe(true);
        let source: UserTurnTranscriptRecorder | undefined;
        void fixture.dispatchedRecorder.then((recorder) => {
          source = recorder;
        });
        await vi.waitFor(() => expect(source).toBeDefined(), { timeout: 5_000 });
        if (!source) {
          throw new Error("Expected the original accepted input recorder");
        }
        const message = source.getPendingInputMessage?.();
        if (!message) {
          throw new Error("Expected the approved original source before collection");
        }
        const { timestamp: _timestamp, ...stableMessage } = message;
        // This is the exact pre-upgrade stored format. Keep the real accepted
        // source and collector, changing only the historical request hash.
        const legacyHash = createHash("sha256")
          .update(stableStringify(stableMessage))
          .digest("hex");
        const database = openOpenClawAgentDatabase(
          toDatabaseOptions(resolveSqliteScope(fixture.scope)),
        );
        const seeded = database.db
          .prepare(
            "UPDATE session_pending_inputs SET request_hash = ? WHERE session_key = ? AND session_id = ? AND run_id = ?",
          )
          .run(
            legacyHash,
            fixture.scope.sessionKey,
            fixture.scope.sessionId,
            fixture.params.idempotencyKey,
          );
        expect(seeded.changes).toBe(1);
        if (disposition !== "interrupted") {
          const aggregate = createUserTurnTranscriptRecorder({
            input: {
              text: "Collected follow-up already accepted for execution.",
              idempotencyKey: "legacy-collected-follow-up:user",
              timestamp: Date.now(),
            },
            pendingInputSources: [source],
            target: () => ({
              ...fixture.scope,
              sessionEntry: loadSessionEntry(fixture.scope),
              expectedSessionId: fixture.scope.sessionId,
            }),
          });
          await aggregate.persistApproved();
        }
        rotateAgentEventLifecycleGeneration();
        await fixture.finishDispatch();
        await patchSessionEntryCore(fixture.scope, () => ({ status: "done" }));
        const transcript = loadTranscriptEventsSync(fixture.scope);
        fixture.context.dedupe.clear();
        dispatchInboundMessageMock.mockClear();
        if (disposition === "changed-payload") {
          fixture.params.message += " Changed request.";
        }

        const retried = await fixture.send();
        if (disposition === "consumed") {
          expect(retried).toHaveBeenCalledWith(
            true,
            { runId: fixture.params.idempotencyKey, status: "ok" },
            undefined,
            expect.objectContaining({ cached: true }),
          );
        } else {
          expect(retried.mock.calls[0]?.[0]).toBe(false);
        }
        expect(dispatchInboundMessageMock).not.toHaveBeenCalled();
        expect(fixture.beforeApprove).toHaveBeenCalledOnce();
        expect(loadTranscriptEventsSync(fixture.scope)).toEqual(transcript);
        expect(listSessionPendingInputs(fixture.scope).total).toBe(
          disposition === "interrupted" ? 1 : 0,
        );
      } finally {
        await fixture.cleanup();
      }
    },
  );

  it.each([false, true])(
    "re-admits an unconsumed browser input after restart with fresh custody (attachment: %s)",
    async (attachment) => {
      const fixture = await createBrowserFollowupFixture({ preserveContent: true });
      const resumedRelease = createDeferred();
      let resumedRecorder: UserTurnTranscriptRecorder | undefined;
      const profile = ensureProfileForEmail("restart-input@example.test");
      fixture.client.authenticatedUserProfile = {
        profileId: profile.id,
        displayName: "Input author",
        hasAvatar: false,
        updatedAt: 1,
      };
      if (!attachment) {
        delete fixture.params.sessionId;
      }
      if (attachment) {
        fixture.params.attachments = [
          {
            type: "file",
            mimeType: "text/plain",
            fileName: "review.txt",
            content: Buffer.from("Keep these exact attachment bytes.").toString("base64"),
          },
        ];
      }
      try {
        const originalAck = await fixture.send();
        expect(originalAck).toHaveBeenCalledWith(
          true,
          expect.objectContaining({ status: "started" }),
          undefined,
          expect.anything(),
        );
        const originalRecorder = await fixture.dispatchedRecorder;
        const original = listSessionPendingInputs(fixture.scope).items[0];
        expect(original).toBeDefined();
        rotateAgentEventLifecycleGeneration();
        await fixture.finishDispatch();
        expect(listSessionPendingInputs(fixture.scope).items).toEqual([
          { ...original, state: "interrupted" },
        ]);
        expect(loadTranscriptEventsSync(fixture.scope)).toEqual(fixture.activeTranscript);
        fixture.context.dedupe.clear();
        await patchSessionEntryCore(fixture.scope, () => ({ status: "done" }));
        dispatchInboundMessageMock.mockImplementation(async (options: unknown) => {
          const { replyOptions } = options as Parameters<typeof dispatchInboundMessage>[0];
          if (replyOptions?.userTurnTranscriptRecorder) {
            resumedRecorder = replyOptions.userTurnTranscriptRecorder;
          }
          await resumedRelease.promise;
          return {};
        });

        // Exercise the actual browser reconnect envelope through request normalization.
        Object.assign(fixture.params, {
          sessionId: fixture.scope.sessionId,
          __controlUiReconnectResume: true,
        });
        const ack = await fixture.send();
        expect(ack).toHaveBeenCalledWith(
          true,
          expect.objectContaining({ status: "started", runId: fixture.params.idempotencyKey }),
          undefined,
          expect.anything(),
        );
        await vi.waitFor(() => expect(resumedRecorder).toBeDefined(), { timeout: 5_000 });
        if (!resumedRecorder) {
          throw new Error("Fresh input admission did not dispatch its recorder");
        }
        const resumed = resumedRecorder;
        expect(listSessionPendingInputs(fixture.scope).items).toEqual([
          { ...original, state: "queued" },
        ]);
        expect(() => originalRecorder.withPendingInput?.(() => {})).toThrow("ownership ended");
        const committed = await resumed.persistApproved();
        expect(committed).toMatchObject({ appended: true, messageId: original?.id });
        expect(committed?.message).toEqual(original?.message);
        expect(fixture.beforeApprove).toHaveBeenCalledOnce();
        expect(listSessionPendingInputs(fixture.scope).items).toEqual([]);
      } finally {
        resumedRelease.resolve();
        await fixture.cleanup();
      }
    },
  );

  it.each(["sender", "payload", "cancelled", "same-generation"] as const)(
    "does not recover pending input when its %s prevents fresh admission",
    async (change) => {
      const fixture = await createMentionFixture({ preserveContent: true });
      try {
        const originalAck = await fixture.send();
        expect(originalAck.mock.calls[0]?.[0]).toBe(true);
        const recorder = await fixture.dispatchedRecorder;
        const original = listSessionPendingInputs(fixture.scope).items[0];
        expect(original).toBeDefined();
        if (change === "cancelled" || change === "same-generation") {
          recorder?.finishPendingInput?.(change === "cancelled" ? "cancelled" : "interrupted");
        }
        if (change !== "same-generation") {
          rotateAgentEventLifecycleGeneration();
        }
        await fixture.finishDispatch();
        fixture.context.dedupe.clear();
        dispatchInboundMessageMock.mockClear();
        await patchSessionEntryCore(fixture.scope, () => ({ status: "done" }));
        if (change === "sender") {
          fixture.client.authenticatedUserProfile = fixture.bobClient.authenticatedUserProfile;
        } else if (change === "payload") {
          fixture.params.message += " Changed request.";
        }
        const rejected = await fixture.send();
        expect(rejected.mock.calls[0]?.[0]).toBe(false);
        expect(dispatchInboundMessageMock).not.toHaveBeenCalled();
        expect(listSessionPendingInputs(fixture.scope).items).toEqual([
          { ...original, state: change === "cancelled" ? "cancelled" : "interrupted" },
        ]);
        expect(loadTranscriptEventsSync(fixture.scope)).toEqual(fixture.activeTranscript);
      } finally {
        await fixture.cleanup();
      }
    },
  );
});
