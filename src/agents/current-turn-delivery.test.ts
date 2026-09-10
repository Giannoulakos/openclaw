import { Type } from "typebox";
import { Value } from "typebox/value";
import { afterEach, describe, expect, it, vi } from "vitest";
import { createDeferred } from "../../test/helpers/promise.js";
import type { ChannelOutboundAdapter } from "../channels/plugins/types.public.js";
import type { OpenClawConfig } from "../config/config.js";
import {
  mintMessageActionTurnCapability,
  revokeMessageActionTurnCapability,
} from "../gateway/message-action-turn-capability.js";
import { resetPluginRuntimeStateForTest, setActivePluginRegistry } from "../plugins/runtime.js";
import {
  createDirectOutboundTestAdapter,
  createOutboundTestPlugin,
  createTestRegistry,
} from "../test-utils/channel-plugins.js";
import { createEmbeddedAttemptCodingTools } from "./agent-tools.js";
import { runWithAgentRingZeroTools } from "./agent-tools.ring-zero-context.js";
import {
  createCurrentTurnDelivery,
  createCurrentTurnDeliveryTool,
  type CurrentTurnDelivery,
} from "./current-turn-delivery.js";
import {
  closeCurrentTurnReplyCompletionOwner,
  copyCurrentTurnReplyCompletion,
  createCurrentTurnReplyCompletionOwner,
  readCurrentTurnReplyCompletion,
} from "./current-turn-reply-completion.js";
import {
  applyEmbeddedAttemptToolsAllow,
  resolveEmbeddedAttemptToolConstructionPlan,
} from "./embedded-agent-runner/run/attempt-tool-construction-plan.js";
import { isAgentToolReplaySafe, isAgentToolRestartSafe } from "./tool-replay-safety.js";
import { withGatewayToolCallerIdentity } from "./tools/gateway-caller-context.js";

type CurrentTurnDeliveryResult = Awaited<ReturnType<CurrentTurnDelivery["send"]>>;

function delivery(send: CurrentTurnDelivery["send"]): CurrentTurnDelivery {
  return { send };
}

type DirectSendText = NonNullable<ChannelOutboundAdapter["sendText"]>;
type DirectSendTextMock = ReturnType<typeof vi.fn<DirectSendText>>;
type DirectSendMedia = NonNullable<ChannelOutboundAdapter["sendMedia"]>;
type DirectSendMediaMock = ReturnType<typeof vi.fn<DirectSendMedia>>;

function createCurrentTurnDeliveryHarness(params?: {
  tokenSessionId?: string;
  contextSessionId?: string;
  abortController?: AbortController;
  assertActive?: () => void;
  cfg?: OpenClawConfig;
  onPayloadNormalize?: (token: string) => void;
  sendText?: DirectSendTextMock;
  sendMedia?: DirectSendMediaMock;
}): {
  constructionParams: Parameters<typeof createCurrentTurnDelivery>[0];
  current: CurrentTurnDelivery;
  sendText: DirectSendTextMock;
  sessionKey: string;
  token: string;
} {
  const sessionKey = "agent:main:telegram:direct:123";
  const sessionId = params?.contextSessionId ?? "session-current-reply";
  const runId = "run-current-reply";
  const sendText =
    params?.sendText ??
    vi.fn<DirectSendText>(async () => ({
      channel: "telegram" as const,
      messageId: "sent-1",
    }));
  let token = "";
  setActivePluginRegistry(
    createTestRegistry([
      {
        pluginId: "telegram",
        source: "test",
        plugin: createOutboundTestPlugin({
          id: "telegram",
          messaging: {
            targetResolver: {
              looksLikeId: (raw) => /^-?\d+$/.test(raw),
              hint: "<chatId>",
            },
          },
          outbound: {
            deliveryMode: "direct",
            ...(params?.onPayloadNormalize
              ? {
                  normalizePayload: ({ payload }) => {
                    params.onPayloadNormalize?.(token);
                    return payload;
                  },
                }
              : {}),
            sendText,
            ...(params?.sendMedia ? { sendMedia: params.sendMedia } : {}),
          },
        }),
      },
    ]),
  );
  token = mintMessageActionTurnCapability({
    agentId: "main",
    runId,
    sessionKey,
    sessionId: params?.tokenSessionId ?? sessionId,
  });
  const cfg = params?.cfg ?? ({} as OpenClawConfig);
  const abortController = params?.abortController ?? new AbortController();
  try {
    const constructionParams: Parameters<typeof createCurrentTurnDelivery>[0] = {
      authority: {
        abortSignal: abortController.signal,
        assertActive: params?.assertActive ?? (() => {}),
      },
      context: {
        runtimeConfig: cfg,
        getRuntimeConfig: () => cfg,
        agentId: "main",
        sessionKey,
        sessionId,
        deliveryContext: { channel: "telegram", to: "123" },
      },
      runId,
      token,
    };
    const current = createCurrentTurnDelivery(constructionParams);
    if (!current) {
      throw new Error("expected current-turn delivery");
    }
    return { constructionParams, current, sendText, sessionKey, token };
  } catch (error) {
    revokeMessageActionTurnCapability(token);
    throw error;
  }
}

describe("current-turn delivery tool", () => {
  afterEach(() => {
    resetPluginRuntimeStateForTest();
  });

  it("declares closed schemas and is neither replay-safe nor restart-safe", () => {
    const tool = createCurrentTurnDeliveryTool(delivery(async () => ({ status: "sent" })));

    expect(Value.Check(tool.parameters, { text: "hello" })).toBe(true);
    expect(Value.Check(tool.parameters, { text: "hello", target: "elsewhere" })).toBe(false);
    expect(Value.Check(tool.outputSchema!, { status: "sent" })).toBe(true);
    expect(Value.Check(tool.outputSchema!, { status: "sent", raw: true })).toBe(false);
    expect(isAgentToolReplaySafe(tool)).toBe(false);
    expect(isAgentToolRestartSafe(tool)).toBe(false);
  });

  it("validates before synchronously consuming its one-shot authority", async () => {
    let release!: (result: CurrentTurnDeliveryResult) => void;
    const send = vi.fn(
      async () =>
        await new Promise<CurrentTurnDeliveryResult>((resolve) => {
          release = resolve;
        }),
    );
    const tool = createCurrentTurnDeliveryTool(delivery(send));

    await expect(tool.execute("bad", { text: " " })).rejects.toThrow("text required");
    const first = tool.execute("first", { text: "hello" });
    await expect(tool.execute("second", { text: "again" })).rejects.toThrow(
      "already been consumed",
    );
    release({ status: "sent" });

    await expect(first).resolves.toMatchObject({
      details: { status: "sent" },
      terminate: true,
    });
    expect(send).toHaveBeenCalledWith(
      { text: "hello", mediaUrl: undefined },
      true,
      undefined,
      expect.any(Function),
    );
  });

  it.each([
    {
      outcome: { status: "sent" } satisfies CurrentTurnDeliveryResult,
      terminal: true,
    },
    {
      outcome: {
        status: "partial_failed",
        sentBeforeError: true,
        error: "receipt failed",
      } satisfies CurrentTurnDeliveryResult,
      terminal: true,
    },
    {
      outcome: {
        status: "partial_failed",
        error: "delivery failed",
      } satisfies CurrentTurnDeliveryResult,
      terminal: false,
    },
    {
      outcome: {
        status: "suppressed",
        suppressionReason: "policy",
      } satisfies CurrentTurnDeliveryResult,
      terminal: false,
    },
    {
      outcome: {
        status: "not_sent",
        suppressionReason: "adapter_returned_no_send",
      } satisfies CurrentTurnDeliveryResult,
      terminal: false,
    },
    {
      outcome: { status: "failed", error: "offline" } satisfies CurrentTurnDeliveryResult,
      terminal: false,
    },
  ])("projects $outcome.status with terminal=$terminal", async ({ outcome, terminal }) => {
    const result = await createCurrentTurnDeliveryTool(delivery(async () => outcome)).execute(
      "outcome",
      { text: "hello" },
    );

    expect(result.details).toEqual(outcome);
    expect(result.terminate).toBe(terminal ? true : undefined);
  });

  it("returns typed nonterminal failures for same-cell branching", async () => {
    const tool = createCurrentTurnDeliveryTool(
      delivery(async () => {
        throw new Error("authority expired");
      }),
    );

    await expect(tool.execute("failed", { text: "hello" })).resolves.toMatchObject({
      details: { status: "failed", error: "authority expired" },
    });
  });

  it("allows one direct adapter call while current-turn authority remains active", async () => {
    const { current, sendText, token } = createCurrentTurnDeliveryHarness();

    try {
      await expect(current.send({ text: "hello" })).resolves.toMatchObject({
        status: "sent",
        messageId: "sent-1",
      });
      expect(sendText).toHaveBeenCalledOnce();
    } finally {
      revokeMessageActionTurnCapability(token);
    }
  });

  it("retains authenticated pending dispatch through abort and late acknowledgement", async () => {
    const entered = createDeferred();
    const acknowledge = createDeferred();
    const abortController = new AbortController();
    const sendText = vi.fn<DirectSendText>(async () => {
      entered.resolve();
      await acknowledge.promise;
      return { channel: "telegram", messageId: "sent-1" };
    });
    const { current, token } = createCurrentTurnDeliveryHarness({ abortController, sendText });
    const owner = createCurrentTurnReplyCompletionOwner();
    const retained = copyCurrentTurnReplyCompletion(owner, {});
    const next = createCurrentTurnReplyCompletionOwner();
    const tool = createCurrentTurnDeliveryTool(current, owner);
    const pending = tool.execute("held", { text: "hello" });
    try {
      await entered.promise;
      expect(readCurrentTurnReplyCompletion(retained)).toBe("pending");
      abortController.abort(new Error("cancelled during dispatch"));
      closeCurrentTurnReplyCompletionOwner(owner);
      expect(readCurrentTurnReplyCompletion(retained)).toBe("pending");
      expect(readCurrentTurnReplyCompletion(next)).toBeUndefined();
      acknowledge.resolve();
      const result = await pending;
      expect(result.details).toMatchObject({ status: "partial_failed", sentBeforeError: true });
      expect(result.terminate).toBe(true);
      expect(readCurrentTurnReplyCompletion(retained)).toBe("ambiguous");
      expect(readCurrentTurnReplyCompletion(next)).toBeUndefined();
      expect(sendText).toHaveBeenCalledOnce();
    } finally {
      acknowledge.resolve();
      await pending;
      closeCurrentTurnReplyCompletionOwner(owner);
      closeCurrentTurnReplyCompletionOwner(next);
      revokeMessageActionTurnCapability(token);
    }
  });

  it("does not record pending dispatch for a rejected pre-I/O authority", async () => {
    const { current, token, sendText } = createCurrentTurnDeliveryHarness({
      assertActive: () => {
        throw new Error("host closed before dispatch");
      },
    });
    const owner = createCurrentTurnReplyCompletionOwner();
    try {
      const result = await createCurrentTurnDeliveryTool(current, owner).execute("denied", {
        text: "hello",
      });
      expect(result.details).toEqual({ status: "failed", error: "host closed before dispatch" });
      expect(result.terminate).toBeUndefined();
      expect(readCurrentTurnReplyCompletion(owner)).toBeUndefined();
      expect(sendText).not.toHaveBeenCalled();
    } finally {
      closeCurrentTurnReplyCompletionOwner(owner);
      revokeMessageActionTurnCapability(token);
    }
  });

  it("keeps an identityless lost acknowledgement terminal after exactly one dispatch", async () => {
    const sendText = vi.fn<DirectSendText>(async ({ onPlatformSendDispatch }) => {
      await onPlatformSendDispatch?.();
      throw new Error("adapter acknowledgement lost");
    });
    const { current, token } = createCurrentTurnDeliveryHarness({ sendText });
    const tool = createCurrentTurnDeliveryTool(current);
    try {
      const result = await tool.execute("ack-loss", { text: "hello" });
      expect(result).toMatchObject({
        terminate: true,
        details: {
          status: "partial_failed",
          sentBeforeError: true,
          error: "adapter acknowledgement lost",
        },
      });
      expect(result.details).not.toHaveProperty("messageId");
      expect(sendText).toHaveBeenCalledOnce();
      await expect(tool.execute("duplicate", { text: "again" })).rejects.toThrow(
        "already been consumed",
      );
      expect(sendText).toHaveBeenCalledOnce();
    } finally {
      revokeMessageActionTurnCapability(token);
    }
  });

  it("retains the construction-time authority identity across awaited preparation", async () => {
    let originalAuthorityActive = true;
    const construction: {
      params?: Parameters<typeof createCurrentTurnDelivery>[0];
    } = {};
    const fixture = createCurrentTurnDeliveryHarness({
      assertActive: () => {
        if (!originalAuthorityActive) {
          throw new Error("original current-turn authority closed");
        }
      },
      onPayloadNormalize: () => {
        originalAuthorityActive = false;
        if (construction.params) {
          construction.params.authority = {
            abortSignal: new AbortController().signal,
            assertActive: () => {},
          };
        }
      },
    });
    construction.params = fixture.constructionParams;

    try {
      await expect(fixture.current.send({ text: "must not escape" })).rejects.toThrow(
        "original current-turn authority closed",
      );
      expect(fixture.sendText).not.toHaveBeenCalled();
    } finally {
      revokeMessageActionTurnCapability(fixture.token);
    }
  });

  it.each(["revoked", "replaced"] as const)(
    "rejects %s authority during delivery without adapter I/O",
    async (mode) => {
      const onPayloadNormalize = vi.fn((activeToken: string) => {
        if (mode === "revoked") {
          revokeMessageActionTurnCapability(activeToken);
        } else {
          setActivePluginRegistry(createTestRegistry([]));
        }
      });
      const { current, sendText, token } = createCurrentTurnDeliveryHarness({
        onPayloadNormalize,
      });

      try {
        await expect(current.send({ text: "must not escape" })).rejects.toThrow(
          "current-turn delivery capability is no longer active",
        );
        expect(sendText).not.toHaveBeenCalled();
        // Preparation normalizes before and after modifying policy.
        expect(onPayloadNormalize.mock.calls).toEqual([[token], [token]]);
      } finally {
        revokeMessageActionTurnCapability(token);
      }
    },
  );

  it.each(["token", "registry", "source-run", "abort", "session-writer"] as const)(
    "revalidates %s authority after payload preparation without adapter I/O",
    async (mode) => {
      let sourceRunActive = true;
      let sessionWriterCurrent = true;
      const abortController = new AbortController();
      const { current, sendText, sessionKey, token } = createCurrentTurnDeliveryHarness({
        abortController,
        assertActive: () => {
          if (!sessionWriterCurrent) {
            throw new Error("Session writer changed before current-turn delivery");
          }
        },
        onPayloadNormalize: () => {
          if (mode === "token") {
            revokeMessageActionTurnCapability(token);
          } else if (mode === "registry") {
            setActivePluginRegistry(createTestRegistry([]));
          } else if (mode === "source-run") {
            sourceRunActive = false;
          } else if (mode === "abort") {
            abortController.abort(new Error("current turn closed"));
          } else {
            sessionWriterCurrent = false;
          }
        },
      });

      try {
        const pending = withGatewayToolCallerIdentity(
          {
            agentId: "main",
            sessionKey,
            receiptAuthority: () => sourceRunActive,
          },
          async () =>
            await createCurrentTurnDeliveryTool(current).execute("revoked", {
              text: "must not escape",
            }),
        );
        await withGatewayToolCallerIdentity(
          {
            agentId: "main",
            sessionKey,
            receiptAuthority: () => true,
          },
          async () => await Promise.resolve(),
        );

        const result = await pending;
        expect(result).toMatchObject({
          details: {
            status: "failed",
          },
        });
        expect(result.details).not.toHaveProperty("sentBeforeError");
        expect(result).not.toHaveProperty("terminate");
        expect(sendText).not.toHaveBeenCalled();
      } finally {
        revokeMessageActionTurnCapability(token);
      }
    },
  );

  it("uses invocation cancellation during held media preparation before physical transport", async () => {
    const preparationStarted = createDeferred();
    const releasePreparation = createDeferred();
    const platformSend = vi.fn(async () => ({
      channel: "telegram" as const,
      messageId: "must-not-send",
    }));
    const sendMedia = vi.fn<DirectSendMedia>(async (params) => {
      preparationStarted.resolve();
      await releasePreparation.promise;
      await params.onPlatformSendDispatch?.();
      return await platformSend();
    });
    const invocationAbort = new AbortController();
    const { current, token } = createCurrentTurnDeliveryHarness({ sendMedia });
    const owner = createCurrentTurnReplyCompletionOwner();

    try {
      const pending = createCurrentTurnDeliveryTool(current, owner).execute(
        "cancelled",
        { text: "must not escape", mediaUrl: "held.png" },
        invocationAbort.signal,
      );
      await preparationStarted.promise;
      expect(readCurrentTurnReplyCompletion(owner)).toBe("pending");
      invocationAbort.abort(new Error("tool invocation cancelled"));
      releasePreparation.resolve();

      await expect(pending).resolves.toMatchObject({
        details: { status: "failed" },
      });
      expect(sendMedia).toHaveBeenCalledOnce();
      expect(platformSend).not.toHaveBeenCalled();
      // The adapter handoff occurred; cancellation supplies no authoritative
      // non-dispatch receipt that could safely authorize another reply.
      expect(readCurrentTurnReplyCompletion(owner)).toBe("ambiguous");
    } finally {
      releasePreparation.resolve();
      closeCurrentTurnReplyCompletionOwner(owner);
      revokeMessageActionTurnCapability(token);
    }
  });

  it("returns an explicit nonterminal policy denial without adapter I/O", async () => {
    const { current, sendText, token } = createCurrentTurnDeliveryHarness({
      cfg: {
        tools: {
          message: {
            actions: {
              allow: ["react"],
            },
          },
        },
      } as OpenClawConfig,
    });

    try {
      await expect(
        createCurrentTurnDeliveryTool(current).execute("denied", { text: "must not escape" }),
      ).resolves.toMatchObject({
        details: {
          status: "failed",
          error: 'Message action "send" is disabled for this agent.',
        },
      });
      expect(sendText).not.toHaveBeenCalled();
    } finally {
      revokeMessageActionTurnCapability(token);
    }
  });

  it("rejects a wrong-session capability before adapter I/O", () => {
    const sendText = vi.fn<DirectSendText>(async () => ({
      channel: "telegram" as const,
      messageId: "must-not-send",
    }));
    expect(() =>
      createCurrentTurnDeliveryHarness({
        tokenSessionId: "owning-session",
        contextSessionId: "replacement-session",
        sendText,
      }),
    ).toThrow("current-turn delivery capability is no longer active");
    expect(sendText).not.toHaveBeenCalled();
  });

  it.each([
    {
      requester: "cron",
      runSessionKey: "agent:main:cron:daily-report:run:run-current-reply",
      keepsYield: false,
    },
    {
      requester: "permitted descendant",
      runSessionKey: "agent:main:subagent:current-reply-child",
      keepsYield: true,
    },
  ])(
    "keeps exact terminal authority after wrapping for a $requester requester",
    async ({ runSessionKey, keepsYield }) => {
      const sessionKey = "agent:main:telegram:direct:123";
      const runId = "run-current-reply";
      const sessionId = "session-current-reply";
      const token = mintMessageActionTurnCapability({
        agentId: "main",
        runId,
        sessionKey,
        sessionId,
      });
      setActivePluginRegistry(
        createTestRegistry([
          {
            pluginId: "telegram",
            source: "test",
            plugin: createOutboundTestPlugin({
              id: "telegram",
              messaging: {
                targetResolver: {
                  looksLikeId: (raw) => /^-?\d+$/.test(raw),
                  hint: "<chatId>",
                },
              },
              outbound: createDirectOutboundTestAdapter({ channel: "telegram" }),
            }),
          },
        ]),
      );
      const requesterYield = {
        name: "sessions_yield",
        label: "Yield",
        description: "Yield to the requester.",
        parameters: Type.Object({}, { additionalProperties: false }),
        execute: async () => ({
          content: [{ type: "text" as const, text: "yielded" }],
          details: { status: "yielded" },
        }),
      };
      const currentTurnDeliveryToolRef: {
        value?: ReturnType<typeof createEmbeddedAttemptCodingTools>[number];
      } = {};

      try {
        const tools = runWithAgentRingZeroTools([requesterYield], () =>
          createEmbeddedAttemptCodingTools(
            {
              agentId: "main",
              config: {} as OpenClawConfig,
              sessionKey,
              runSessionKey,
              runId,
              sessionId,
              messageChannel: "telegram",
              agentAccountId: "default",
              messageTo: "123",
              currentMessagingTarget: "123",
              messageActionTurnCapability: token,
              onYield: () => {},
              toolConstructionPlan: {
                includeBaseCodingTools: false,
                includeShellTools: false,
                includeChannelTools: false,
                includeOpenClawTools: true,
                includePluginTools: true,
              },
            },
            {
              deliveryAuthority: {
                abortSignal: new AbortController().signal,
                assertActive: () => {},
              },
              terminalReply: {
                authority: {
                  abortSignal: new AbortController().signal,
                  assertActive: () => {},
                },
                toolRef: currentTurnDeliveryToolRef,
              },
            },
          ),
        );
        const terminalTool = currentTurnDeliveryToolRef.value;
        const terminalInventory = tools.filter((tool) => tool.name === "send_current_reply");
        const publicInventory = tools.filter(
          (tool) => tool !== terminalTool && tool.name !== "sessions_yield",
        );

        expect(terminalTool).toBeDefined();
        expect(terminalInventory).toEqual([terminalTool]);
        expect(publicInventory.length).toBeGreaterThan(0);
        expect(publicInventory).not.toContain(terminalTool);
        expect(tools.some((tool) => tool.name === "sessions_yield")).toBe(keepsYield);
        const terminalResult = await terminalTool?.execute("terminal", { text: "hello" });
        expect(terminalResult?.details).toMatchObject({ status: "sent" });
        expect(terminalResult?.terminate).toBe(true);
      } finally {
        revokeMessageActionTurnCapability(token);
      }
    },
  );

  it("keeps the actual host tool when toolsAllow only constructs read", () => {
    const sessionKey = "agent:main:telegram:direct:123";
    const runId = "run-current-reply";
    const sessionId = "session-current-reply";
    const token = mintMessageActionTurnCapability({
      agentId: "main",
      runId,
      sessionKey,
      sessionId,
    });
    setActivePluginRegistry(
      createTestRegistry([
        {
          pluginId: "telegram",
          source: "test",
          plugin: createOutboundTestPlugin({
            id: "telegram",
            outbound: createDirectOutboundTestAdapter({ channel: "telegram" }),
          }),
        },
      ]),
    );
    const plan = resolveEmbeddedAttemptToolConstructionPlan({
      toolsEnabled: true,
      toolsAllow: ["read"],
    });

    try {
      const currentTurnDeliveryToolRef: {
        value?: ReturnType<typeof createEmbeddedAttemptCodingTools>[number];
      } = {};
      const tools = createEmbeddedAttemptCodingTools(
        {
          agentId: "main",
          config: {} as OpenClawConfig,
          sessionKey,
          runSessionKey: sessionKey,
          runId,
          sessionId,
          messageChannel: "telegram",
          agentAccountId: "default",
          messageTo: "123",
          currentMessagingTarget: "123",
          messageActionTurnCapability: token,
          includeCoreTools: plan.includeCoreTools,
          runtimeToolAllowlist: plan.runtimeToolAllowlist,
          toolConstructionPlan: plan.codingToolConstructionPlan,
        },
        {
          deliveryAuthority: {
            abortSignal: new AbortController().signal,
            assertActive: () => {},
          },
          terminalReply: {
            authority: {
              abortSignal: new AbortController().signal,
              assertActive: () => {},
            },
            toolRef: currentTurnDeliveryToolRef,
          },
        },
      );
      const currentReply = tools.find((tool) => tool.name === "send_current_reply");
      const filtered = applyEmbeddedAttemptToolsAllow(tools, plan.runtimeToolAllowlist, {
        preserveTools: currentReply ? new Set([currentReply]) : undefined,
      });

      expect(plan.codingToolConstructionPlan).toMatchObject({
        includeOpenClawTools: false,
        includePluginTools: false,
      });
      expect(currentReply).toBeDefined();
      expect(currentTurnDeliveryToolRef.value).toBe(currentReply);
      expect(filtered.map((tool) => tool.name)).toEqual(["send_current_reply", "read"]);
    } finally {
      revokeMessageActionTurnCapability(token);
    }
  });
});
