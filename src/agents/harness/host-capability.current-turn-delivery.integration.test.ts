import { describe, expect, it } from "vitest";
import { withCurrentReplyIntegration } from "../current-turn-delivery.integration.test-support.js";

describe("admitted host current reply through Slack transport", () => {
  it("delivers after the same transport's held SDK admission opens", async () => {
    await withCurrentReplyIntegration(async (fixture) => {
      const turn = await fixture.createTurn();
      const tool = turn.createHostTool();
      const release = await fixture.occupyTransport();
      const admitted = fixture.observeSdkAdmission("allowed");
      const pending = fixture.track(
        turn.runWithHostScope(() => tool.execute("allowed", { text: "allowed" })),
      );
      await admitted();
      expect(fixture.network.counts.post).toBe(0);
      expect(turn.completion()).toBe("pending");
      release.resolve();
      await expect(pending).resolves.toMatchObject({
        details: { status: "sent" },
        terminate: true,
      });
      expect(fixture.network.counts.post).toBe(1);
      expect(turn.completion()).toBe("confirmed");
    });
  });

  it.each([
    "host closure",
    "authority release",
    "admission replacement",
    "writer",
    "lifecycle",
  ] as const)(
    "fences a queued physical request after %s without changing the successor row",
    async (revocation) => {
      await withCurrentReplyIntegration(async (fixture) => {
        const turn = await fixture.createTurn();
        const tool = turn.createHostTool();
        const release = await fixture.occupyTransport();
        const admitted = fixture.observeSdkAdmission("revoked");
        const pending = fixture.track(
          turn.runWithHostScope(() => tool.execute("revoked", { text: "revoked" })),
        );
        await admitted();
        expect(fixture.network.counts.post).toBe(0);
        expect(turn.completion()).toBe("pending");
        const originalRow = fixture.readRow();
        if (revocation === "host closure") {
          turn.closeHost();
        } else if (revocation === "authority release") {
          turn.releaseAuthority();
        } else if (revocation === "admission replacement") {
          await turn.replaceAdmission();
        } else if (revocation === "lifecycle") {
          const replaced = turn.replaceLifecycle();
          expect(replaced?.activeWriterRunId).toBe(originalRow?.activeWriterRunId);
          expect(replaced?.lifecycleRevision).not.toBe(originalRow?.lifecycleRevision);
        } else {
          turn.replaceWriter();
        }
        const successor = fixture.readRow();
        release.resolve();
        if (revocation === "writer" || revocation === "lifecycle") {
          await expect(pending).resolves.toMatchObject({ details: { status: "failed" } });
        } else {
          await expect(pending).rejects.toThrow();
        }
        await expect.poll(turn.completion).toBe("ambiguous");
        expect(fixture.network.counts).toMatchObject({ blocker: 100, post: 0 });
        expect(fixture.readRow()).toEqual(successor);
      });
    },
  );

  it("delivers a real local attachment through URL allocation, bytes, and completion", async () => {
    await withCurrentReplyIntegration(async (fixture) => {
      const turn = await fixture.createTurn();
      const mediaUrl = await fixture.state.writeText("media/current-reply.txt", "attachment");
      const tool = turn.createHostTool();
      await expect(
        fixture.track(
          turn.runWithHostScope(() =>
            tool.execute("upload-allowed", { text: "attachment", mediaUrl }),
          ),
        ),
      ).resolves.toMatchObject({ details: { status: "sent" }, terminate: true });
      expect(fixture.network.counts).toMatchObject({
        "upload-url": 1,
        dns: 1,
        upload: 1,
        complete: 1,
        post: 0,
      });
      expect(turn.completion()).toBe("confirmed");
    });
  });

  it.each(["upload-url", "dns", "upload"] as const)(
    "preserves accepted %s evidence but prevents every later upload stage after writer replacement",
    async (stage) => {
      await withCurrentReplyIntegration(async (fixture) => {
        const turn = await fixture.createTurn();
        const mediaUrl = await fixture.state.writeText("media/current-reply.txt", "attachment");
        const held = fixture.network.hold(stage);
        const tool = turn.createHostTool();
        const pending = fixture.track(
          turn.runWithHostScope(() =>
            tool.execute("upload-revoked", { text: "attachment", mediaUrl }),
          ),
        );
        await held.wait();
        expect(turn.completion()).toBe("pending");
        const successor = turn.replaceWriter();
        held.release.resolve();
        await expect(pending).resolves.toMatchObject({ details: { status: "failed" } });
        await expect.poll(turn.completion).toBe("ambiguous");
        expect(fixture.network.counts).toMatchObject({
          "upload-url": 1,
          dns: stage === "upload-url" ? 0 : 1,
          upload: stage === "upload" ? 1 : 0,
          complete: 0,
          post: 0,
        });
        expect(fixture.readRow()).toEqual(successor);
      });
    },
  );

  it.each(["accepted", "lost response"] as const)(
    "keeps a late %s receipt ambiguous after host closure and gives the next turn a new owner",
    async (acknowledgement) => {
      await withCurrentReplyIntegration(async (fixture) => {
        const turn = await fixture.createTurn();
        const tool = turn.createHostTool();
        const held = fixture.network.hold("post");
        if (acknowledgement === "lost response") {
          fixture.network.loseFirstPostResponse();
        }
        const pending = fixture.track(
          turn.runWithHostScope(() => tool.execute("late", { text: "late" })),
        );
        await held.wait();
        expect(fixture.pendingToolExecutions()).toBe(1);
        turn.closeHost();
        await expect(pending).rejects.toThrow();
        expect(fixture.pendingToolExecutions()).toBe(1);
        const joined = fixture.join();
        held.release.resolve();
        await joined;
        expect(fixture.pendingToolExecutions()).toBe(0);
        await expect.poll(turn.completion).toBe("ambiguous");
        expect(fixture.network.counts.post).toBe(1);
        expect(() => turn.createHostTool()).toThrow();

        fixture.network.allowNextTurn();
        const next = await fixture.createTurn();
        const nextTool = next.createHostTool();
        await expect(
          fixture.track(
            next.runWithHostScope(() => nextTool.execute("next", { text: "next turn" })),
          ),
        ).resolves.toMatchObject({ details: { status: "sent" }, terminate: true });
        expect(next.completion()).toBe("confirmed");
        expect(turn.completion()).toBe("ambiguous");
        expect(fixture.network.counts.post).toBe(2);
      });
    },
  );
});
