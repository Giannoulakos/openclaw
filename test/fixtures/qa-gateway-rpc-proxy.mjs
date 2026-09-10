import { appendFileSync, writeFileSync } from "node:fs";
import { createServer, request } from "node:http";
import { createRequire } from "node:module";
import path from "node:path";
import { pathToFileURL } from "node:url";

export async function startQaGatewayRpcProxy({
  backendPort,
  repoRoot,
  recordPath,
  token,
  port = 0,
  upstreamHeaders,
}) {
  const { WebSocket, WebSocketServer } = createRequire(path.join(repoRoot, "package.json"))("ws");
  const peers = new Set();
  let events = [];
  let sequence = 0;
  let connection = 0;
  let dropResponse = false;
  let holdHello = false;
  let held;
  let holdMethod;
  let heldResponse;
  let heldWaiter;
  const snapshot = () => ({
    events,
    held: Boolean(held),
    heldResponse: heldResponse?.summary,
    pid: process.pid,
  });
  const record = (kind, facts = {}) => {
    if (events.length >= 256) {
      throw new Error("proxy evidence limit exceeded");
    }
    const event = { sequence: ++sequence, kind, ...facts };
    events.push(event);
    if (recordPath) {
      appendFileSync(recordPath, `${JSON.stringify(event)}\n`);
    }
  };
  if (recordPath) {
    writeFileSync(recordPath, "");
  }
  const server = createServer((req, res) => {
    if (req.url === "/__fixture") {
      if (!token || req.headers["x-qa-fixture-token"] !== token) {
        res.writeHead(403).end();
        return;
      }
      void (async () => {
        let text = "";
        for await (const chunk of req) {
          text += chunk;
          if (text.length > 1024) {
            throw new Error("fixture control limit exceeded");
          }
        }
        const input = text ? JSON.parse(text) : {};
        const action = input.action ?? "snapshot";
        if (action === "reset") {
          events = [];
          sequence = 0;
          dropResponse = false;
          if (recordPath) {
            writeFileSync(recordPath, "");
          }
        } else if (action === "hold-response") {
          if (!["users.self", "chat.send"].includes(input.method) || holdMethod || heldResponse) {
            throw new Error("invalid or overlapping response hold");
          }
          holdMethod = input.method;
        } else if (action === "wait-held") {
          if (!heldResponse) {
            if (!holdMethod || heldWaiter) {
              throw new Error("no response hold or another waiter is active");
            }
            await new Promise((resolve, reject) => {
              const timer = setTimeout(() => {
                heldWaiter = undefined;
                reject(new Error("response hold timed out"));
              }, 30_000);
              heldWaiter = (error) => {
                clearTimeout(timer);
                heldWaiter = undefined;
                if (error) {
                  reject(error);
                } else {
                  resolve();
                }
              };
            });
          }
        } else if (action === "release-response") {
          if (!heldResponse) {
            throw new Error("no held response");
          }
          const releasing = heldResponse;
          heldResponse = undefined;
          const delivered = releasing.front.readyState === WebSocket.OPEN;
          if (delivered) {
            releasing.front.send(releasing.raw);
          }
          record("response-released", { ...releasing.summary, delivered });
        } else if (action === "drop-response") {
          dropResponse = true;
        } else if (action === "hold-reconnect") {
          holdHello = true;
          for (const peer of peers) {
            peer.front.terminate();
            peer.back.terminate();
          }
        } else if (action === "release-hello") {
          if (!held) {
            throw new Error("no held hello");
          }
          record("hello-released", { connection: held.connection });
          const releasing = held;
          held = undefined;
          for (const raw of releasing.frames) {
            releasing.front.send(raw);
          }
        } else if (action !== "snapshot") {
          throw new Error("unknown fixture action");
        }
        res.writeHead(200, { "content-type": "application/json" });
        res.end(JSON.stringify(snapshot()));
      })().catch(() => res.writeHead(500).end("fixture control failed"));
      return;
    }
    const upstream = request(
      { hostname: "127.0.0.1", port: Number(backendPort), path: req.url, method: req.method },
      (response) => {
        res.writeHead(response.statusCode ?? 503, response.headers);
        response.pipe(res);
      },
    );
    upstream.on("error", () => res.writeHead(503).end());
    req.pipe(upstream);
  });
  const sockets = new WebSocketServer({ server });
  sockets.on("connection", (front) => {
    const id = ++connection;
    // Native ws:// clients deliberately omit custom headers. This fixture acts
    // as their trusted proxy without changing signed client/device identity.
    const back = new WebSocket(`ws://127.0.0.1:${backendPort}`, {
      headers: upstreamHeaders,
    });
    const peer = { front, back };
    peers.add(peer);
    const methods = new Map();
    const pending = [];
    front.on("message", (raw) => {
      const frame = JSON.parse(raw.toString());
      if (frame.type === "req") {
        if (methods.size >= 128 || pending.length >= 128) {
          front.terminate();
          return;
        }
        methods.set(frame.id, frame.method);
        if (frame.method === "connect") {
          record("connect-request", {
            connection: id,
            clientId: frame.params?.client?.id,
            deviceId: frame.params?.device?.id,
          });
        }
        if (frame.method === "sessions.create") {
          record("mutation-request", { connection: id, requestId: frame.id });
        }
      }
      if (back.readyState === WebSocket.OPEN) {
        back.send(raw);
      } else {
        pending.push(raw);
      }
    });
    back.on("open", () => {
      for (const raw of pending.splice(0)) {
        back.send(raw);
      }
    });
    back.on("message", (raw) => {
      const frame = JSON.parse(raw.toString());
      const method = methods.get(frame.id);
      if (frame.type === "res") {
        methods.delete(frame.id);
        if (method === "connect" && frame.ok) {
          record("connect-success", { connection: id, scopes: frame.payload?.auth?.scopes });
        }
        if (method === "chat.send") {
          record("send-response", {
            connection: id,
            ok: frame.ok,
            runId: frame.payload?.runId,
            status: frame.payload?.status,
          });
        }
        if (holdMethod && method === holdMethod) {
          holdMethod = undefined;
          heldResponse = {
            front,
            raw,
            summary: {
              method,
              connection: id,
              ok: frame.ok,
              runId: frame.payload?.runId,
              status: frame.payload?.status,
            },
          };
          record("response-held", heldResponse.summary);
          heldWaiter?.();
          return;
        }
      }
      if (frame.type === "res" && method === "sessions.create") {
        record(frame.ok ? "mutation-success" : "mutation-error", {
          connection: id,
          requestId: frame.id,
          ...(frame.ok
            ? { key: frame.payload?.key }
            : {
                labelCollision: frame.error?.message?.startsWith("label already in use") === true,
              }),
        });
        if (frame.ok && dropResponse) {
          // A successful real response proves commit before the only injected loss.
          dropResponse = false;
          record("response-dropped", {
            connection: id,
            requestId: frame.id,
            key: frame.payload?.key,
          });
          front.terminate();
          back.terminate();
          return;
        }
      }
      if (frame.type === "res" && method === "connect" && frame.ok && holdHello) {
        holdHello = false;
        held = { connection: id, front, frames: [raw] };
        record("hello-held", { connection: id });
        return;
      }
      if (held?.front === front) {
        if (held.frames.length >= 128) {
          throw new Error("held frame limit exceeded");
        }
        held.frames.push(raw);
      } else if (front.readyState === WebSocket.OPEN) {
        front.send(raw);
      }
    });
    front.on("close", () => {
      back.terminate();
      peers.delete(peer);
      if (held?.front === front) {
        held = undefined;
      }
    });
    back.on("close", () => front.terminate());
    front.on("error", () => back.terminate());
    back.on("error", () => front.terminate());
  });
  let stopping;
  const stop = () =>
    (stopping ??= (async () => {
      heldWaiter?.(new Error("proxy stopped"));
      heldResponse = undefined;
      for (const peer of peers) {
        peer.front.terminate();
        peer.back.terminate();
      }
      server.closeAllConnections();
      await new Promise((resolve) => {
        sockets.close(resolve);
      });
      await new Promise((resolve, reject) => {
        server.close((error) => (error ? reject(error) : resolve()));
      });
    })());
  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(port, "127.0.0.1", resolve);
  });
  const address = server.address();
  return {
    url: `ws://127.0.0.1:${address.port}`,
    controlUrl: `http://127.0.0.1:${address.port}/__fixture`,
    snapshot,
    stop,
  };
}

if (process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href) {
  const [backendPort, repoRoot, recordPath, command, ...args] = process.argv.slice(2);
  if (command === "models") {
    for await (const chunk of process.stdin) {
      // The packaged-bootstrap fixture consumes synthetic auth without retaining it.
      void chunk;
    }
  } else if (command === "update") {
    if (args.includes("--help")) {
      process.stdout.write("--accept-capabilities\n");
    }
  } else if (command === "gateway") {
    const proxy = await startQaGatewayRpcProxy({
      backendPort: Number(backendPort),
      repoRoot,
      recordPath,
      token: process.env.OPENCLAW_GATEWAY_TOKEN,
      port: Number(args[args.indexOf("--port") + 1]),
    });
    const stop = () =>
      void proxy.stop().catch(() => {
        process.exitCode = 1;
      });
    process.once("SIGTERM", stop);
    process.once("SIGINT", stop);
    setTimeout(stop, 240_000).unref();
  } else {
    throw new Error("unexpected proxy fixture command");
  }
}
