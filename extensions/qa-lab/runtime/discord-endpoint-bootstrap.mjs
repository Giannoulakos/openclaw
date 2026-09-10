import fs from "node:fs";

const MAX_DESCRIPTOR_BYTES = 8 * 1024;
const chunks = [];
let totalBytes = 0;

for (;;) {
  const chunk = Buffer.allocUnsafe(Math.min(1024, MAX_DESCRIPTOR_BYTES + 1 - totalBytes));
  const bytesRead = fs.readSync(3, chunk, 0, chunk.length, null);
  if (bytesRead === 0) {
    break;
  }
  totalBytes += bytesRead;
  if (totalBytes > MAX_DESCRIPTOR_BYTES) {
    throw new Error("Discord endpoint bootstrap descriptor exceeds 8 KiB");
  }
  chunks.push(chunk.subarray(0, bytesRead));
}

const descriptor = JSON.parse(Buffer.concat(chunks, totalBytes).toString("utf8"));
const runtimeSetterUrl = new URL(
  "../../../dist/extensions/discord/runtime-setter-api.js",
  import.meta.url,
);
const { installDiscordEndpointRuntime } = await import(runtimeSetterUrl.href);
installDiscordEndpointRuntime(descriptor);
