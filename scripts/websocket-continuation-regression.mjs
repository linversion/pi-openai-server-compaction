import assert from "node:assert/strict";
import { once } from "node:events";
import { WebSocketServer } from "ws";
import { createOpenAIWebSocketStreamFn, releaseAllWsSessions, releaseWsSession } from "../src/openai-ws-stream.ts";
import { getContinuationState } from "../src/state.ts";
import { applyPayloadPatch } from "../src/openai.ts";
import { loadConfig } from "../src/config.ts";

process.env.PI_OPENAI_SERVER_COMPACTION_ENABLED = "1";
process.env.PI_OPENAI_SERVER_COMPACTION_PREVIOUS_RESPONSE_ID = "1";
const model = {
  id: "test-model", provider: "openai", api: "openai-responses",
  baseUrl: "https://api.openai.com/v1", reasoning: false, input: ["text"],
  contextWindow: 272000, maxTokens: 128000,
  cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
};
const user = (text) => ({ role: "user", content: [{ type: "text", text }], timestamp: 1 });

async function scenario(name) {
  const server = new WebSocketServer({ port: 0 });
  await once(server, "listening");
  const requests = [];
  let connections = 0;
  let didExpire = false;
  let reconnected;
  const reconnect = new Promise((resolve) => { reconnected = resolve; });
  server.on("connection", (socket) => {
    const connection = ++connections;
    if (connection === 2) {
      // A pong proves the client's open handler ran; the server handshake alone
      // can race the next request and open an extra client connection.
      socket.once("pong", reconnected);
      socket.ping();
    }
    socket.on("message", (raw) => {
      const payload = JSON.parse(raw);
      requests.push({ connection, payload });
      assert.equal(payload.store, false);
      assert.equal(payload.context_management, undefined);
      if (payload.generate === false) assert.deepEqual(payload.input, [], "warmup needs an explicit empty input");
      if (payload.previous_response_id && name.startsWith("expired") && !didExpire) {
        didExpire = true;
        if (name === "expired-after-text") {
          socket.send(JSON.stringify({ type: "response.output_text.delta", delta: "partial" }));
        } else if (name === "expired-after-tool-start") {
          socket.send(JSON.stringify({ type: "response.output_item.added", output_index: 0,
            item: { type: "function_call", call_id: "call_test", name: "probe", arguments: "" } }));
        }
        const error = name === "expired-code"
          ? { code: "previous_response_not_found", message: "Response not found" }
          : { code: "invalid_request_error", message: "Invalid `previous_response_id`." };
        socket.send(JSON.stringify({ type: "error", error }));
        // The real backend can close this socket immediately after rejection.
        if (name === "expired-close") socket.close();
        return;
      }
      if (name === "retry-fails" && requests.length > 1) {
        socket.send(JSON.stringify({ type: "error", error: {
          code: "invalid_request_error", message: "Invalid `previous_response_id`." } }));
        return;
      }
      socket.send(JSON.stringify({ type: "response.completed", response: {
        id: `resp_${requests.length}`, output: [{ type: "message", id: "msg_test", role: "assistant",
          content: [{ type: "output_text", text: "OK" }] }],
      } }));
      if (name === "auto-reconnect" && connection === 1) setImmediate(() => socket.close());
    });
  });
  const sessionId = crypto.randomUUID();
  const stream = createOpenAIWebSocketStreamFn({
    url: `ws://127.0.0.1:${server.address().port}`, maxRetries: 1, backoffDelaysMs: [1],
  });
  const options = {
    apiKey: "test-only", sessionId, transport: "websocket", openaiWsWarmup: name === "warmup",
    onPayload: (payload) => ({
      ...applyPayloadPatch({ payload, model, cfg: loadConfig("/tmp"),
        previousResponseId: getContinuationState(sessionId)?.responseId }),
      // Deliberately emulate an HTTP-oriented hook to exercise the final boundary.
      store: true, context_management: [{ type: "compaction", compact_threshold: 100 }],
    }),
  };
  try {
    const messages = [user("Remember a fact")];
    const first = await stream(model, { messages }, { ...options, signal: AbortSignal.timeout(3000) }).result();
    assert.equal(first.stopReason, "stop");
    if (name === "explicit-reconnect") releaseWsSession(sessionId);
    if (name === "auto-reconnect") await Promise.race([
      reconnect, new Promise((_, reject) => setTimeout(() => reject(Error("reconnect timeout")), 3000).unref()),
    ]);
    const secondStream = stream(model, { messages: [...messages, first, user("Recall it")] },
      { ...options, signal: AbortSignal.timeout(3000) });
    const events = [];
    for await (const event of secondStream) events.push(event);
    const second = await secondStream.result();
    assert.equal(events.filter((event) => event.type === "start").length, 1, "recovery is one logical stream");
    assert.equal(events.filter((event) => event.type === "done" || event.type === "error").length, 1);
    const partial = name === "expired-after-text" || name === "expired-after-tool-start";
    if (partial || name === "retry-fails") {
      assert.equal(second.stopReason, "error");
      assert.match(second.errorMessage, /previous_response_id/);
      assert.equal(requests.length, partial ? 2 : 3, "retry must be bounded and never replay partial output");
    } else {
      assert.equal(second.stopReason, "stop", second.errorMessage);
      if (name === "healthy" || name === "warmup") {
        assert.equal(requests.length, name === "warmup" ? 3 : 2);
        assert.equal(requests.at(-1).payload.previous_response_id, name === "warmup" ? "resp_2" : "resp_1");
      } else {
        assert.equal(requests.at(-1).connection, 2);
        assert.equal(requests.at(-1).payload.previous_response_id, undefined);
        assert.equal(requests.at(-1).payload.input.length, 3, "fresh socket must replay full history");
        assert.equal(requests.length, name.startsWith("expired") ? 3 : 2);
      }
    }
    console.log(`PASS ${name}`);
  } finally {
    releaseAllWsSessions();
    for (const socket of server.clients) socket.terminate();
    await new Promise((resolve) => server.close(resolve));
  }
}
for (const name of ["healthy", "warmup", "explicit-reconnect", "auto-reconnect", "expired", "expired-close",
  "expired-code", "expired-after-text", "expired-after-tool-start", "retry-fails"]) await scenario(name);
