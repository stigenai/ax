import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createPi } from "./pi.mjs";

test("real Pi loop uses the session key and configured model through OpenAI completions", async (t) => {
  const workspace = await mkdtemp(join(tmpdir(), "lap-ax-pi-"));
  t.after(() => rm(workspace, { recursive: true, force: true }));
  const originalFetch = globalThis.fetch;
  const originalKey = process.env.LITELLM_API_KEY;
  process.env.LITELLM_API_KEY = "sk-session-test";
  t.after(() => {
    globalThis.fetch = originalFetch;
    if (originalKey === undefined) delete process.env.LITELLM_API_KEY;
    else process.env.LITELLM_API_KEY = originalKey;
  });
  let requests = 0;
  globalThis.fetch = async (input, init) => {
    const request = new Request(input, init);
    assert.equal(
      request.url,
      "http://litellm.litellm.svc.cluster.local:4000/v1/chat/completions",
    );
    assert.equal(
      request.headers.get("authorization"),
      "Bearer sk-session-test",
    );
    const body = await request.json();
    assert.equal(body.model, "qwen3.6-35b");
    requests++;
    const chunk = (delta, finish_reason = null) =>
      `data: ${JSON.stringify({ id: "test", object: "chat.completion.chunk", created: 1, model: body.model, choices: [{ index: 0, delta, finish_reason }] })}\n\n`;
    return new Response(
      chunk({ role: "assistant", content: "hello from AX" }) +
        chunk({}, "stop") +
        "data: [DONE]\n\n",
      { headers: { "content-type": "text/event-stream" } },
    );
  };
  const session = await createPi(
    { model: "qwen3.6-35b", system: "Answer briefly." },
    workspace,
  );
  t.after(() => session.dispose());
  const messages = [];
  session.subscribe((event) => {
    if (event.type === "message_end" && event.message.role === "assistant")
      messages.push(event.message);
  });
  await session.prompt("hello");
  assert.equal(requests, 1);
  assert.equal(messages[0].content[0].text, "hello from AX");
  assert.notEqual(messages[0].stopReason, "error");
});
