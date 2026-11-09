import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setup, turnServer } from "./server.mjs";

async function fixture(t) {
  const workspace = await mkdtemp(join(tmpdir(), "lap-ax-"));
  t.after(() => rm(workspace, { recursive: true, force: true }));
  return workspace;
}
test("setup runs once and refuses retry after a partial failure", async (t) => {
  const workspace = await fixture(t);
  await setup(workspace, ["echo ran >> setup-proof"]);
  await setup(workspace, ["echo ran >> setup-proof"]);
  assert.equal(await readFile(join(workspace, "setup-proof"), "utf8"), "ran\n");
  const failed = await fixture(t);
  await assert.rejects(setup(failed, ["exit 1"]), /failed/);
  await assert.rejects(setup(failed, []), /interrupted/);
});
test("authenticated turns retain early events, reject concurrency, interrupt, and survive restart", async (t) => {
  const workspace = await fixture(t);
  await setup(workspace, []);
  let listener, finish;
  const session = {
    subscribe(fn) {
      listener = fn;
      return () => {};
    },
    prompt: async () => {
      listener({
        type: "message_end",
        message: { role: "user", content: "hidden" },
      });
      await new Promise((r) => {
        finish = r;
      });
    },
    abort: async () => {
      listener({
        type: "message_end",
        message: {
          role: "assistant",
          content: [{ type: "text", text: "stopped" }],
        },
      });
      finish();
    },
  };
  const server = turnServer({ workspace, token: "test-token", session });
  await new Promise((r) => server.listen(0, "127.0.0.1", r));
  t.after(() => server.close());
  const base = `http://127.0.0.1:${server.address().port}`;
  const call = (path, body) =>
    fetch(base + path, {
      method: body ? "POST" : "GET",
      headers: { authorization: "Bearer test-token" },
      ...(body ? { body: JSON.stringify(body) } : {}),
    });
  assert.equal((await fetch(base + "/health")).status, 401);
  assert.equal((await call("/turn", { id: "one", text: "hello" })).status, 202);
  assert.equal((await call("/turn", { id: "one", text: "hello" })).status, 202);
  assert.equal((await call("/turn", { id: "two", text: "hello" })).status, 409);
  assert.deepEqual((await (await call("/turn?id=one")).json()).events, [
    { type: "running" },
  ]);
  await call("/interrupt", {});
  const turn = await (await call("/turn?id=one")).json();
  assert.equal(turn.done, true);
  assert.deepEqual(
    turn.events.map((e) => e.type),
    ["running", "message_end", "idle"],
  );
  assert.equal(
    JSON.parse(await readFile(join(workspace, ".lap/turn.json"), "utf8")).done,
    true,
  );
  assert.equal((await call("/turn?id=one&after=-1")).status, 400);
});
test("restart reports an interrupted turn without repeating its prompt", async (t) => {
  const workspace = await fixture(t);
  await setup(workspace, []);
  const { writeFile } = await import("node:fs/promises");
  await writeFile(
    join(workspace, ".lap/turn.json"),
    JSON.stringify({ id: "lost", done: false, events: [] }),
  );
  const server = turnServer({
    workspace,
    token: "test",
    session: {
      subscribe: () => () => {},
      prompt: () => assert.fail("must not retry"),
    },
  });
  server.close();
  const saved = JSON.parse(
    await readFile(join(workspace, ".lap/turn.json"), "utf8"),
  );
  assert.equal(saved.done, true);
  assert.equal(saved.events[0].type, "error");
});
