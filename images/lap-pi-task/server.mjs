import { createServer } from "node:http";
import { timingSafeEqual } from "node:crypto";
import {
  existsSync,
  mkdirSync,
  readFileSync,
  writeFileSync,
  renameSync,
} from "node:fs";
import { join } from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { pathToFileURL } from "node:url";

const exec = promisify(execFile);
function persist(path, value) {
  writeFileSync(`${path}.tmp`, JSON.stringify(value), { mode: 0o600 });
  renameSync(`${path}.tmp`, path);
}

export async function setup(workspace, commands) {
  if (!Array.isArray(commands) || commands.some((c) => typeof c !== "string"))
    throw new Error("setup_commands must be strings");
  const dir = join(workspace, ".lap");
  mkdirSync(dir, { recursive: true });
  const marker = join(dir, "setup.json");
  if (existsSync(marker)) {
    if (JSON.parse(readFileSync(marker, "utf8")).state === "done") return;
    throw new Error(
      "setup was interrupted or failed; inspect the task before retrying",
    );
  }
  persist(marker, { state: "started" });
  for (const command of commands) {
    try {
      await exec("/bin/bash", ["-lc", command], {
        cwd: workspace,
        timeout: 300_000,
        maxBuffer: 1024 * 1024,
      });
    } catch {
      throw new Error("setup command failed or timed out");
    }
  }
  persist(marker, { state: "done" });
}

export function turnServer({ workspace, token, session }) {
  if (!token) throw new Error("LAP_TURN_TOKEN is required");
  const path = join(workspace, ".lap", "turn.json");
  let turn = existsSync(path) ? JSON.parse(readFileSync(path, "utf8")) : null;
  if (turn && !turn.done) {
    turn.events.push({
      type: "error",
      message: "task restarted during a turn; prompt was not retried",
    });
    turn.done = true;
    persist(path, turn);
  }
  let active = false;
  function record(event) {
    turn.events.push(event);
    persist(path, turn);
  }
  const unsubscribe = session.subscribe((event) => {
    if (!active) return;
    if (event.type === "message_end" && event.message?.role === "assistant")
      record(event);
    if (["tool_execution_start", "tool_execution_end"].includes(event.type))
      record(event);
  });
  async function run(text) {
    try {
      await session.prompt(text);
    } catch {
      record({ type: "error", message: "Pi turn failed" });
    } finally {
      record({ type: "idle" });
      turn.done = true;
      persist(path, turn);
      active = false;
    }
  }
  const server = createServer(async (req, res) => {
    const reply = (status, body) => {
      res.writeHead(status, { "content-type": "application/json" });
      res.end(JSON.stringify(body));
    };
    const supplied = Buffer.from(req.headers.authorization || "");
    const expected = Buffer.from(`Bearer ${token}`);
    if (
      supplied.length !== expected.length ||
      !timingSafeEqual(supplied, expected)
    )
      return reply(401, { error: "unauthorized" });
    try {
      const url = new URL(req.url, "http://task");
      if (req.method === "GET" && url.pathname === "/health")
        return reply(200, { ready: true, active });
      if (req.method === "POST" && url.pathname === "/interrupt") {
        if (active) await session.abort();
        return reply(200, { interrupted: true });
      }
      if (req.method === "GET" && url.pathname === "/turn") {
        if (!turn || url.searchParams.get("id") !== turn.id)
          return reply(404, { error: "unknown turn" });
        const after = Number(url.searchParams.get("after") || 0);
        if (
          !Number.isSafeInteger(after) ||
          after < 0 ||
          after > turn.events.length
        )
          return reply(400, { error: "invalid cursor" });
        return reply(200, {
          id: turn.id,
          events: turn.events.slice(after),
          next: turn.events.length,
          done: turn.done,
        });
      }
      if (req.method !== "POST" || url.pathname !== "/turn")
        return reply(404, { error: "not found" });
      let body = "";
      for await (const chunk of req) {
        body += chunk;
        if (Buffer.byteLength(body) > 1024 * 1024)
          return reply(413, { error: "request too large" });
      }
      const input = JSON.parse(body);
      if (
        !/^[a-zA-Z0-9-]{1,80}$/.test(input.id || "") ||
        typeof input.text !== "string" ||
        !input.text.trim()
      )
        return reply(400, { error: "id and text required" });
      if (turn?.id === input.id) return reply(202, { id: turn.id });
      if (active) return reply(409, { error: "turn already running" });
      turn = { id: input.id, done: false, events: [{ type: "running" }] };
      persist(path, turn);
      active = true;
      reply(202, { id: turn.id });
      void run(input.text);
    } catch {
      reply(500, { error: "turn server request failed" });
    }
  });
  server.on("close", unsubscribe);
  return server;
}

if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  const workspace = process.cwd();
  const config = JSON.parse(process.env.LAP_AGENT_CONFIG || "{}");
  await setup(workspace, config.setup_commands || []);
  const { createPi } = await import("./pi.mjs");
  const session = await createPi(config, workspace);
  turnServer({ workspace, token: process.env.LAP_TURN_TOKEN, session }).listen(
    8081,
    "0.0.0.0",
  );
}
