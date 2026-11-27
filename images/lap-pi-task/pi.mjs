import { mkdir, writeFile } from "node:fs/promises";
import { join } from "node:path";
import {
  AuthStorage,
  ModelRegistry,
  DefaultResourceLoader,
  SessionManager,
  createAgentSession,
} from "@earendil-works/pi-coding-agent";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import { SSEClientTransport } from "@modelcontextprotocol/sdk/client/sse.js";

export async function createPi(config, workspace) {
  const id = config.model?.id || config.model || "qwen3.6-35b";
  const authStorage = AuthStorage.inMemory();
  const modelRegistry = ModelRegistry.create(authStorage);
  modelRegistry.registerProvider("litellm", {
    baseUrl: "http://litellm.litellm.svc.cluster.local:4000/v1",
    // atenet replaces this placeholder; the sandbox never receives the real key.
    apiKey: process.env.LITELLM_API_KEY || "atenet-injected",
    api: "openai-completions",
    authHeader: true,
    models: [
      {
        id,
        name: id,
        api: "openai-completions",
        reasoning: false,
        input: ["text"],
        cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
        contextWindow: 200000,
        maxTokens: 8192,
      },
    ],
  });
  const skillRoot = join(workspace, ".lap", "skills");
  for (const [index, skill] of (config.skills || []).entries()) {
    if (typeof skill.content !== "string")
      throw new Error("skill content is required");
    const dir = join(skillRoot, String(index));
    await mkdir(dir, { recursive: true });
    await writeFile(join(dir, "SKILL.md"), skill.content);
  }
  const customTools = [];
  for (const server of config.mcp_servers || []) {
    const url = new URL(server.url);
    if (!["http:", "https:"].includes(url.protocol))
      throw new Error("MCP requires an HTTP URL");
    const client = new Client({ name: "lap-ax-task", version: "0.1.0" });
    const Transport =
      server.transport === "sse" || url.pathname.endsWith("/sse")
        ? SSEClientTransport
        : StreamableHTTPClientTransport;
    await client.connect(
      new Transport(url, { requestInit: { headers: server.headers || {} } }),
    );
    for (const tool of (await client.listTools()).tools) {
      if (server.allowed_tools && !server.allowed_tools.includes(tool.name))
        continue;
      customTools.push({
        name: `${server.name}__${tool.name}`,
        label: tool.name,
        description: tool.description || tool.name,
        parameters: tool.inputSchema,
        async execute(_id, args, signal) {
          const result = await client.callTool(
            { name: tool.name, arguments: args },
            undefined,
            { signal },
          );
          if (result.isError) throw new Error("MCP tool failed");
          return { content: result.content, details: {} };
        },
      });
    }
  }
  const resourceLoader = new DefaultResourceLoader({
    cwd: workspace,
    agentDir: join(workspace, ".lap", "agent"),
    additionalSkillPaths: [skillRoot],
    appendSystemPrompt: config.system ? [config.system] : [],
    noThemes: true,
    noPromptTemplates: true,
  });
  await resourceLoader.reload();
  const { session } = await createAgentSession({
    cwd: workspace,
    model: modelRegistry.find("litellm", id),
    modelRegistry,
    authStorage,
    resourceLoader,
    customTools,
    sessionManager: SessionManager.continueRecent(
      workspace,
      join(workspace, ".lap", "sessions"),
    ),
  });
  return session;
}
