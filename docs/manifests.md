# Writing manifests

All four kinds can live in one multi-document YAML file. See [`examples/task.yaml`](../examples/task.yaml) for a complete, working set.

## Task

```yaml
apiVersion: ax.io/v1alpha1
kind: Task
metadata:
  name: task123
  atespace: default
spec:
  image: "ghcr.io/my-org/my-agent-image"
  command: ["python", "agent.py"]
  env:
    - name: ENVIRONMENT
      value: "production"

  resources:
    requests:
      cpu: "500m"
      memory: "1Gi"
    limits:
      cpu: "2"
      memory: "4Gi"

  workspaces:
    - name: default-workspace
      path: "/workspace"
      goal: "Install dependencies and run the test suite"   # Antigravity prepares the workspace to this goal on first run

  debug: true   # serve guest services inside the sandbox so `ax ssh` works; off by default
  capabilities: ["SETUID", "SETGID"]   # added to the container's default set; see below
  sandboxClass: gvisor   # gvisor (default) or microvm; see below
```

### Capabilities

The task container starts as root with Substrate's default capabilities, `AUDIT_WRITE`, `KILL` and `NET_BIND_SERVICE`, so it cannot change user. `spec.capabilities` adds to that set, named without the `CAP_` prefix, and becomes the container's `securityContext.capabilities.add` in its ActorTemplate. `SETUID` and `SETGID` let a task drop its work to a non-root user, which tools that refuse root (PostgreSQL's `initdb`, say) need. Substrate refuses `ALL` and names it does not know, and the task fails with its reason. The sandbox stays the isolation boundary.

### Sandbox class

`spec.sandboxClass` selects the Substrate sandbox for the task actor. Empty or `gvisor` uses gVisor and the `gvisor-default` SandboxConfig. `microvm` uses a Cloud Hypervisor guest with a real Linux kernel and the cluster-scoped SandboxConfig named `microvm`. Use `microvm` for work that gVisor cannot run, such as Nix builds. A `microvm` task stays pending until a WorkerPool with `sandboxClass: microvm` has a free worker. The Fabriek supervisor profile rejects `microvm`.

A root Nix build inside a `microvm` task also needs capabilities, because Nix changes the owner of `/nix/store` and starts its builds as the `nixbld` users. In a test on 2026-10-26, a build with these capabilities passed: `CHOWN`, `DAC_OVERRIDE`, `FOWNER`, `FSETID`, `SETUID`, `SETGID`, `SETPCAP`, `SETFCAP`, `MKNOD`, `SYS_ADMIN`, `SYS_CHROOT` and `SYS_PTRACE`. A smaller set was not tested.

### Binding several workspaces

`spec.workspaces` takes as many entries as you like, so a task can compose reusable `Workspace` resources, for example the code to work on plus a shared set of tools:

```yaml
spec:
  workspaces:
    - name: my-service          # mounted at /workspace/my-service, the command's working directory
      goal: "Install dependencies and run the test suite"
    - name: team-tools
      path: "/workspace/tools"  # explicit mount path
```

Each entry is set up independently at its own path, in order. Every entry needs a `name`; without a `path` it lands at `/workspace/<name>`, and paths must be unique. The first entry is the working directory of `spec.command`, and the task reports `WorkspaceReady` only once all of them are prepared. See [`examples/multi-workspace.yaml`](../examples/multi-workspace.yaml) for a complete set.

## Workspace

```yaml
apiVersion: ax.io/v1alpha1
kind: Workspace
metadata:
  name: default-workspace
  atespace: default
spec:
  git:
    - name: origin
      repo: "https://github.com/chalk/chalk.git"
      branch: "main"
  mcp:
    registries:
      - provider: google
        query: "mcp.tags:build"
    servers:
      - name: git-tools
        endpoint: "http://git-mcp.default.svc.cluster.local:8080"
  skills:
    registries:
      - provider: google
        query: "skills.tags:nodejs"
    path: "/.agents/skills"
```


## Model

For Google models, store the API key and set `provider: google`.

```bash
kubectl create secret generic gemini-api-secret --from-literal=GEMINI_API_KEY="AIzaSy..."
```

Then reference it from the `Model`:

```yaml
apiVersion: ax.io/v1alpha1
kind: Model
metadata:
  name: default-model
  atespace: default
spec:
  provider: google
  model: gemini-3.8-flash
  secretKey:
    name: gemini-api-secret
    key: GEMINI_API_KEY
```

For Anthropic models, store the key the same way and set `provider: anthropic`.

```bash
kubectl create secret generic anthropic-api-secret --from-literal=ANTHROPIC_API_KEY="sk-ant-..."
```

```yaml
apiVersion: ax.io/v1alpha1
kind: Model
metadata:
  name: claude-model
  atespace: default
spec:
  provider: anthropic
  model: claude-opus-5
  secretKey:
    name: anthropic-api-secret
    key: ANTHROPIC_API_KEY
  parameters:
    maxTokens: 16000
```
