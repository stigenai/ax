# Protected Fabriek supervisors

`spec.supervisorProfile: fabriek-native-v1` opts a Task into Substrate's protected
activation bootstrap. Ordinary AX tasks retain their existing behavior.

This profile requires a digest-pinned image, `debug: false`, the fixed command
`/usr/local/bin/python -m factory.execution.ax_worker serve`, explicit CPU/memory
limits, and only `FACTORY_ATTEMPT_ID` and `FACTORY_NATIVE_LAUNCH_DIGEST` in task
environment. Workspace goals and nonempty Workspace specs are rejected before
provisioning. Task-supplied capabilities are rejected, including unknown names.
The protected root supervisor receives only `CHOWN`, `DAC_OVERRIDE`, `FOWNER`,
`SETGID` and `SETUID` in addition to Substrate defaults (`AUDIT_WRITE`, `KILL`,
`NET_BIND_SERVICE`), for UID isolation and native scratch cleanup. The native
UID must have no effective, permitted or ambient capabilities. Ordinary Tasks
retain their existing `spec.capabilities` behavior.

The controller does not fetch or inject its shared Gemini key.

Protected templates select only WorkerPools labeled
`fabriek.dev/supervisor-profile: fabriek-native-v1`. Without a matching pool,
Substrate cannot place the actor; AX does not fall back to an unlabeled pool.
A retained protected template with a missing or different selector is rejected
before any actor is created or resumed; it is not rewritten or deleted.

Provision a separate pool with that metadata label and a digest-pinned worker
image and node version that support the protected bootstrap. Keep any serving
pool unchanged while actors are active; adding this label to an old pool would
advertise support it does not have. The candidate pool's placement label is:

```yaml
metadata:
  labels:
    fabriek.dev/supervisor-profile: fabriek-native-v1
```

Ordinary AX templates keep their existing unconstrained placement, so this label
does not make the pool exclusive or replace private API and writer controls.
Pool provisioning and gVisor placement qualification are separate deployment steps.

AX mounts the dedicated SystemInfo projection at
`/run/substrate/fabriek-supervisor`. Substrate owns `activation.jwt`, its root-only
permissions, freshness on activation, and restricted issuance. No credential is
stored in Task environment or metadata. The qualified worker image supplies the
public controller trust configuration; the root supervisor exchanges the platform
proof before any native child can receive a boot permit.

The template carries actual container CPU/memory limits and Data-only pause/commit
with Golden resume. Substrate must ensure golden actors contain no credentials
and reject Full restore for this profile. AX checks a retained template against
the immutable requested spec and rejects an existing actor bound to another
template. It does not automatically delete a protected crashed actor.

This profile pins [Substrate bootstrap PR #1](https://github.com/stigenai/substrate/pull/1)
at `6fbe6ab2aeed95bef9aa9240f02232cbbacf48e6` through the published
`github.com/stigenai/substrate` Go module replacement. It needs that platform
version deployed and a separately
qualified Fabriek worker image. A private AX endpoint with exclusive authorized
writers remains required: this opt-in field is not API authorization. Unit tests
and a successful build do not qualify gVisor UID isolation, Golden/Data restore,
hosted account routing or factory delivery. No deployment is activated here.
