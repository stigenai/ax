// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/ax/internal/controller"
	"github.com/google/ax/internal/store/redis"
	"github.com/google/ax/internal/substrate"
	"github.com/google/ax/pkg/apis/v1alpha1"
	goredis "github.com/redis/go-redis/v9"
	"k8s.io/apimachinery/pkg/api/resource"
)

// defaultTaskMemoryDefault is the value of --default-task-memory and
// AX_DEFAULT_TASK_MEMORY when a user sets neither one.
//
// ax-9qu raised this value from 4Gi. A Claude Code session needs more
// memory than 4Gi. A Go or Node build also needs more than 4Gi.
//
// The person set this value by hand at 8Gi, live, on 2026-10-16 03:20Z.
// This commit matches the repo to that live value.
//
// 2*8192Mi equals the 16384Mi (16Gi) limit of each worker exactly. 2 tasks
// fit. No margin is left for ateom's own overhead (about 400Mi). See
// deploy/stigenai/workerpool.yaml for the full sizing math.
const defaultTaskMemoryDefault = "8Gi"

// sandboxCheckIntervalDefault is the --sandbox-check-interval /
// AX_SANDBOX_CHECK_INTERVAL value used when neither is set: a Go duration
// string.
//
// ax-9qu.9 renamed this flag and env var from --liveness-interval /
// AX_LIVENESS_INTERVAL. The old names collided in meaning with the
// pre-existing --task-liveness-interval / AX_TASK_LIVENESS_INTERVAL, which
// controls a different check (LivenessChecker's stale-routing sweep, not
// TaskMonitor's actor-and-runner sweep).
const sandboxCheckIntervalDefault = "30s"

// resolveMaxTaskMemory parses the --max-task-memory / AX_MAX_TASK_MEMORY value.
// The result is the cap Reconcile enforces.
//
// Empty or "0" disables the check and returns a zero Quantity.
//
// Any other value must parse as a Kubernetes resource.Quantity greater than zero.
// If it does not, the controller must not start.
func resolveMaxTaskMemory(value string) (resource.Quantity, error) {
	if value == "0" || value == "" {
		return resource.Quantity{}, nil
	}
	q, err := resource.ParseQuantity(value)
	if err != nil {
		return resource.Quantity{}, err
	}
	if q.Sign() <= 0 {
		return resource.Quantity{}, fmt.Errorf("%q must be greater than zero", value)
	}
	return q, nil
}

// checkDefaultWithinMax reports an error when defaultTaskMemory, the memory
// booked for a task that names no limit, would itself exceed max. A
// deployment with this mismatch would fail every default-sized task on its
// first reconcile, so the controller refuses to start instead. An empty
// defaultTaskMemory or a zero max always passes.
func checkDefaultWithinMax(defaultTaskMemory string, max resource.Quantity) error {
	if defaultTaskMemory == "" || max.IsZero() {
		return nil
	}
	q, err := resource.ParseQuantity(defaultTaskMemory)
	if err != nil {
		return err
	}
	if q.Cmp(max) > 0 {
		return fmt.Errorf("default task memory %q exceeds max task memory %q", defaultTaskMemory, max.String())
	}
	return nil
}

// resolveDefaultTaskMemory applies the AX_DEFAULT_TASK_MEMORY override (the
// caller does that before calling this) and validates the result. "0"
// disables the default and returns "". Any other value must parse as a
// Kubernetes resource.Quantity greater than zero, like a task's own
// spec.resources.limits.memory (pkg/apis/v1alpha1/types.go
// validateQuantity), or the controller must not start.
func resolveDefaultTaskMemory(value string) (string, error) {
	if value == "0" || value == "" {
		return "", nil
	}
	q, err := resource.ParseQuantity(value)
	if err != nil {
		return "", err
	}
	if q.Sign() <= 0 {
		return "", fmt.Errorf("%q must be greater than zero", value)
	}
	return value, nil
}

// resolveSandboxCheckInterval parses the --sandbox-check-interval /
// AX_SANDBOX_CHECK_INTERVAL value as a Go duration (for example "30s"). The
// caller treats zero or a negative result as disabled, instead of as an
// error. So "0", "0s" and "-1s" all disable the monitor.
func resolveSandboxCheckInterval(value string) (time.Duration, error) {
	return time.ParseDuration(value)
}

// deleteSweepIntervalDefault is the --delete-sweep-interval /
// AX_DELETE_SWEEP_INTERVAL value used when neither is set.
//
// The ax-8qz postmortem (ax-706) found actors stuck in
// ACTOR_STATE_DELETING for about 8.5 hours. No alert or sweep caught
// them. 5 minutes bounds how long a dropped retry can go unnoticed
// before DeletionSweeper republishes it.
const deleteSweepIntervalDefault = 5 * time.Minute

// deleteSweepMinAgeDefault is the --delete-sweep-min-age /
// AX_DELETE_SWEEP_MIN_AGE value used when neither is set: how long a
// Terminating task or a DELETING actor must sit before DeletionSweeper
// counts and acts on it. 15 minutes matches the postmortem's own
// threshold for "stuck" (ax-706).
const deleteSweepMinAgeDefault = 15 * time.Minute

// resolveDeleteSweepInterval reads the AX_DELETE_SWEEP_INTERVAL override
// for the --delete-sweep-interval flag. The caller passes the env value.
// An empty envValue keeps flagValue. The value "0" (or env "0") turns off
// the sweep.
func resolveDeleteSweepInterval(flagValue time.Duration, envValue string) (time.Duration, error) {
	if envValue == "" {
		return flagValue, nil
	}
	d, err := time.ParseDuration(envValue)
	if err != nil {
		return 0, fmt.Errorf("invalid duration %q: %w", envValue, err)
	}
	return d, nil
}

// resolveDeleteSweepMinAge reads the AX_DELETE_SWEEP_MIN_AGE override for
// the --delete-sweep-min-age flag. The caller passes the env value. An
// empty envValue keeps flagValue. Unlike the sweep interval, a 0 or
// negative result is always an error, never a way to turn the age check
// off: the controller must not start with a bad value here.
func resolveDeleteSweepMinAge(flagValue time.Duration, envValue string) (time.Duration, error) {
	d := flagValue
	if envValue != "" {
		parsed, err := time.ParseDuration(envValue)
		if err != nil {
			return 0, fmt.Errorf("invalid duration %q: %w", envValue, err)
		}
		d = parsed
	}
	if d <= 0 {
		return 0, fmt.Errorf("must be greater than zero, got %v", d)
	}
	return d, nil
}

// resolveTaskLivenessInterval reads the AX_TASK_LIVENESS_INTERVAL override
// for the --task-liveness-interval flag. The caller passes the env value.
// An empty envValue keeps flagValue. The value "0" turns off the sweep.
func resolveTaskLivenessInterval(flagValue time.Duration, envValue string) (time.Duration, error) {
	if envValue == "" {
		return flagValue, nil
	}
	d, err := time.ParseDuration(envValue)
	if err != nil {
		return 0, fmt.Errorf("invalid duration %q: %w", envValue, err)
	}
	return d, nil
}

// taskLivenessSweepEnabled reports whether main must start the LivenessChecker
// sweep goroutine. The sweep needs a router address to probe through
// (atenet-router, read from ATENET_ROUTER_ADDR) and an interval greater than
// zero. If either is missing, the sweep stays off.
func taskLivenessSweepEnabled(routerAddr string, interval time.Duration) bool {
	return routerAddr != "" && interval > 0
}

func main() {
	var (
		substrateEndpoint       string
		substrateAuthority      string
		substrateTokenFile      string
		substrateCAFile         string
		substrateInsecureTLS    bool
		substratePlaintext      bool
		defaultTemplate         string
		defaultTemplateAtespace string
		defaultTaskMemory       string
		maxTaskMemory           string
		sandboxCheckInterval    string
		redisAddr               string
		redisPassword           string
		redisGroup              string
		redisConsumer           string
		nativeTLSDestination    string
		taskLivenessInterval    time.Duration
		taskLivenessThreshold   int
		deleteSweepInterval     time.Duration
		deleteSweepMinAge       time.Duration
	)

	flag.StringVar(&redisAddr, "redis-addr", "localhost:6379", "Redis server address (e.g. localhost:6379)")
	flag.StringVar(&redisPassword, "redis-password", "", "Redis password")
	flag.StringVar(&redisGroup, "redis-group", "ax-controllers", "Redis stream consumer group")
	flag.StringVar(&redisConsumer, "redis-consumer", "", "Redis stream consumer ID (defaults to hostname)")
	flag.StringVar(&substrateEndpoint, "substrate-endpoint", "api.ate-system.svc.cluster.local:443", "Agent Substrate Control API endpoint")
	flag.StringVar(&substrateAuthority, "substrate-authority", "api.ate-system.svc", "Authority / TLS ServerName for Substrate endpoint")
	flag.StringVar(&substrateTokenFile, "substrate-token-file", "", "Path to bearer token file for Substrate auth")
	flag.StringVar(&substrateCAFile, "substrate-ca-file", "", "Path to CA PEM file for Substrate TLS")
	flag.BoolVar(&substrateInsecureTLS, "substrate-insecure-tls", false, "Skip Substrate TLS verification")
	flag.BoolVar(&substratePlaintext, "substrate-plaintext", false, "Use insecure plaintext gRPC connection to Substrate")
	flag.StringVar(&defaultTemplate, "template", "default-template", "Default Substrate ActorTemplate name")
	flag.StringVar(&defaultTemplateAtespace, "template-atespace", "ax-system", "Default Substrate ActorTemplate atespace")
	flag.StringVar(&defaultTaskMemory, "default-task-memory", defaultTaskMemoryDefault, "Memory limit booked for a task that names none (Substrate quantity, e.g. 8Gi); 0 disables")
	flag.StringVar(&maxTaskMemory, "max-task-memory", "", "Largest memory limit any worker can hold (Kubernetes quantity, e.g. 16Gi); Reconcile fails a task above it instead of waiting for capacity. Empty or 0 disables the check")
	flag.StringVar(&sandboxCheckInterval, "sandbox-check-interval", sandboxCheckIntervalDefault, "How often TaskMonitor checks each Running task's actor and runner (Go duration, e.g. 30s); 0 disables it (env AX_SANDBOX_CHECK_INTERVAL)")
	flag.StringVar(&nativeTLSDestination, "native-tls-destination", "", "Private protected-actor TLS destination; install or verify exact policy before resume")
	flag.DurationVar(&taskLivenessInterval, "task-liveness-interval", time.Minute, "How often to probe each Running task's actor through atenet-router; 0 turns off the sweep (env AX_TASK_LIVENESS_INTERVAL)")
	flag.IntVar(&taskLivenessThreshold, "task-liveness-threshold", controller.DefaultLivenessThreshold, "Consecutive stale probes that mark a Running task Failed")
	flag.DurationVar(&deleteSweepInterval, "delete-sweep-interval", deleteSweepIntervalDefault, "How often DeletionSweeper checks for stuck Terminating tasks and stuck DELETING actors; 0 disables it (env AX_DELETE_SWEEP_INTERVAL)")
	flag.DurationVar(&deleteSweepMinAge, "delete-sweep-min-age", deleteSweepMinAgeDefault, "How long a Terminating task or a DELETING actor must sit before DeletionSweeper counts and acts on it; must be greater than zero (env AX_DELETE_SWEEP_MIN_AGE)")
	flag.Parse()

	if envRedis := os.Getenv("REDIS_ADDR"); envRedis != "" {
		redisAddr = envRedis
	}
	if envPass := os.Getenv("REDIS_PASSWORD"); envPass != "" && redisPassword == "" {
		redisPassword = envPass
	}
	if envImage := os.Getenv("AX_DEFAULT_TASK_IMAGE"); envImage != "" {
		v1alpha1.DefaultTaskImage = envImage
	}
	if envMemory := os.Getenv("AX_DEFAULT_TASK_MEMORY"); envMemory != "" {
		defaultTaskMemory = envMemory
	}
	if envSandboxCheck := os.Getenv("AX_SANDBOX_CHECK_INTERVAL"); envSandboxCheck != "" {
		sandboxCheckInterval = envSandboxCheck
	}
	defaultTaskMemory, err := resolveDefaultTaskMemory(defaultTaskMemory)
	if err != nil {
		slog.Error("invalid --default-task-memory / AX_DEFAULT_TASK_MEMORY", "error", err)
		os.Exit(1)
	}
	if envMax := os.Getenv("AX_MAX_TASK_MEMORY"); envMax != "" {
		maxTaskMemory = envMax
	}
	maxTaskMemoryQuantity, err := resolveMaxTaskMemory(maxTaskMemory)
	if err != nil {
		slog.Error("invalid --max-task-memory / AX_MAX_TASK_MEMORY", "error", err)
		os.Exit(1)
	}
	if err := checkDefaultWithinMax(defaultTaskMemory, maxTaskMemoryQuantity); err != nil {
		slog.Error("default task memory cannot exceed max task memory", "error", err)
		os.Exit(1)
	}
	sandboxCheckIntervalDuration, err := resolveSandboxCheckInterval(sandboxCheckInterval)
	if err != nil {
		slog.Error("invalid --sandbox-check-interval / AX_SANDBOX_CHECK_INTERVAL", "error", err)
		os.Exit(1)
	}
	taskLivenessInterval, err = resolveTaskLivenessInterval(taskLivenessInterval, os.Getenv("AX_TASK_LIVENESS_INTERVAL"))
	if err != nil {
		slog.Error("invalid --task-liveness-interval / AX_TASK_LIVENESS_INTERVAL", "error", err)
		os.Exit(1)
	}
	deleteSweepInterval, err = resolveDeleteSweepInterval(deleteSweepInterval, os.Getenv("AX_DELETE_SWEEP_INTERVAL"))
	if err != nil {
		slog.Error("invalid --delete-sweep-interval / AX_DELETE_SWEEP_INTERVAL", "error", err)
		os.Exit(1)
	}
	deleteSweepMinAge, err = resolveDeleteSweepMinAge(deleteSweepMinAge, os.Getenv("AX_DELETE_SWEEP_MIN_AGE"))
	if err != nil {
		slog.Error("invalid --delete-sweep-min-age / AX_DELETE_SWEEP_MIN_AGE", "error", err)
		os.Exit(1)
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	slog.Info("starting ax-controller",
		"redisAddr", redisAddr,
		"group", redisGroup,
		"substrateEndpoint", substrateEndpoint,
		"authority", substrateAuthority,
		"templateAtespace", defaultTemplateAtespace,
		"template", defaultTemplate,
		"defaultTaskMemory", defaultTaskMemory,
		"maxTaskMemory", maxTaskMemoryQuantity.String(),
		"sandboxCheckInterval", sandboxCheckIntervalDuration,
	)

	subClient, err := substrate.NewClientWithOptions(substrate.ClientOptions{
		Target:      substrateEndpoint,
		Authority:   substrateAuthority,
		TokenFile:   substrateTokenFile,
		CAFile:      substrateCAFile,
		InsecureTLS: substrateInsecureTLS,
		Plaintext:   substratePlaintext,
	})
	if err != nil {
		slog.Error("failed to initialize substrate client", "error", err)
		os.Exit(1)
	}
	defer subClient.Close()

	reconciler := controller.NewTaskReconciler(subClient, defaultTemplate, defaultTemplateAtespace)
	reconciler.DefaultTaskMemory = defaultTaskMemory
	reconciler.MaxTaskMemory = maxTaskMemoryQuantity
	if nativeTLSDestination != "" {
		if err := reconciler.ConfigureNativeTLS(nativeTLSDestination, os.Getenv("POD_NAMESPACE"), substrateEndpoint, substrateAuthority, substrateInsecureTLS || substratePlaintext); err != nil {
			slog.Error("invalid native TLS configuration", "error", err)
			os.Exit(1)
		}
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	rClient := goredis.NewClient(&goredis.Options{
		Addr:     redisAddr,
		Password: redisPassword,
	})
	defer rClient.Close()

	rStore := redis.NewStore(rClient, redis.Options{})

	routerAddr := os.Getenv("ATENET_ROUTER_ADDR")
	livenessSweepEnabled := taskLivenessSweepEnabled(routerAddr, taskLivenessInterval)

	monitor := controller.NewTaskMonitor(rStore, reconciler, sandboxCheckIntervalDuration)
	// ax-9qu.16: set these fields once here. checkTask no longer reads
	// ATENET_ROUTER_ADDR itself for every task in every sweep.
	monitor.RouterAddr = routerAddr
	// ax-9qu.17: defer a stale-assignment 421 to LivenessChecker only when
	// that sweep will run. Otherwise, TaskMonitor is the only sweep that
	// can catch a stale-routed task. It must count the 421 itself, not
	// drop it.
	monitor.DeferStaleAssignment = livenessSweepEnabled

	sandboxCheckLogArgs := []any{
		"interval", sandboxCheckIntervalDuration,
		"routerAddr", routerAddr,
	}
	if routerAddr == "" {
		// ax-9qu.16: log this note only when ATENET_ROUTER_ADDR is unset.
		// It names the real cause only in that case.
		sandboxCheckLogArgs = append(sandboxCheckLogArgs, "note", "SandboxUnresponsive stays off until ATENET_ROUTER_ADDR is set (ax-9qu.10)")
	}
	slog.Info("sandbox check monitor", sandboxCheckLogArgs...)
	go monitor.Run(ctx)

	if livenessSweepEnabled {
		checker := controller.NewLivenessChecker(rStore, subClient, routerAddr)
		checker.Threshold = taskLivenessThreshold
		slog.Info("task liveness sweep",
			"interval", taskLivenessInterval,
			"threshold", taskLivenessThreshold,
			"routerAddr", routerAddr,
		)
		go func() {
			ticker := time.NewTicker(taskLivenessInterval)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					checker.SweepOnce(ctx)
				}
			}
		}()
	} else {
		slog.Info("task liveness sweep disabled", "routerAddr", routerAddr, "interval", taskLivenessInterval)
	}

	if deleteSweepInterval > 0 {
		sweeper := &controller.DeletionSweeper{
			Store:  rStore,
			Actors: subClient,
			MinAge: deleteSweepMinAge,
		}
		slog.Info("delete sweep",
			"interval", deleteSweepInterval,
			"minAge", deleteSweepMinAge,
		)
		go func() {
			ticker := time.NewTicker(deleteSweepInterval)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					sweeper.SweepOnce(ctx)
				}
			}
		}()
	} else {
		slog.Info("delete sweep disabled", "interval", deleteSweepInterval)
	}

	worker := controller.NewWorker(rStore, reconciler, redisGroup, redisConsumer)
	if err := worker.Run(ctx); err != nil && err != context.Canceled {
		slog.Error("redis worker stopped with error", "error", err)
		os.Exit(1)
	}
}
