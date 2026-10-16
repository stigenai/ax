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

// taskLivenessSweepEnabled reports whether main must start the liveness
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
		redisAddr               string
		redisPassword           string
		redisGroup              string
		redisConsumer           string
		nativeTLSDestination    string
		taskLivenessInterval    time.Duration
		taskLivenessThreshold   int
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
	flag.StringVar(&defaultTaskMemory, "default-task-memory", "4Gi", "Memory limit booked for a task that names none (Substrate quantity, e.g. 4Gi); 0 disables")
	flag.StringVar(&nativeTLSDestination, "native-tls-destination", "", "Private protected-actor TLS destination; install or verify exact policy before resume")
	flag.DurationVar(&taskLivenessInterval, "task-liveness-interval", time.Minute, "How often to probe each Running task's actor through atenet-router; 0 turns off the sweep (env AX_TASK_LIVENESS_INTERVAL)")
	flag.IntVar(&taskLivenessThreshold, "task-liveness-threshold", controller.DefaultLivenessThreshold, "Consecutive stale probes that mark a Running task Failed")
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
	defaultTaskMemory, err := resolveDefaultTaskMemory(defaultTaskMemory)
	if err != nil {
		slog.Error("invalid --default-task-memory / AX_DEFAULT_TASK_MEMORY", "error", err)
		os.Exit(1)
	}
	taskLivenessInterval, err = resolveTaskLivenessInterval(taskLivenessInterval, os.Getenv("AX_TASK_LIVENESS_INTERVAL"))
	if err != nil {
		slog.Error("invalid --task-liveness-interval / AX_TASK_LIVENESS_INTERVAL", "error", err)
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
	if taskLivenessSweepEnabled(routerAddr, taskLivenessInterval) {
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

	worker := controller.NewWorker(rStore, reconciler, redisGroup, redisConsumer)
	if err := worker.Run(ctx); err != nil && err != context.Canceled {
		slog.Error("redis worker stopped with error", "error", err)
		os.Exit(1)
	}
}
