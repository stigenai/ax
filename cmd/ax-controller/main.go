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
// ax-9qu raised this value from 4Gi. A Claude Code session plus a Go or Node
// build needs more memory than 4Gi. 7680Mi still fits 2 tasks on each 16Gi
// worker. The sum is 2*7680Mi plus about 400Mi for ateom overhead. The total,
// 15760Mi, stays under the 16384Mi limit. See
// deploy/stigenai/workerpool.yaml for the full sizing math.
const defaultTaskMemoryDefault = "7680Mi"

// livenessIntervalDefault is the --liveness-interval / AX_LIVENESS_INTERVAL
// value used when neither is set: a Go duration string.
const livenessIntervalDefault = "30s"

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

// resolveLivenessInterval parses the --liveness-interval / AX_LIVENESS_INTERVAL
// value as a Go duration (for example "30s"). The caller treats zero or a
// negative result as disabled, instead of as an error. So "0", "0s" and
// "-1s" all disable the monitor.
func resolveLivenessInterval(value string) (time.Duration, error) {
	return time.ParseDuration(value)
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
		livenessInterval        string
		redisAddr               string
		redisPassword           string
		redisGroup              string
		redisConsumer           string
		nativeTLSDestination    string
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
	flag.StringVar(&defaultTaskMemory, "default-task-memory", defaultTaskMemoryDefault, "Memory limit booked for a task that names none (Substrate quantity, e.g. 7680Mi); 0 disables")
	flag.StringVar(&livenessInterval, "liveness-interval", livenessIntervalDefault, "How often the liveness monitor checks each Running task's actor and runner (Go duration, e.g. 30s); 0 disables it")
	flag.StringVar(&nativeTLSDestination, "native-tls-destination", "", "Private protected-actor TLS destination; install or verify exact policy before resume")
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
	if envLiveness := os.Getenv("AX_LIVENESS_INTERVAL"); envLiveness != "" {
		livenessInterval = envLiveness
	}
	defaultTaskMemory, err := resolveDefaultTaskMemory(defaultTaskMemory)
	if err != nil {
		slog.Error("invalid --default-task-memory / AX_DEFAULT_TASK_MEMORY", "error", err)
		os.Exit(1)
	}
	livenessIntervalDuration, err := resolveLivenessInterval(livenessInterval)
	if err != nil {
		slog.Error("invalid --liveness-interval / AX_LIVENESS_INTERVAL", "error", err)
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
		"livenessInterval", livenessIntervalDuration,
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

	monitor := controller.NewTaskMonitor(rStore, reconciler, livenessIntervalDuration)
	go monitor.Run(ctx)

	worker := controller.NewWorker(rStore, reconciler, redisGroup, redisConsumer)
	if err := worker.Run(ctx); err != nil && err != context.Canceled {
		slog.Error("redis worker stopped with error", "error", err)
		os.Exit(1)
	}
}
