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
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/google/ax/internal/server"
	"github.com/google/ax/internal/store/redis"
	goredis "github.com/redis/go-redis/v9"
	"k8s.io/apimachinery/pkg/api/resource"
)

// resolveMaxQuantity parses a flag or env value for a resource cap. Two
// callers use it: --max-task-memory / AX_MAX_TASK_MEMORY and
// --max-task-cpu / AX_MAX_TASK_CPU. The result is the cap UpdateTask
// enforces for that resource.
//
// Empty or "0" disables the check and returns a zero Quantity.
//
// Any other value must parse as a Kubernetes resource.Quantity above zero.
// If it does not, the server must not start.
func resolveMaxQuantity(value string) (resource.Quantity, error) {
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

// minAuthTokenLen is the shortest AX_AUTH_TOKEN the server accepts.
const minAuthTokenLen = 32

// resolveAuthToken reads the AX_AUTH_TOKEN value. An empty value turns caller
// authentication off. Any other value is trimmed and must then hold at least
// minAuthTokenLen bytes. If it does not, the server must not start.
func resolveAuthToken(value string) (string, error) {
	if value == "" {
		return "", nil
	}
	token := strings.TrimSpace(value)
	if len(token) < minAuthTokenLen {
		return "", fmt.Errorf("must hold at least %d bytes after trimming whitespace", minAuthTokenLen)
	}
	return token, nil
}

func main() {
	var (
		listenAddr    string
		redisAddr     string
		redisPassword string
		maxTaskMemory string
		maxTaskCPU    string
		tlsCertFile   string
		tlsKeyFile    string
		plainAddr     string
	)

	flag.StringVar(&listenAddr, "addr", ":8080", "HTTP listen address")
	flag.StringVar(&redisAddr, "redis-addr", "localhost:6379", "Redis server address")
	flag.StringVar(&redisPassword, "redis-password", "", "Redis password")
	flag.StringVar(&maxTaskMemory, "max-task-memory", "", "Largest memory limit any worker can hold (Kubernetes quantity, e.g. 16Gi); UpdateTask rejects a task above it. Empty or 0 disables the check")
	flag.StringVar(&maxTaskCPU, "max-task-cpu", "", "Largest cpu limit any worker can hold (Kubernetes quantity, e.g. 6); UpdateTask rejects a task above it. Empty or 0 disables the check")
	flag.StringVar(&tlsCertFile, "tls-cert-file", "", "PEM certificate file; with --tls-key-file turns TLS on. Both files reload on change. Plaintext when unset")
	flag.StringVar(&plainAddr, "plain-addr", "", "Extra plaintext listen address, for probes and the ax tunnel; only with TLS on. Off when unset")
	flag.StringVar(&tlsKeyFile, "tls-key-file", "", "PEM private key file for --tls-cert-file")
	flag.Parse()

	if envAddr := os.Getenv("ADDR"); envAddr != "" {
		listenAddr = envAddr
	}
	if envRedis := os.Getenv("REDIS_ADDR"); envRedis != "" {
		redisAddr = envRedis
	}
	if envPass := os.Getenv("REDIS_PASSWORD"); envPass != "" {
		redisPassword = envPass
	}
	if envMax := os.Getenv("AX_MAX_TASK_MEMORY"); envMax != "" {
		maxTaskMemory = envMax
	}
	if envMaxCPU := os.Getenv("AX_MAX_TASK_CPU"); envMaxCPU != "" {
		maxTaskCPU = envMaxCPU
	}
	maxTaskMemoryQuantity, err := resolveMaxQuantity(maxTaskMemory)
	if err != nil {
		slog.Error("invalid --max-task-memory / AX_MAX_TASK_MEMORY", "error", err)
		os.Exit(1)
	}
	maxTaskCPUQuantity, err := resolveMaxQuantity(maxTaskCPU)
	if err != nil {
		slog.Error("invalid --max-task-cpu / AX_MAX_TASK_CPU", "error", err)
		os.Exit(1)
	}

	authToken, err := resolveAuthToken(os.Getenv("AX_AUTH_TOKEN"))
	if err != nil {
		slog.Error("invalid AX_AUTH_TOKEN", "error", err)
		os.Exit(1)
	}

	tlsConfig, err := newTLSConfig(tlsCertFile, tlsKeyFile)
	if err != nil {
		slog.Error("invalid TLS flags", "error", err)
		os.Exit(1)
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	slog.Info("starting ax-server", "listenAddr", listenAddr, "redisAddr", redisAddr, "maxTaskMemory", maxTaskMemoryQuantity.String(), "maxTaskCpu", maxTaskCPUQuantity.String(), "callerAuth", map[bool]string{true: "on", false: "off"}[authToken != ""])
	if authToken == "" {
		slog.Warn("caller authentication is off: any client that reaches ax-server can change tasks; set AX_AUTH_TOKEN")
	}

	rClient := goredis.NewClient(&goredis.Options{
		Addr:     redisAddr,
		Password: redisPassword,
	})
	defer rClient.Close()

	rStore := redis.NewStore(rClient, redis.Options{})
	srv := server.NewServer(rStore)
	srv.MaxTaskMemory = maxTaskMemoryQuantity
	srv.MaxTaskCPU = maxTaskCPUQuantity
	srv.AuthToken = authToken

	httpServer := &http.Server{
		Addr:    listenAddr,
		Handler: srv.Handler(),
	}
	httpServer.Protocols = new(http.Protocols)
	httpServer.Protocols.SetHTTP1(true)
	if tlsConfig != nil {
		httpServer.TLSConfig = tlsConfig
		httpServer.Protocols.SetHTTP2(true)
	} else {
		httpServer.Protocols.SetUnencryptedHTTP2(true)
	}

	var plainServer *http.Server
	if plainAddr != "" {
		if tlsConfig == nil {
			slog.Error("--plain-addr needs TLS: set --tls-cert-file and --tls-key-file, or use --addr")
			os.Exit(1)
		}
		plainServer = newPlainServer(plainAddr, srv.Handler())
		go func() {
			if err := plainServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				slog.Error("plaintext server failed", "error", err)
				os.Exit(1)
			}
		}()
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	go func() {
		serve := httpServer.ListenAndServe
		if tlsConfig != nil {
			serve = func() error { return httpServer.ListenAndServeTLS("", "") }
		}
		if err := serve(); err != nil && err != http.ErrServerClosed {
			slog.Error("HTTP server failed", "error", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	slog.Info("shutting down ax-server")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = httpServer.Shutdown(shutdownCtx)
	if plainServer != nil {
		_ = plainServer.Shutdown(shutdownCtx)
	}
}
