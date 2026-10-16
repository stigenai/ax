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

package guest

import (
	"context"
	"net"
	"testing"
	"time"

	ateenvv1alpha "github.com/agent-substrate/env/proto/ateenv/v1alpha"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// hangingProcessService never answers StartProcess, like a router that
// accepted the connection but never responds (AC3: ax ssh must not hang on
// it; StartProcess must fail within ExecOptions.StartTimeout).
type hangingProcessService struct {
	ateenvv1alpha.UnimplementedProcessServiceServer
}

func (hangingProcessService) StartProcess(ctx context.Context, _ *ateenvv1alpha.StartProcessRequest) (*ateenvv1alpha.Process, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

// TestExecRespectsStartTimeout proves Exec returns once StartTimeout elapses,
// instead of hanging forever on a router that accepts a connection but never
// answers StartProcess.
func TestExecRespectsStartTimeout(t *testing.T) {
	lis := bufconn.Listen(1024 * 1024)
	defer lis.Close()

	grpcServer := grpc.NewServer()
	ateenvv1alpha.RegisterProcessServiceServer(grpcServer, hangingProcessService{})
	go grpcServer.Serve(lis)
	defer grpcServer.Stop()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	client := &Client{
		grpcConn: conn,
		process:  ateenvv1alpha.NewProcessServiceClient(conn),
	}

	start := time.Now()
	_, err = client.Exec(context.Background(), ExecOptions{
		Command:      []string{"true"},
		StartTimeout: 200 * time.Millisecond,
	})
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("Exec returned no error for a StartProcess call that never answered")
	}
	if elapsed > 5*time.Second {
		t.Fatalf("Exec took %s to fail; it must respect StartTimeout (200ms) and not hang", elapsed)
	}
}
