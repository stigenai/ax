package guest

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
)

func TestDialTargetTLSSendsActorHeader(t *testing.T) {
	var got []string
	srv := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		got = md.Get("ate-target-actor")
		return h(ctx, req)
	}))
	healthpb.RegisterHealthServer(srv, health.NewServer())
	ts := httptest.NewUnstartedServer(http.HandlerFunc(srv.ServeHTTP))
	ts.EnableHTTP2 = true
	ts.StartTLS()
	t.Cleanup(ts.Close)
	t.Setenv("AX_CA_FILE", writeCA(t, ts))

	c, err := DialTarget("https://"+ts.Listener.Addr().String(), "ns/actor1")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := healthpb.NewHealthClient(c.grpcConn).Check(ctx, &healthpb.HealthCheckRequest{}); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != "ns/actor1" {
		t.Fatalf("header = %v", got)
	}
}
