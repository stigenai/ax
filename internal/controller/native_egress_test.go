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

package controller_test

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/ax/internal/controller"
	"github.com/google/ax/internal/substrate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const privateDestination = "factory-native-api.fabriek-m1-reviewer.svc:8443"

type nativePolicyServer struct {
	mockControlServer
	actor                  *ateapipb.Actor
	policy                 *ateapipb.EgressPolicy
	getErr, createErr      error
	race, replace          bool
	ordinary, conflictRace bool
	creates, reads         int
}

func (s *nativePolicyServer) CreateActor(_ context.Context, r *ateapipb.CreateActorRequest) (*ateapipb.Actor, error) {
	if s.actor != nil {
		return nil, status.Error(codes.AlreadyExists, "retained")
	}
	s.actor = proto.CloneOf(r.Actor)
	s.actor.Metadata.Uid = "retained-uid"
	s.actor.Status = &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED}
	return proto.CloneOf(s.actor), nil
}
func (s *nativePolicyServer) GetActor(context.Context, *ateapipb.GetActorRequest) (*ateapipb.Actor, error) {
	a := proto.CloneOf(s.actor)
	if s.replace && s.reads > 0 {
		a.Metadata.Uid = "replacement"
	}
	return a, nil
}
func (s *nativePolicyServer) GetActorEgressPolicy(context.Context, *ateapipb.GetActorEgressPolicyRequest) (*ateapipb.EgressPolicy, error) {
	s.reads++
	if s.getErr != nil {
		return nil, s.getErr
	}
	if s.policy == nil {
		return nil, status.Error(codes.NotFound, "absent")
	}
	return proto.CloneOf(s.policy), nil
}
func (s *nativePolicyServer) CreateActorEgressPolicy(_ context.Context, r *ateapipb.CreateActorEgressPolicyRequest) (*ateapipb.EgressPolicy, error) {
	s.creates++
	s.policy = proto.CloneOf(r.EgressPolicy)
	if s.conflictRace {
		s.policy.Rules[0].TlsPassthrough.Hostnames = []string{"wrong.example"}
	}
	if s.createErr != nil {
		return nil, s.createErr
	}
	if s.race {
		return nil, status.Error(codes.AlreadyExists, "raced")
	}
	return proto.CloneOf(s.policy), nil
}
func (s *nativePolicyServer) ResumeActor(ctx context.Context, r *ateapipb.ResumeActorRequest) (*ateapipb.ResumeActorResponse, error) {
	if s.policy == nil && !s.ordinary {
		return nil, status.Error(codes.FailedPrecondition, "policy missing at resume")
	}
	s.actor.Status.State = ateapipb.ActorState_ACTOR_STATE_RUNNING
	return s.mockControlServer.ResumeActor(ctx, r)
}
func nativePolicyReconciler(t *testing.T, s *nativePolicyServer) *controller.TaskReconciler {
	t.Helper()
	l, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	server := grpc.NewServer()
	ateapipb.RegisterControlServer(server, s)
	go server.Serve(l)
	t.Cleanup(server.Stop)
	client, e := substrate.NewClient(l.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = client.Close() })
	r := controller.NewTaskReconciler(client, "test-template", "fabriek-native")
	r.SecretResolver = noSecrets
	r.WorkspaceReadyTimeout = time.Millisecond
	if e := r.ConfigureNativeTLS(privateDestination, "fabriek-native", "api.fabriek-native.svc.cluster.local:443", "api.fabriek-native.svc", false); e != nil {
		t.Fatal(e)
	}
	return r
}
func TestNativePolicyPreparedBeforeResumeAndReplayed(t *testing.T) {
	for _, name := range []string{"create", "race", "lost response"} {
		t.Run(name, func(t *testing.T) {
			s := &nativePolicyServer{race: name == "race"}
			r := nativePolicyReconciler(t, s)
			task := supervisorTask()
			task.Metadata.Atespace = "fabriek-native"
			if name == "lost response" {
				s.createErr = status.Error(codes.DeadlineExceeded, "unknown")
				if _, e := r.Reconcile(t.Context(), task); e == nil {
					t.Fatal("unknown create accepted")
				}
				if len(s.resumedActors) != 0 {
					t.Fatal("resumed after unknown create")
				}
				s.createErr = nil
			}
			for range 2 {
				if _, e := r.Reconcile(t.Context(), task); e != nil {
					t.Fatal(e)
				}
			}
			if s.creates != 1 || len(s.resumedActors) != 2 {
				t.Fatalf("creates=%d resumes=%d", s.creates, len(s.resumedActors))
			}
			desired, _ := substrate.NativeTLSPolicy(privateDestination)
			if !proto.Equal(&ateapipb.EgressPolicy{Rules: s.policy.Rules}, desired) {
				t.Fatal("policy widened")
			}
		})
	}
}
func TestNativePolicyDenialsNeverResume(t *testing.T) {
	for _, name := range []string{"lookup unavailable", "conflict", "create denied", "actor replaced", "running missing", "wrong atespace", "conflicting create race"} {
		t.Run(name, func(t *testing.T) {
			s := &nativePolicyServer{}
			r := nativePolicyReconciler(t, s)
			task := supervisorTask()
			task.Metadata.Atespace = "fabriek-native"
			switch name {
			case "conflicting create race":
				s.race, s.conflictRace = true, true
			case "lookup unavailable":
				s.getErr = status.Error(codes.Unavailable, "unavailable")
			case "conflict":
				s.policy = &ateapipb.EgressPolicy{Metadata: &ateapipb.ResourceMetadata{Atespace: "fabriek-native", Name: "default"}, Rules: []*ateapipb.EgressRule{{TlsPassthrough: &ateapipb.TLSPassthroughRule{Hostnames: []string{"wrong.example"}, Ports: &ateapipb.Ports{Numbers: []int32{8443}}}}}}
			case "create denied":
				s.createErr = status.Error(codes.PermissionDenied, "denied")
			case "actor replaced":
				s.replace = true
			case "running missing":
				if _, e := r.Reconcile(t.Context(), task); e != nil {
					t.Fatal(e)
				}
				s.policy = nil
				s.resumedActors = nil
			case "wrong atespace":
				task.Metadata.Atespace = "ordinary"
			}
			before := s.creates
			if _, e := r.Reconcile(t.Context(), task); e == nil {
				t.Fatal("invalid policy authority accepted")
			}
			if len(s.resumedActors) != 0 {
				t.Fatal("resumed without exact policy")
			}
			if name == "running missing" && s.creates != before {
				t.Fatal("revoked policy recreated for running actor")
			}
			if name == "wrong atespace" && (len(s.createdAtespaces) != 0 || s.actor != nil) {
				t.Fatal("wrong atespace reached provisioning")
			}
		})
	}
}
func TestNativePolicyConfigurationIsPrivateOnly(t *testing.T) {
	for _, name := range []string{"namespace", "endpoint", "authority", "insecure", "wildcard", "ip", "missing port", "leading zero", "uppercase", "empty"} {
		t.Run(name, func(t *testing.T) {
			r := controller.NewTaskReconciler(nil, "", "")
			ns, ep, a, d := "fabriek-native", "api.fabriek-native.svc.cluster.local:443", "api.fabriek-native.svc", privateDestination
			badTLS := false
			switch name {
			case "namespace":
				ns = "ate-system"
			case "endpoint":
				ep = "api.ate-system.svc:443"
			case "authority":
				a = "api.ate-system.svc"
			case "insecure":
				badTLS = true
			case "wildcard":
				d = "*.svc:8443"
			case "ip":
				d = "127.0.0.1:8443"
			case "missing port":
				d = "factory-native-api.svc"
			case "leading zero":
				d = "native.svc:08443"
			case "uppercase":
				d = strings.ToUpper(privateDestination)
			case "empty":
				d = ""
			}
			if r.ConfigureNativeTLS(d, ns, ep, a, badTLS) == nil {
				t.Fatal("unsafe private binding accepted")
			}
		})
	}
}

func TestNativePolicyLeavesOrdinaryActorUnchanged(t *testing.T) {
	s := &nativePolicyServer{ordinary: true}
	r := nativePolicyReconciler(t, s)
	task := supervisorTask()
	task.Spec.SupervisorProfile = ""
	task.Metadata.Atespace = "ordinary"
	if _, e := r.Reconcile(t.Context(), task); e != nil {
		t.Fatal(e)
	}
	if s.creates != 0 || s.reads != 0 || len(s.resumedActors) != 1 {
		t.Fatalf("ordinary actor changed: creates=%d reads=%d resumes=%d", s.creates, s.reads, len(s.resumedActors))
	}
}
