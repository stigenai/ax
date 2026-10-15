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

package substrate

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"regexp"
	"strconv"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// NativeTLSPolicy accepts one canonical DNS name and explicit port. It never
// creates a wildcard, all-port, HTTP or credential-injection grant.
func NativeTLSPolicy(destination string) (*ateapipb.EgressPolicy, error) {
	host, portText, err := net.SplitHostPort(destination)
	port, numberErr := strconv.ParseUint(portText, 10, 16)
	_, ipErr := netip.ParseAddr(host)
	dns := regexp.MustCompile(`^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)*$`)
	if err != nil || numberErr != nil || port == 0 || ipErr == nil || len(host) > 253 || !dns.MatchString(host) || strconv.FormatUint(port, 10) != portText {
		return nil, fmt.Errorf("native TLS destination must be one canonical DNS hostname:port")
	}
	return &ateapipb.EgressPolicy{Rules: []*ateapipb.EgressRule{{TlsPassthrough: &ateapipb.TLSPassthroughRule{Hostnames: []string{host}, Ports: &ateapipb.Ports{Numbers: []int32{int32(port)}}}}}}, nil
}

// EnsureNativeTLSPolicy runs before the protected actor is resumed. Conflicting
// policy is never replaced, and missing policy on an already running actor is
// never silently re-granted. A lost create reply is reconciled on the next call.
func (c *Client) EnsureNativeTLSPolicy(ctx context.Context, actor *ateapipb.Actor, desired *ateapipb.EgressPolicy) error {
	m := actor.GetMetadata()
	if m.GetAtespace() != "fabriek-native" || m.GetName() == "" || m.GetUid() == "" {
		return fmt.Errorf("native policy requires retained private actor identity")
	}
	ref := &ateapipb.ObjectRef{Atespace: m.GetAtespace(), Name: m.GetName()}
	existing, err := c.control.GetActorEgressPolicy(ctx, &ateapipb.GetActorEgressPolicyRequest{Actor: ref})
	if status.Code(err) == codes.NotFound {
		if actor.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
			return fmt.Errorf("cannot create missing policy after actor start")
		}
		policy := proto.CloneOf(desired)
		policy.Metadata = &ateapipb.ResourceMetadata{Atespace: m.GetAtespace(), Name: "default"}
		_, err = c.control.CreateActorEgressPolicy(ctx, &ateapipb.CreateActorEgressPolicyRequest{Actor: ref, EgressPolicy: policy})
		if err != nil && status.Code(err) != codes.AlreadyExists {
			return fmt.Errorf("create native policy: %w", err)
		}
		existing, err = c.control.GetActorEgressPolicy(ctx, &ateapipb.GetActorEgressPolicyRequest{Actor: ref})
	}
	if err != nil {
		return fmt.Errorf("observe native policy: %w", err)
	}
	if existing.GetMetadata().GetAtespace() != m.GetAtespace() || existing.GetMetadata().GetName() != "default" || !proto.Equal(&ateapipb.EgressPolicy{Rules: existing.GetRules()}, desired) {
		return fmt.Errorf("existing native policy differs from pinned destination")
	}
	current, err := c.control.GetActor(ctx, &ateapipb.GetActorRequest{Actor: ref})
	if err != nil {
		return fmt.Errorf("reobserve native actor: %w", err)
	}
	if current.GetMetadata().GetUid() != m.GetUid() || current.GetMetadata().GetName() != m.GetName() || current.GetMetadata().GetAtespace() != m.GetAtespace() || !proto.Equal(current.GetActorTemplate(), actor.GetActorTemplate()) {
		return fmt.Errorf("native actor changed during policy preparation")
	}
	switch current.GetStatus().GetState() {
	case ateapipb.ActorState_ACTOR_STATE_SUSPENDED, ateapipb.ActorState_ACTOR_STATE_RUNNING:
		return nil
	}
	return fmt.Errorf("native actor cannot resume from current state")
}
