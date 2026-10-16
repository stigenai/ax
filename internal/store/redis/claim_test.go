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

package redis

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	redisclient "github.com/redis/go-redis/v9"
)

// newClaimTestStore returns a Store on a fresh stream, so tests do not
// collide. It skips the test when no disposable Redis is configured, the
// same convention requeue_test.go and delete_test.go use.
func newClaimTestStore(t *testing.T, name string, opts Options) (*Store, *redisclient.Client) {
	t.Helper()
	address := os.Getenv("AX_DELETE_TEST_REDIS_ADDRESS")
	if address == "" {
		t.Skip("requires explicit disposable AX_DELETE_TEST_REDIS_ADDRESS")
	}
	client := redisclient.NewClient(&redisclient.Options{Addr: address})
	t.Cleanup(func() { client.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatal(err)
	}

	prefix := fmt.Sprintf("ax-claim-test-%s-%d", name, time.Now().UnixNano())
	opts.KeyPrefix = prefix
	opts.StreamName = prefix + ":events"
	return NewStore(client, opts), client
}

func addTestEvent(t *testing.T, ctx context.Context, client *redisclient.Client, stream, name string) string {
	t.Helper()
	id, err := client.XAdd(ctx, &redisclient.XAddArgs{
		Stream: stream,
		Values: map[string]interface{}{
			"action":   "reconcile",
			"atespace": "private",
			"name":     name,
		},
	}).Result()
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// AC1: consumer A reads an event and never acks it. Consumer B, in the same
// group, must get the same event id from Next within 2s once it has gone
// idle for ClaimMinIdle. After B acks it, XPENDING must show 0 entries.
// This must fail on pre-fix code, which never claims.
func TestClaimReassignsEventFromDeadConsumerAfterIdleThreshold(t *testing.T) {
	s, client := newClaimTestStore(t, "ac1", Options{
		ReadBatchSize: 10,
		ReadBlock:     50 * time.Millisecond,
		ClaimMinIdle:  200 * time.Millisecond,
		ClaimInterval: 50 * time.Millisecond,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	subA, err := s.Subscribe(ctx, "group", "consumer-a")
	if err != nil {
		t.Fatal(err)
	}
	subB, err := s.Subscribe(ctx, "group", "consumer-b")
	if err != nil {
		t.Fatal(err)
	}

	id := addTestEvent(t, ctx, client, s.opts.StreamName, "job")

	evA, err := subA.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if evA.ID != id {
		t.Fatalf("consumer A got id %q, want %q", evA.ID, id)
	}
	// Consumer A never acks evA here: this stands in for it dying.

	claimCtx, claimCancel := context.WithTimeout(ctx, 2*time.Second)
	defer claimCancel()
	evB, err := subB.Next(claimCtx)
	if err != nil {
		t.Fatalf("consumer B never reclaimed the entry: %v", err)
	}
	if evB.ID != id {
		t.Fatalf("consumer B got id %q, want %q", evB.ID, id)
	}
	if err := subB.Ack(ctx, evB); err != nil {
		t.Fatal(err)
	}

	pending, err := client.XPending(ctx, s.opts.StreamName, "group").Result()
	if err != nil {
		t.Fatal(err)
	}
	if pending.Count != 0 {
		t.Fatalf("XPENDING count = %d, want 0", pending.Count)
	}
}

// AC2: consumer B must not get the entry before it has been idle for
// ClaimMinIdle. A 100ms ctx is shorter than a 1s ClaimMinIdle, so Next must
// time out with context.DeadlineExceeded, not reclaim early.
func TestClaimDoesNotReclaimBeforeMinIdleElapses(t *testing.T) {
	s, client := newClaimTestStore(t, "ac2", Options{
		ReadBatchSize: 10,
		ReadBlock:     20 * time.Millisecond,
		ClaimMinIdle:  1 * time.Second,
		ClaimInterval: 10 * time.Millisecond,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	subA, err := s.Subscribe(ctx, "group", "consumer-a")
	if err != nil {
		t.Fatal(err)
	}
	subB, err := s.Subscribe(ctx, "group", "consumer-b")
	if err != nil {
		t.Fatal(err)
	}

	addTestEvent(t, ctx, client, s.opts.StreamName, "job")
	if _, err := subA.Next(ctx); err != nil {
		t.Fatal(err)
	}

	shortCtx, shortCancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer shortCancel()
	if _, err := subB.Next(shortCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got err %v, want context.DeadlineExceeded", err)
	}
}

// AC3: consumer A reads a batch of 3 (ReadBatchSize 3) in one XREADGROUP
// call, consumes only 1 of them via Next, and never acks any. Consumer B
// must reclaim all 3, because XREADGROUP already put all 3 in A's pending
// list in Redis, regardless of how many A handed to its own caller.
func TestClaimReassignsWholeBatchFromDeadConsumer(t *testing.T) {
	s, client := newClaimTestStore(t, "ac3", Options{
		ReadBatchSize: 3,
		ReadBlock:     50 * time.Millisecond,
		ClaimMinIdle:  200 * time.Millisecond,
		ClaimInterval: 50 * time.Millisecond,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	subA, err := s.Subscribe(ctx, "group", "consumer-a")
	if err != nil {
		t.Fatal(err)
	}
	subB, err := s.Subscribe(ctx, "group", "consumer-b")
	if err != nil {
		t.Fatal(err)
	}

	ids := make(map[string]bool, 3)
	for i := 0; i < 3; i++ {
		ids[addTestEvent(t, ctx, client, s.opts.StreamName, fmt.Sprintf("job-%d", i))] = true
	}

	first, err := subA.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !ids[first.ID] {
		t.Fatalf("consumer A got unexpected id %q", first.ID)
	}

	got := make(map[string]bool, 3)
	for len(got) < 3 {
		claimCtx, claimCancel := context.WithTimeout(ctx, 2*time.Second)
		ev, err := subB.Next(claimCtx)
		claimCancel()
		if err != nil {
			t.Fatalf("consumer B did not reclaim the whole batch: %v (got %d of 3)", err, len(got))
		}
		if !ids[ev.ID] {
			t.Fatalf("consumer B got unexpected id %q", ev.ID)
		}
		got[ev.ID] = true
		if err := subB.Ack(ctx, ev); err != nil {
			t.Fatal(err)
		}
	}
}

// AC4: a negative ClaimMinIdle turns reclaim off, so consumer B never gets
// the entry consumer A read, no matter how long it waits.
func TestClaimMinIdleNegativeDisablesReclaim(t *testing.T) {
	s, client := newClaimTestStore(t, "ac4", Options{
		ReadBatchSize: 10,
		ReadBlock:     20 * time.Millisecond,
		ClaimMinIdle:  -1,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	subA, err := s.Subscribe(ctx, "group", "consumer-a")
	if err != nil {
		t.Fatal(err)
	}
	subB, err := s.Subscribe(ctx, "group", "consumer-b")
	if err != nil {
		t.Fatal(err)
	}

	addTestEvent(t, ctx, client, s.opts.StreamName, "job")
	if _, err := subA.Next(ctx); err != nil {
		t.Fatal(err)
	}

	shortCtx, shortCancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer shortCancel()
	if _, err := subB.Next(shortCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got err %v, want context.DeadlineExceeded (reclaim must stay off)", err)
	}
}

// AC5: a consumer that acks each of 3 events must not see any of them
// delivered a second time after they go idle past ClaimMinIdle.
func TestClaimDoesNotRedeliverAckedEvents(t *testing.T) {
	s, client := newClaimTestStore(t, "ac5", Options{
		ReadBatchSize: 10,
		ReadBlock:     20 * time.Millisecond,
		ClaimMinIdle:  50 * time.Millisecond,
		ClaimInterval: 10 * time.Millisecond,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	sub, err := s.Subscribe(ctx, "group", "consumer-a")
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 3; i++ {
		addTestEvent(t, ctx, client, s.opts.StreamName, fmt.Sprintf("job-%d", i))
	}

	for i := 0; i < 3; i++ {
		ev, err := sub.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := sub.Ack(ctx, ev); err != nil {
			t.Fatal(err)
		}
	}

	// Wait past ClaimMinIdle: a duplicate delivery here would prove acked
	// entries are still being reclaimed.
	pollCtx, pollCancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer pollCancel()
	if ev, err := sub.Next(pollCtx); err == nil {
		t.Fatalf("got a duplicate delivery of id %q after every event was acked", ev.ID)
	} else if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("unexpected error waiting for no duplicate: %v", err)
	}
}
