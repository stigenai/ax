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
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
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

func TestSubscriptionRestoresConsumerGroup(t *testing.T) {
	for _, tc := range []struct {
		name    string
		minIdle time.Duration
	}{
		{name: "read", minIdle: -1},
		{name: "claim", minIdle: time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, client := newClaimTestStore(t, tc.name, Options{
				ReadBlock:    50 * time.Millisecond,
				ClaimMinIdle: tc.minIdle,
			})
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			addTestEvent(t, ctx, client, s.opts.StreamName, "history")
			sub, err := s.Subscribe(ctx, "group", "consumer")
			if err != nil {
				t.Fatal(err)
			}
			initial := addTestEvent(t, ctx, client, s.opts.StreamName, "initial")
			ev, err := sub.Next(ctx)
			if err != nil || ev.ID != initial {
				t.Fatalf("fresh subscription got %v, %v, want %s", ev, err, initial)
			}
			if err := sub.Ack(ctx, ev); err != nil {
				t.Fatal(err)
			}
			if err := client.Del(ctx, s.opts.StreamName, s.checkpointKey("group")).Err(); err != nil {
				t.Fatal(err)
			}
			id, err := client.XAdd(ctx, &redisclient.XAddArgs{
				Stream: s.opts.StreamName, ID: "1-0", Values: map[string]any{"name": "job"},
			}).Result()
			if err != nil {
				t.Fatal(err)
			}
			sub.(*subscription).lastClaim = time.Time{}
			ev, err = sub.Next(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if ev.ID != id {
				t.Fatalf("got event ID %q, want %q", ev.ID, id)
			}
		})
	}
}

func TestRecoveryPreservesPendingGaps(t *testing.T) {
	s, client := newClaimTestStore(t, "pending-gaps", Options{
		ClaimMinIdle: time.Second, ClaimInterval: time.Millisecond, ReadBlock: time.Millisecond,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream := s.opts.StreamName
	const group = "group"
	if err := client.XGroupCreateMkStream(ctx, stream, group, "$").Err(); err != nil {
		t.Fatal(err)
	}
	low := addTestEvent(t, ctx, client, stream, "low")
	high := addTestEvent(t, ctx, client, stream, "high")
	if _, err := client.XReadGroup(ctx, &redisclient.XReadGroupArgs{
		Group: group, Consumer: "original", Streams: []string{stream, ">"}, Count: 2,
	}).Result(); err != nil {
		t.Fatal(err)
	}
	if err := client.XAck(ctx, stream, group, high).Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.Do(ctx, "XCLAIM", stream, group, "original", 0, low, "IDLE", 200, "RETRYCOUNT", 2).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Subscribe(ctx, group, "original"); err != nil {
		t.Fatal(err)
	}
	subscriber, err := s.Subscribe(ctx, group, "replacement")
	if err != nil {
		t.Fatal(err)
	}
	sub := subscriber.(*subscription)
	for _, name := range []string{"first", "second"} {
		if err := client.XGroupDestroy(ctx, stream, group).Err(); err != nil {
			t.Fatal(err)
		}
		marker := addTestEvent(t, ctx, client, stream, name)
		ev, err := sub.Next(ctx)
		if err != nil || ev.ID != marker {
			t.Fatalf("after %s recovery got %v, %v, want marker %s", name, ev, err, marker)
		}
		pending, err := client.XPendingExt(ctx, &redisclient.XPendingExtArgs{
			Stream: stream, Group: group, Start: low, End: low, Count: 1,
		}).Result()
		if err != nil || len(pending) != 1 || pending[0].Consumer != "original" || pending[0].RetryCount != 2 || pending[0].Idle < 200*time.Millisecond {
			t.Fatalf("after %s recovery got pending %v, %v", name, pending, err)
		}
		if err := sub.Ack(ctx, ev); err != nil {
			t.Fatal(err)
		}
	}
	ev, err := sub.Next(ctx)
	if err != nil || ev.ID != low {
		t.Fatalf("got reclaimed event %v, %v, want %s", ev, err, low)
	}
	if err := sub.Ack(ctx, ev); err != nil {
		t.Fatal(err)
	}
	if err := client.XGroupDestroy(ctx, stream, group).Err(); err != nil {
		t.Fatal(err)
	}
	marker := addTestEvent(t, ctx, client, stream, "before resubscribe")
	subscriber, err = s.Subscribe(ctx, group, "replacement")
	if err != nil {
		t.Fatal(err)
	}
	if ev, err := subscriber.Next(ctx); err != nil || ev.ID != marker {
		t.Fatalf("resubscribe got %v, %v, want %s", ev, err, marker)
	}
}

func TestCheckpointRestoresPendingPages(t *testing.T) {
	s, client := newClaimTestStore(t, "pages", Options{ClaimMinIdle: -1, ReadBatchSize: 300})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	sub, err := s.Subscribe(ctx, "group", "consumer")
	if err != nil {
		t.Fatal(err)
	}
	stream := s.opts.StreamName
	first := addTestEvent(t, ctx, client, stream, "trimmed")
	for range 256 {
		addTestEvent(t, ctx, client, stream, "pending")
	}
	last := addTestEvent(t, ctx, client, stream, "acknowledged")
	for range 258 {
		ev, err := sub.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if ev.ID == last {
			if err := sub.Ack(ctx, ev); err != nil {
				t.Fatal(err)
			}
			if err := sub.Ack(ctx, ev); err != nil {
				t.Fatalf("duplicate ACK: %v", err)
			}
		}
	}
	if err := client.XDel(ctx, stream, first).Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.XGroupDestroy(ctx, stream, "group").Err(); err != nil {
		t.Fatal(err)
	}
	if err := sub.(*subscription).restoreConsumerGroup(ctx); err != nil {
		t.Fatal(err)
	}
	pending, err := client.XPending(ctx, stream, "group").Result()
	if err != nil || pending.Count != 256 {
		t.Fatalf("got pending %v, %v, want 256 surviving IDs", pending, err)
	}
}

func TestCheckpointRejectsDeniedWrites(t *testing.T) {
	s, client := newClaimTestStore(t, "acl", Options{ClaimMinIdle: -1})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	username := s.opts.KeyPrefix
	if err := client.Do(ctx, "ACL", "SETUSER", username, "on", "nopass", "~*", "+@all", "-set").Err(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := client.Do(context.Background(), "ACL", "DELUSER", username).Err(); err != nil {
			t.Error(err)
		}
	})
	limitedClient := redisclient.NewClient(&redisclient.Options{
		Addr: client.Options().Addr,
		OnConnect: func(ctx context.Context, conn *redisclient.Conn) error {
			return conn.Do(ctx, "AUTH", username, "").Err()
		},
	})
	t.Cleanup(func() { _ = limitedClient.Close() })
	limited := NewStore(limitedClient, s.opts)
	stream := s.opts.StreamName
	const group = "group"
	if _, err := limited.Subscribe(ctx, group, "limited"); !redisclient.HasErrorPrefix(err, "NOPERM") {
		t.Fatalf("Subscribe with SET denied: %v", err)
	}
	if err := client.XPending(ctx, stream, group).Err(); !redisclient.HasErrorPrefix(err, "NOGROUP") {
		t.Fatalf("failed Subscribe created the group: %v", err)
	}
	sub, err := s.Subscribe(ctx, group, "allowed")
	if err != nil {
		t.Fatal(err)
	}
	id := addTestEvent(t, ctx, client, stream, "pending")
	ev, err := sub.Next(ctx)
	if err != nil || ev.ID != id {
		t.Fatalf("initial delivery = %v, %v, want %s", ev, err, id)
	}
	if err := client.Do(ctx, "ACL", "SETUSER", username, "+set", "-xack", "-xclaim").Err(); err != nil {
		t.Fatal(err)
	}
	checkpoint := s.checkpointKey(group)
	before, err := client.Get(ctx, checkpoint).Result()
	if err != nil {
		t.Fatal(err)
	}
	replacement := &subscription{store: limited, group: group, consumer: "limited"}
	if err := replacement.Ack(ctx, ev); !redisclient.HasErrorPrefix(err, "NOPERM") {
		t.Fatalf("ACK with XACK denied: %v", err)
	}
	if after, err := client.Get(ctx, checkpoint).Result(); err != nil || after != before {
		t.Fatalf("failed ACK changed the checkpoint: %v", err)
	}
	if _, err := s.Subscribe(ctx, group, "allowed"); err != nil {
		t.Fatal(err)
	}
	if err := client.XGroupDestroy(ctx, stream, group).Err(); err != nil {
		t.Fatal(err)
	}
	if err := replacement.restoreConsumerGroup(ctx); !redisclient.HasErrorPrefix(err, "NOPERM") {
		t.Fatalf("recovery with XCLAIM denied: %v", err)
	}
	if err := client.XPending(ctx, stream, group).Err(); !redisclient.HasErrorPrefix(err, "NOGROUP") {
		t.Fatalf("failed recovery created the group: %v", err)
	}
}

func TestRestoreConsumerGroup(t *testing.T) {
	s, client := newClaimTestStore(t, "restore", Options{ClaimMinIdle: -1, ReadBatchSize: 1})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })

	sub := &subscription{store: s, group: "group", consumer: "first", claimCursor: "42-0"}
	competitor := &subscription{store: s, group: "group", consumer: "second", claimCursor: "42-0"}
	other, err := s.Subscribe(ctx, "other", "first")
	if err != nil {
		t.Fatal(err)
	}
	old := addTestEvent(t, ctx, client, s.opts.StreamName, "other pending")
	ev, err := other.Next(ctx)
	if err != nil || ev.ID != old {
		t.Fatalf("other group got %v, %v, want %s", ev, err, old)
	}
	if err := other.Ack(ctx, ev); err != nil {
		t.Fatal(err)
	}
	otherPending := addTestEvent(t, ctx, client, s.opts.StreamName, "another pending")
	if ev, err := other.Next(ctx); err != nil || ev.ID != otherPending {
		t.Fatalf("other group got %v, %v, want %s", ev, err, otherPending)
	}

	before := time.Now()
	results := make(chan error, 2)
	go func() { results <- sub.restoreConsumerGroup(ctx) }()
	go func() { results <- competitor.restoreConsumerGroup(ctx) }()
	for range 2 {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	if sub.claimCursor != "0-0" || sub.lastClaim.Before(before) {
		t.Fatalf("recovery left claim cursor %q and last claim %v", sub.claimCursor, sub.lastClaim)
	}
	if strings.Count(logs.String(), "level=WARN msg=\"restored Redis consumer group\"") != 1 {
		t.Fatalf("expected one recovery warning, got %q", logs.String())
	}

	logs.Reset()
	if ev, err := sub.Next(ctx); err != nil || ev.ID != old {
		t.Fatalf("recovered group got %v, %v, want %s", ev, err, old)
	}
	if err := competitor.restoreConsumerGroup(ctx); err != nil {
		t.Fatalf("recovery must accept BUSYGROUP: %v", err)
	}
	if ev, err := competitor.Next(ctx); err != nil || ev.ID != otherPending {
		t.Fatalf("second recovery changed the group cursor: got %v, %v, want %s", ev, err, otherPending)
	}
	if logs.Len() != 0 {
		t.Fatalf("recovery logged an existing group: %q", logs.String())
	}
	pending, err := client.XPendingExt(ctx, &redisclient.XPendingExtArgs{
		Stream: s.opts.StreamName, Group: "other", Start: "-", End: "+", Count: 10,
	}).Result()
	if err != nil || len(pending) != 1 || pending[0].ID != otherPending || pending[0].Consumer != "first" {
		t.Fatalf("recovery changed another group's pending entries: %v, %v", pending, err)
	}
	next := addTestEvent(t, ctx, client, s.opts.StreamName, "other next")
	ev, err = other.Next(ctx)
	if err != nil || ev.ID != next {
		t.Fatalf("recovery changed another group's cursor: got %v, %v, want %s", ev, err, next)
	}

	if err := client.LPush(ctx, s.checkpointKey("broken"), "wrong type").Err(); err != nil {
		t.Fatal(err)
	}
	broken := &subscription{store: s, group: "broken"}
	if err := broken.restoreConsumerGroup(ctx); !redisclient.HasErrorPrefix(err, "WRONGTYPE") {
		t.Fatalf("recovery must reject a checkpoint with the wrong type: %v", err)
	}
	if err := client.XPending(ctx, s.opts.StreamName, "broken").Err(); !redisclient.HasErrorPrefix(err, "NOGROUP") {
		t.Fatalf("recovery created a group with an invalid checkpoint: %v", err)
	}
	if err := client.Del(ctx, s.checkpointKey("other")).Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.LPush(ctx, s.checkpointKey("other"), "wrong type").Err(); err != nil {
		t.Fatal(err)
	}
	if err := other.Ack(ctx, ev); !redisclient.HasErrorPrefix(err, "WRONGTYPE") {
		t.Fatalf("ACK must reject a checkpoint with the wrong type: %v", err)
	}
	remaining, err := client.XPending(ctx, s.opts.StreamName, "other").Result()
	if err != nil || remaining.Count != 2 {
		t.Fatalf("failed ACK changed pending entries: %v, %v", remaining, err)
	}

	if err := client.Set(ctx, s.opts.StreamName, "wrong type", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := sub.restoreConsumerGroup(ctx); !redisclient.HasErrorPrefix(err, "WRONGTYPE") {
		t.Fatalf("recovery must return WRONGTYPE: %v", err)
	}
	if logs.Len() != 0 {
		t.Fatalf("recovery logged a failed group creation: %q", logs.String())
	}
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
	if err := client.XGroupDestroy(ctx, s.opts.StreamName, "group").Err(); err != nil {
		t.Fatal(err)
	}
	marker := addTestEvent(t, ctx, client, s.opts.StreamName, "after normal ack")
	if ev, err := subB.Next(ctx); err != nil || ev.ID != marker {
		t.Fatalf("after recovery got %v, %v, want marker %s", ev, err, marker)
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

// AC6: a poison event (one that is never acked) must not be delivered more
// than MaxDeliveries times. With MaxDeliveries=2, delivery 1 (the initial
// XREADGROUP) and delivery 2 (the first self-claim once idle) must both
// return the event; delivery 3 (the next self-claim) must instead ack and
// drop it, so it is never returned and XPENDING ends at 0.
func TestClaimDropsPoisonEventAfterMaxDeliveries(t *testing.T) {
	s, client := newClaimTestStore(t, "ac6", Options{
		ReadBatchSize: 10,
		ReadBlock:     20 * time.Millisecond,
		ClaimMinIdle:  50 * time.Millisecond,
		ClaimInterval: 10 * time.Millisecond,
		MaxDeliveries: 2,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	sub, err := s.Subscribe(ctx, "group", "consumer-a")
	if err != nil {
		t.Fatal(err)
	}

	id := addTestEvent(t, ctx, client, s.opts.StreamName, "job")

	// Delivery 1: the initial XREADGROUP read. Never ack it, so it goes
	// idle and becomes claimable.
	ev, err := sub.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if ev.ID != id {
		t.Fatalf("delivery 1: got id %q, want %q", ev.ID, id)
	}

	// Delivery 2: a self-claim once idle. Still within MaxDeliveries, so it
	// must come back. Never ack it either.
	claim2Ctx, claim2Cancel := context.WithTimeout(ctx, 2*time.Second)
	ev, err = sub.Next(claim2Ctx)
	claim2Cancel()
	if err != nil {
		t.Fatalf("delivery 2: want the event back, got: %v", err)
	}
	if ev.ID != id {
		t.Fatalf("delivery 2: got id %q, want %q", ev.ID, id)
	}

	// Delivery 3 would push the count past MaxDeliveries=2: it must be
	// dropped, not returned.
	claim3Ctx, claim3Cancel := context.WithTimeout(ctx, 2*time.Second)
	_, err = sub.Next(claim3Ctx)
	claim3Cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("delivery 3: want context.DeadlineExceeded (event must be dropped), got: %v", err)
	}

	pending, err := client.XPending(ctx, s.opts.StreamName, "group").Result()
	if err != nil {
		t.Fatal(err)
	}
	if pending.Count != 0 {
		t.Fatalf("XPENDING count = %d, want 0", pending.Count)
	}
	if err := client.XGroupDestroy(ctx, s.opts.StreamName, "group").Err(); err != nil {
		t.Fatal(err)
	}
	marker := addTestEvent(t, ctx, client, s.opts.StreamName, "after poison drop")
	if ev, err := sub.Next(ctx); err != nil || ev.ID != marker {
		t.Fatalf("after recovery got %v, %v, want marker %s", ev, err, marker)
	}
}
