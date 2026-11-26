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

type recoveryCommandHook struct {
	command string
	noGroup bool
}

func (h *recoveryCommandHook) DialHook(next redisclient.DialHook) redisclient.DialHook {
	return next
}

func (h *recoveryCommandHook) ProcessPipelineHook(next redisclient.ProcessPipelineHook) redisclient.ProcessPipelineHook {
	return next
}

func (h *recoveryCommandHook) ProcessHook(next redisclient.ProcessHook) redisclient.ProcessHook {
	return func(ctx context.Context, cmd redisclient.Cmder) error {
		err := next(ctx, cmd)
		if cmd.Name() == h.command && redisclient.HasErrorPrefix(err, "NOGROUP") {
			h.noGroup = true
		}
		return err
	}
}

func TestGroupRecoveryOrder(t *testing.T) {
	t.Run("first_setup", func(t *testing.T) {
		s, client := newClaimTestStore(t, "first-setup", Options{ReadBlock: 20 * time.Millisecond})
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		addTestEvent(t, ctx, client, s.opts.StreamName, "before-setup")
		sub, err := s.Subscribe(ctx, "group", "first")
		if err != nil {
			t.Fatal(err)
		}
		want := addTestEvent(t, ctx, client, s.opts.StreamName, "after-setup")
		if ev, err := sub.Next(ctx); err != nil || ev.ID != want {
			t.Fatalf("Next = %q, %v; want %q", ev.ID, err, want)
		}
	})

	for _, loss := range []string{"full_loss", "saved_checkpoint", "missing_checkpoint"} {
		for _, winner := range []string{"fresh_first", "active_first"} {
			for _, command := range []string{"xreadgroup", "xautoclaim"} {
				t.Run(loss+"/"+winner+"/"+command, func(t *testing.T) {
					testGroupRecoveryOrder(t, loss, winner, command)
				})
			}
		}
	}
}

func testGroupRecoveryOrder(t *testing.T, loss, winner, command string) {
	t.Helper()
	const group = "group"
	opts := Options{ReadBatchSize: 1, ReadBlock: 20 * time.Millisecond, ClaimMinIdle: 10 * time.Millisecond, ClaimInterval: time.Millisecond}
	fresh, freshClient := newClaimTestStore(t, "recovery-order", opts)
	stream := fresh.opts.StreamName
	checkpoint := fmt.Sprintf("ax:stream-checkpoint:%d:%s:%s", len(stream), stream, group)
	client := redisclient.NewClient(&redisclient.Options{Addr: freshClient.Options().Addr})
	t.Cleanup(func() { client.Close() })
	activeClient := redisclient.NewClient(&redisclient.Options{Addr: freshClient.Options().Addr})
	t.Cleanup(func() { activeClient.Close() })
	hook := &recoveryCommandHook{command: command}
	activeClient.AddHook(hook)
	activeOpts := fresh.opts
	if command == "xreadgroup" {
		activeOpts.ClaimMinIdle = -1
	}
	active := NewStore(activeClient, activeOpts)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	joined, err := active.Subscribe(ctx, group, t.Name()+"/active")
	if err != nil {
		t.Fatal(err)
	}
	activeSub := joined.(*subscription)
	deletedID, err := client.XAdd(ctx, &redisclient.XAddArgs{
		Stream: stream,
		ID:     "1-0",
		Values: map[string]interface{}{"action": "delete", "namespace": "private", "name": "old-task"},
	}).Result()
	if err != nil {
		t.Fatal(err)
	}
	ev, err := activeSub.Next(ctx)
	if err != nil || ev.ID != deletedID {
		t.Fatalf("legacy delete = %q, %v; want %q", ev.ID, err, deletedID)
	}
	if err := activeSub.Ack(ctx, ev); err != nil {
		t.Fatal(err)
	}
	want := make(map[string]bool)
	if loss == "saved_checkpoint" {
		id := addTestEvent(t, ctx, client, stream, "pending")
		if ev, err := activeSub.Next(ctx); err != nil || ev.ID != id {
			t.Fatalf("pending event = %q, %v; want %q", ev.ID, err, id)
		}
		want[id] = true
	}

	// Neither client starts another operation until XADD returns the marker ID.
	if n, err := client.XGroupDestroy(ctx, stream, group).Result(); err != nil || n != 1 {
		t.Fatalf("XGROUP DESTROY = %d, %v; want 1", n, err)
	}
	if loss != "saved_checkpoint" {
		if err := client.Del(ctx, checkpoint).Err(); err != nil {
			t.Fatal(err)
		}
	}
	if loss == "full_loss" {
		if err := client.Del(ctx, stream).Err(); err != nil {
			t.Fatal(err)
		}
	}
	marker := addTestEvent(t, ctx, client, stream, "outage")
	want[marker] = true

	var freshSub *subscription
	resumeFresh := func() error {
		sub, err := fresh.Subscribe(ctx, group, t.Name()+"/fresh")
		if err == nil {
			freshSub = sub.(*subscription)
		}
		return err
	}
	resumeActive := func() (err error) {
		if command == "xautoclaim" {
			activeSub.lastClaim = time.Time{}
			activeSub.pending, err = activeSub.claim(ctx)
		} else {
			activeSub.pending, err = activeSub.read(ctx)
		}
		return err
	}
	// Each call returns before the next client can attempt recovery.
	var freshErr, activeErr error
	if winner == "fresh_first" {
		freshErr = resumeFresh()
		activeErr = resumeActive()
	} else {
		activeErr = resumeActive()
		freshErr = resumeFresh()
	}
	if loss == "missing_checkpoint" {
		if freshErr == nil || activeErr == nil {
			t.Errorf("ambiguous state must produce two errors; Subscribe = %v, %s = %v", freshErr, command, activeErr)
		}
		groups, err := client.XInfoGroups(ctx, stream).Result()
		if err != nil || len(groups) != 0 {
			t.Errorf("XINFO GROUPS = %v, %v; want no groups", groups, err)
		}
		msgs, err := client.XRange(ctx, stream, "-", "+").Result()
		if err != nil || len(msgs) != 2 || msgs[0].ID != deletedID || msgs[1].ID != marker {
			t.Errorf("stream must retain legacy delete %q and marker %q; got %v, %v", deletedID, marker, msgs, err)
		}
		if !hook.noGroup {
			t.Errorf("%s did not encounter NOGROUP", command)
		}
		return
	}
	if freshErr != nil || activeErr != nil {
		t.Fatalf("Subscribe = %v, %s = %v", freshErr, command, activeErr)
	}
	if hook.noGroup != (winner == "active_first") {
		t.Fatalf("%s NOGROUP = %t in %s", command, hook.noGroup, winner)
	}
	for len(want) > 0 {
		sub := freshSub
		if len(activeSub.pending) > 0 {
			sub = activeSub
		}
		readCtx, stop := context.WithTimeout(ctx, time.Second)
		ev, err := sub.Next(readCtx)
		stop()
		if err != nil || !want[ev.ID] {
			t.Fatalf("Next = %q, %v; want remaining IDs %v; acknowledged delete = %q", ev.ID, err, want, deletedID)
		}
		delete(want, ev.ID)
		if err := sub.Ack(ctx, ev); err != nil {
			t.Fatal(err)
		}
	}
	readCtx, stop := context.WithTimeout(ctx, 150*time.Millisecond)
	defer stop()
	if ev, err := freshSub.Next(readCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("extra delivery = %q, %v; want deadline", ev.ID, err)
	}
	pending, err := client.XPending(ctx, stream, group).Result()
	if err != nil || pending.Count != 0 {
		t.Errorf("XPENDING = %v, %v; want no pending entries", pending, err)
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
}
