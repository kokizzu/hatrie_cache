package hatReplication_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"hatrie_cache/hat/hatReplication"
)

func TestCH034ParallelReplicaRangeReadPreservesOrderAndBalancesReplicas(t *testing.T) {
	ranges := []hatReplication.ParallelReplicaRange{
		{ID: "r0", Payload: "zero"},
		{ID: "r1", Payload: "one"},
		{ID: "r2", Payload: "two"},
		{ID: "r3", Payload: "three"},
	}
	result, err := hatReplication.ExecuteParallelReplicaRangeRead(
		context.Background(),
		[]string{"replica-a", "replica-b"},
		ranges,
		hatReplication.ParallelReplicaRangeReadOptions{MaxConcurrency: 2},
		func(_ context.Context, node string, item hatReplication.ParallelReplicaRange) (any, error) {
			return fmt.Sprintf("%s:%s", node, item.Payload), nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	want := []hatReplication.ParallelReplicaRangeResult{
		{ID: "r0", Node: "replica-a", Value: "replica-a:zero"},
		{ID: "r1", Node: "replica-b", Value: "replica-b:one"},
		{ID: "r2", Node: "replica-a", Value: "replica-a:two"},
		{ID: "r3", Node: "replica-b", Value: "replica-b:three"},
	}
	for index := range want {
		if !reflect.DeepEqual(result.Ranges[index].ID, want[index].ID) ||
			result.Ranges[index].Node != want[index].Node ||
			result.Ranges[index].Value != want[index].Value {
			t.Fatalf("range %d = %#v, want %#v", index, result.Ranges[index], want[index])
		}
		if result.Ranges[index].Attempts != nil {
			t.Fatalf("range %d attempts = %#v, want nil by default", index, result.Ranges[index].Attempts)
		}
	}
}

func TestCH034ParallelReplicaRangeReadBoundsConcurrentWork(t *testing.T) {
	ranges := make([]hatReplication.ParallelReplicaRange, 8)
	for index := range ranges {
		ranges[index] = hatReplication.ParallelReplicaRange{ID: fmt.Sprintf("r%d", index), Payload: index}
	}
	var active int64
	var maximum int64
	result, err := hatReplication.ExecuteParallelReplicaRangeRead(
		context.Background(),
		[]string{"replica-a", "replica-b", "replica-c"},
		ranges,
		hatReplication.ParallelReplicaRangeReadOptions{MaxConcurrency: 2},
		func(_ context.Context, _ string, item hatReplication.ParallelReplicaRange) (any, error) {
			current := atomic.AddInt64(&active, 1)
			for {
				previous := atomic.LoadInt64(&maximum)
				if current <= previous || atomic.CompareAndSwapInt64(&maximum, previous, current) {
					break
				}
			}
			time.Sleep(2 * time.Millisecond)
			atomic.AddInt64(&active, -1)
			return item.Payload, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if got := atomic.LoadInt64(&maximum); got > 2 {
		t.Fatalf("maximum concurrent reads = %d, want <= 2", got)
	}
	if len(result.Ranges) != len(ranges) {
		t.Fatalf("result ranges = %d, want %d", len(result.Ranges), len(ranges))
	}
}

func TestCH034ParallelReplicaRangeReadRetriesAFailedRange(t *testing.T) {
	result, err := hatReplication.ExecuteParallelReplicaRangeRead(
		context.Background(),
		[]string{"replica-a", "replica-b"},
		[]hatReplication.ParallelReplicaRange{{ID: "r0", Payload: "payload"}},
		hatReplication.ParallelReplicaRangeReadOptions{MaxConcurrency: 1, RecordAttempts: true},
		func(_ context.Context, node string, item hatReplication.ParallelReplicaRange) (any, error) {
			if node == "replica-a" {
				return nil, errors.New("replica unavailable")
			}
			return item.Payload, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if result.Ranges[0].Node != "replica-b" || result.Ranges[0].Value != "payload" {
		t.Fatalf("range result = %#v, want replica-b/payload", result.Ranges[0])
	}
	if len(result.Ranges[0].Attempts) != 2 || !result.Ranges[0].Attempts[0].Started || !result.Ranges[0].Attempts[1].Succeeded {
		t.Fatalf("attempts = %#v, want failed a then successful b", result.Ranges[0].Attempts)
	}
}

func TestCH034ParallelReplicaRangeReadRejectsPartialFailureAndInvalidInput(t *testing.T) {
	_, err := hatReplication.ExecuteParallelReplicaRangeRead(
		context.Background(),
		[]string{"replica-a", "replica-b"},
		[]hatReplication.ParallelReplicaRange{
			{ID: "ok", Payload: "ok"},
			{ID: "failed", Payload: "failed"},
		},
		hatReplication.ParallelReplicaRangeReadOptions{MaxConcurrency: 1},
		func(_ context.Context, _ string, item hatReplication.ParallelReplicaRange) (any, error) {
			if item.ID == "failed" {
				return nil, errors.New("all replicas unavailable")
			}
			return item.Payload, nil
		},
	)
	if !errors.Is(err, hatReplication.ErrParallelReplicaRangeReadFailed) {
		t.Fatalf("error = %v, want range-read failure", err)
	}

	cases := []struct {
		name   string
		nodes  []string
		ranges []hatReplication.ParallelReplicaRange
		limit  int
		want   error
	}{
		{name: "nil callback", nodes: []string{"a"}, ranges: []hatReplication.ParallelReplicaRange{{ID: "r"}}, want: hatReplication.ErrParallelReplicaRangeReadInvalid},
		{name: "empty nodes", ranges: []hatReplication.ParallelReplicaRange{{ID: "r"}}, want: hatReplication.ErrParallelReplicaRangeReadInvalid},
		{name: "blank node", nodes: []string{" "}, ranges: []hatReplication.ParallelReplicaRange{{ID: "r"}}, want: hatReplication.ErrParallelReplicaRangeReadInvalid},
		{name: "duplicate node", nodes: []string{"a", "a"}, ranges: []hatReplication.ParallelReplicaRange{{ID: "r"}}, want: hatReplication.ErrParallelReplicaRangeReadInvalid},
		{name: "empty ranges", nodes: []string{"a"}, want: hatReplication.ErrParallelReplicaRangeReadInvalid},
		{name: "duplicate range", nodes: []string{"a"}, ranges: []hatReplication.ParallelReplicaRange{{ID: "r"}, {ID: "r"}}, want: hatReplication.ErrParallelReplicaRangeReadInvalid},
		{name: "invalid limit", nodes: []string{"a"}, ranges: []hatReplication.ParallelReplicaRange{{ID: "r"}}, limit: -1, want: hatReplication.ErrParallelReplicaRangeReadInvalid},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			read := hatReplication.ParallelReplicaRangeReadFunc(func(context.Context, string, hatReplication.ParallelReplicaRange) (any, error) {
				return nil, nil
			})
			if test.name == "nil callback" {
				read = nil
			}
			_, err := hatReplication.ExecuteParallelReplicaRangeRead(
				context.Background(),
				test.nodes,
				test.ranges,
				hatReplication.ParallelReplicaRangeReadOptions{MaxConcurrency: test.limit},
				read,
			)
			if !errors.Is(err, test.want) {
				t.Fatalf("error = %v, want %v", err, test.want)
			}
		})
	}
}
