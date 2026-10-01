package hatJournal

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestParallelReplayDefaultIsSerialAndPreservesOrder(t *testing.T) {
	var order []uint64
	tasks := []ReplayTask{
		{Sequence: 11, Partition: 7, Apply: func(context.Context) error { order = append(order, 11); return nil }},
		{Sequence: 12, Partition: 7, Apply: func(context.Context) error { order = append(order, 12); return nil }},
		{Sequence: 13, Partition: 7, Apply: func(context.Context) error { order = append(order, 13); return nil }},
	}
	if err := ParallelReplay(context.Background(), tasks, ReplayOptions{}); err != nil {
		t.Fatalf("ParallelReplay returned error: %v", err)
	}
	want := []uint64{11, 12, 13}
	if len(order) != len(want) {
		t.Fatalf("order length = %d, want %d", len(order), len(want))
	}
	for index := range want {
		if order[index] != want[index] {
			t.Fatalf("order[%d] = %d, want %d", index, order[index], want[index])
		}
	}
}

func TestParallelReplayRunsPartitionsConcurrentlyWithoutReorderingALane(t *testing.T) {
	var active atomic.Int32
	var maximum atomic.Int32
	var firstStarted atomic.Int32
	firstTwoStarted := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	var mu sync.Mutex
	partitionActive := map[uint64]int{}
	partitionOrder := map[uint64][]uint64{}
	var overlap bool

	tasks := make([]ReplayTask, 0, 6)
	for _, partition := range []uint64{1, 2} {
		for sequence := uint64(1); sequence <= 3; sequence++ {
			partition := partition
			sequence := sequence
			tasks = append(tasks, ReplayTask{
				Sequence:  partition*10 + sequence,
				Partition: partition,
				Apply: func(context.Context) error {
					current := active.Add(1)
					for {
						previous := maximum.Load()
						if current <= previous || maximum.CompareAndSwap(previous, current) {
							break
						}
					}
					mu.Lock()
					partitionActive[partition]++
					if partitionActive[partition] > 1 {
						overlap = true
					}
					partitionOrder[partition] = append(partitionOrder[partition], sequence)
					mu.Unlock()
					if sequence == 1 {
						if firstStarted.Add(1) == 2 {
							once.Do(func() { close(firstTwoStarted) })
						}
						select {
						case <-release:
						case <-time.After(2 * time.Second):
							return errors.New("replay start barrier timed out")
						}
					}
					mu.Lock()
					partitionActive[partition]--
					mu.Unlock()
					active.Add(-1)
					return nil
				},
			})
		}
	}

	done := make(chan error, 1)
	go func() { done <- ParallelReplay(context.Background(), tasks, ReplayOptions{Workers: 2}) }()
	select {
	case <-firstTwoStarted:
		close(release)
	case <-time.After(2 * time.Second):
		t.Fatal("independent partitions did not start concurrently")
	}
	if err := <-done; err != nil {
		t.Fatalf("ParallelReplay returned error: %v", err)
	}
	if maximum.Load() < 2 {
		t.Fatalf("maximum concurrency = %d, want at least 2", maximum.Load())
	}
	mu.Lock()
	defer mu.Unlock()
	if overlap {
		t.Fatal("tasks from one partition overlapped")
	}
	for partition, order := range partitionOrder {
		want := []uint64{1, 2, 3}
		if len(order) != len(want) {
			t.Fatalf("partition %d order = %v, want %v", partition, order, want)
		}
		for index := range want {
			if order[index] != want[index] {
				t.Fatalf("partition %d order = %v, want %v", partition, order, want)
			}
		}
	}
}

func TestParallelReplayValidatesBeforeApplying(t *testing.T) {
	var applied atomic.Int32
	tasks := []ReplayTask{
		{Sequence: 2, Partition: 1, Apply: func(context.Context) error { applied.Add(1); return nil }},
		{Sequence: 1, Partition: 2, Apply: func(context.Context) error { applied.Add(1); return nil }},
	}
	if err := ParallelReplay(context.Background(), tasks, ReplayOptions{Workers: 2}); err == nil {
		t.Fatal("ParallelReplay accepted out-of-order tasks")
	}
	if applied.Load() != 0 {
		t.Fatalf("applied = %d, want 0 after validation failure", applied.Load())
	}
}

func TestParallelReplayReturnsApplyError(t *testing.T) {
	want := errors.New("apply failed")
	tasks := []ReplayTask{{
		Sequence:  1,
		Partition: 1,
		Apply:     func(context.Context) error { return want },
	}}
	err := ParallelReplay(context.Background(), tasks, ReplayOptions{Workers: 2})
	if !errors.Is(err, want) {
		t.Fatalf("error = %v, want wrapped %v", err, want)
	}
}

func TestParallelReplayRejectsInvalidOptionsBeforeApplying(t *testing.T) {
	var applied atomic.Int32
	tasks := []ReplayTask{
		{Sequence: 1, Partition: 1, Apply: func(context.Context) error { applied.Add(1); return nil }},
		{Sequence: 2, Partition: 2, Apply: func(context.Context) error { applied.Add(1); return nil }},
	}
	for _, workers := range []int{-1, MaxReplayWorkers + 1} {
		if err := ParallelReplay(context.Background(), tasks, ReplayOptions{Workers: workers}); err == nil {
			t.Fatalf("ParallelReplay accepted workers=%d", workers)
		}
	}
	if applied.Load() != 0 {
		t.Fatalf("applied = %d, want 0 after option validation failure", applied.Load())
	}
}

func TestParallelReplayHonorsCanceledContextBeforeApplying(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var applied atomic.Int32
	tasks := []ReplayTask{
		{Sequence: 1, Partition: 1, Apply: func(context.Context) error { applied.Add(1); return nil }},
		{Sequence: 2, Partition: 2, Apply: func(context.Context) error { applied.Add(1); return nil }},
	}
	if err := ParallelReplay(ctx, tasks, ReplayOptions{Workers: 2}); !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	if applied.Load() != 0 {
		t.Fatalf("applied = %d, want 0 for canceled context", applied.Load())
	}
}
