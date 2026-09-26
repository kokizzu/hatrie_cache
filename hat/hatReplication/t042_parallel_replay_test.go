package hatReplication

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"hatrie_cache/hat/hatJournal"
)

func TestT042ParallelReplayPreservesPerKeyOrder(t *testing.T) {
	records := t042ReplayBenchmarkRecords()
	seen := make(map[string][]uint64)
	var mu sync.Mutex
	var entered atomic.Int32
	var maxActive atomic.Int32
	var active atomic.Int32
	ready := make(chan struct{})
	var release sync.Once

	err := ReplayJournalRecordsParallel(context.Background(), records, ParallelReplayOptions{
		Workers: 8,
		Key: func(record hatJournal.Record) string {
			return record.Request.Key
		},
		Apply: func(ctx context.Context, record hatJournal.Record) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			current := entered.Add(1)
			if current == 2 {
				release.Do(func() { close(ready) })
			}
			if current <= 2 {
				select {
				case <-ready:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			inFlight := active.Add(1)
			for {
				previous := maxActive.Load()
				if inFlight <= previous || maxActive.CompareAndSwap(previous, inFlight) {
					break
				}
			}
			mu.Lock()
			seen[record.Request.Key] = append(seen[record.Request.Key], record.Sequence)
			mu.Unlock()
			active.Add(-1)
			return nil
		},
	})
	if err != nil {
		t.Fatalf("ReplayJournalRecordsParallel() error = %v", err)
	}
	if maxActive.Load() < 2 {
		t.Fatalf("max concurrent callbacks = %d, want at least 2", maxActive.Load())
	}
	if len(seen) != 64 {
		t.Fatalf("seen keys = %d, want 64", len(seen))
	}
	count := 0
	for key, sequences := range seen {
		if len(sequences) == 0 {
			t.Fatalf("key %q has no replayed records", key)
		}
		for index := 1; index < len(sequences); index++ {
			if sequences[index] <= sequences[index-1] {
				t.Fatalf("key %q sequence order = %v", key, sequences)
			}
		}
		count += len(sequences)
	}
	if count != len(records) {
		t.Fatalf("replayed records = %d, want %d", count, len(records))
	}
}

func TestT042ParallelReplaySerialFallbackIsDefault(t *testing.T) {
	records := t042ReplayBenchmarkRecords()[:32]
	var replayed []uint64
	err := ReplayJournalRecordsParallel(context.Background(), records, ParallelReplayOptions{
		Apply: func(_ context.Context, record hatJournal.Record) error {
			replayed = append(replayed, record.Sequence)
			return nil
		},
	})
	if err != nil {
		t.Fatalf("serial fallback error = %v", err)
	}
	if len(replayed) != len(records) {
		t.Fatalf("serial replay count = %d, want %d", len(replayed), len(records))
	}
	for index, sequence := range replayed {
		if sequence != records[index].Sequence {
			t.Fatalf("serial replay sequence[%d] = %d, want %d", index, sequence, records[index].Sequence)
		}
	}
}

func TestT042ParallelReplayRejectsInvalidOptions(t *testing.T) {
	records := t042ReplayBenchmarkRecords()[:1]
	cases := []struct {
		name    string
		options ParallelReplayOptions
	}{
		{name: "negative workers", options: ParallelReplayOptions{Workers: -1, Apply: func(context.Context, hatJournal.Record) error { return nil }}},
		{name: "too many workers", options: ParallelReplayOptions{Workers: MaxParallelReplayWorkers + 1, Apply: func(context.Context, hatJournal.Record) error { return nil }}},
		{name: "nil apply", options: ParallelReplayOptions{Workers: 1}},
		{name: "parallel key required", options: ParallelReplayOptions{Workers: 2, Apply: func(context.Context, hatJournal.Record) error { return nil }}},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if err := ReplayJournalRecordsParallel(context.Background(), records, testCase.options); !errors.Is(err, ErrParallelReplayInvalid) {
				t.Fatalf("error = %v, want ErrParallelReplayInvalid", err)
			}
		})
	}
}

func TestT042ParallelReplayStopsOnApplyError(t *testing.T) {
	records := t042ReplayBenchmarkRecords()[:128]
	wantErr := errors.New("apply failed")
	err := ReplayJournalRecordsParallel(context.Background(), records, ParallelReplayOptions{
		Workers: 4,
		Key: func(record hatJournal.Record) string {
			return record.Request.Key
		},
		Apply: func(_ context.Context, record hatJournal.Record) error {
			if record.Sequence == 42 {
				return wantErr
			}
			return nil
		},
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("error = %v, want apply error", err)
	}
}

func TestT042ParallelReplayHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	called := false
	err := ReplayJournalRecordsParallel(ctx, t042ReplayBenchmarkRecords()[:1], ParallelReplayOptions{
		Apply: func(context.Context, hatJournal.Record) error {
			called = true
			return nil
		},
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	if called {
		t.Fatal("apply callback ran after cancellation")
	}
}
