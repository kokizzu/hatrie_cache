package hatReplication

import (
	"context"
	"errors"
	"testing"
	"time"

	"hatrie_cache/hat/hatCommand"
	"hatrie_cache/hat/hatJournal"
)

func TestTT006HotStandbyReplaysContiguousBatchesAndPromotes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	standby, err := NewHotStandby(HotStandbyOptions{
		StandbyID:     "standby-a",
		SourceID:      "source-a",
		StartSequence: 2,
		BatchSize:     2,
		PollInterval:  time.Millisecond,
		FencingToken:  7,
	})
	if err != nil {
		t.Fatalf("NewHotStandby() error = %v", err)
	}

	fetches := 0
	applied := make([]uint64, 0, 2)
	source := HotStandbyFetchFunc(func(_ context.Context, afterSequence uint64, limit int) (HotStandbyBatch, error) {
		fetches++
		switch fetches {
		case 1:
			if afterSequence != 2 || limit != 2 {
				t.Fatalf("first fetch = after %d limit %d, want after 2 limit 2", afterSequence, limit)
			}
			return HotStandbyBatch{
				SourceSequence: 4,
				Records: []hatJournal.Record{
					{Sequence: 3, Request: hatCommand.Request{Command: "SETSTR", Key: "a"}},
					{Sequence: 4, Request: hatCommand.Request{Command: "SETSTR", Key: "b"}},
				},
			}, nil
		case 2:
			if afterSequence != 4 {
				t.Fatalf("second fetch after = %d, want 4", afterSequence)
			}
			return HotStandbyBatch{SourceSequence: 4}, nil
		default:
			cancel()
			return HotStandbyBatch{SourceSequence: afterSequence}, nil
		}
	})
	applier := HotStandbyApplyFunc(func(_ context.Context, batch HotStandbyBatch) error {
		for _, record := range batch.Records {
			applied = append(applied, record.Sequence)
		}
		return nil
	})

	err = standby.Run(ctx, source, applier)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v, want context.Canceled", err)
	}
	if got, want := applied, []uint64{3, 4}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("applied sequences = %v, want %v", got, want)
	}
	state := standby.Snapshot()
	if state.AppliedSequence != 4 || state.SourceSequence != 4 || state.Lag != 0 || state.Phase != HotStandbyPhaseStopped {
		t.Fatalf("stopped state = %#v, want applied/source 4, zero lag, stopped", state)
	}
	if _, err := standby.Promote(state.Generation, 8); !errors.Is(err, ErrHotStandbyFencing) {
		t.Fatalf("Promote() error = %v, want ErrHotStandbyFencing", err)
	}
	if _, err := standby.Promote(state.Generation-1, 7); !errors.Is(err, ErrHotStandbyGeneration) {
		t.Fatalf("Promote() error = %v, want ErrHotStandbyGeneration", err)
	}

	state, err = standby.Promote(state.Generation, 7)
	if err != nil {
		t.Fatalf("Promote() error = %v", err)
	}
	if state.Phase != HotStandbyPhaseActive || state.AppliedSequence != 4 {
		t.Fatalf("promoted state = %#v, want active at sequence 4", state)
	}
}

func TestTT006HotStandbyRejectsSequenceGapsAndStalePromotion(t *testing.T) {
	standby, err := NewHotStandby(HotStandbyOptions{
		StandbyID:    "standby-a",
		SourceID:     "source-a",
		BatchSize:    4,
		PollInterval: time.Millisecond,
		FencingToken: 9,
	})
	if err != nil {
		t.Fatalf("NewHotStandby() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	source := HotStandbyFetchFunc(func(context.Context, uint64, int) (HotStandbyBatch, error) {
		return HotStandbyBatch{
			SourceSequence: 3,
			Records:        []hatJournal.Record{{Sequence: 2, Request: hatCommand.Request{Command: "SETSTR", Key: "gap"}}},
		}, nil
	})
	err = standby.Run(ctx, source, HotStandbyApplyFunc(func(context.Context, HotStandbyBatch) error {
		t.Fatal("applier called for a non-contiguous batch")
		return nil
	}))
	if !errors.Is(err, ErrHotStandbySequenceGap) {
		t.Fatalf("Run() error = %v, want ErrHotStandbySequenceGap", err)
	}
	state := standby.Snapshot()
	if state.Phase != HotStandbyPhaseFailed || state.AppliedSequence != 0 || state.Lag != 3 {
		t.Fatalf("failed state = %#v, want failed with zero applied and lag 3", state)
	}
	if _, err := standby.Promote(state.Generation, 9); !errors.Is(err, ErrHotStandbyPhase) {
		t.Fatalf("Promote() error = %v, want ErrHotStandbyPhase", err)
	}
}

func TestTT006HotStandbyValidationAndCancellation(t *testing.T) {
	if _, err := NewHotStandby(HotStandbyOptions{BatchSize: -1}); !errors.Is(err, ErrHotStandbyInvalid) {
		t.Fatalf("negative batch size error = %v, want ErrHotStandbyInvalid", err)
	}
	standby, err := NewHotStandby(HotStandbyOptions{FencingToken: 1})
	if err != nil {
		t.Fatalf("NewHotStandby() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = standby.Run(ctx, HotStandbyFetchFunc(func(context.Context, uint64, int) (HotStandbyBatch, error) {
		t.Fatal("fetcher called after cancellation")
		return HotStandbyBatch{}, nil
	}), HotStandbyApplyFunc(func(context.Context, HotStandbyBatch) error { return nil }))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v, want context.Canceled", err)
	}
	if standby.Snapshot().Phase != HotStandbyPhaseStopped {
		t.Fatalf("phase = %v, want stopped", standby.Snapshot().Phase)
	}
}
