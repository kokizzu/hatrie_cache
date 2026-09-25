package hatReplication

import (
	"context"
	"errors"
	"testing"
	"time"

	"hatrie_cache/hat/hatJournal"
)

func BenchmarkTT006HotStandbyReplay(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		standby, err := NewHotStandby(HotStandbyOptions{
			BatchSize:    1,
			PollInterval: time.Nanosecond,
			FencingToken: 1,
		})
		if err != nil {
			b.Fatal(err)
		}
		fetches := 0
		source := HotStandbyFetchFunc(func(_ context.Context, afterSequence uint64, _ int) (HotStandbyBatch, error) {
			fetches++
			if fetches > 64 {
				cancel()
				return HotStandbyBatch{SourceSequence: afterSequence}, nil
			}
			sequence := afterSequence + 1
			return HotStandbyBatch{
				SourceSequence: sequence,
				Records:        []hatJournal.Record{{Sequence: sequence}},
			}, nil
		})
		err = standby.Run(ctx, source, HotStandbyApplyFunc(func(context.Context, HotStandbyBatch) error { return nil }))
		cancel()
		if !errors.Is(err, context.Canceled) {
			b.Fatalf("Run() error = %v, want context.Canceled", err)
		}
	}
}

func BenchmarkTT006DirectContiguousLoop(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var applied uint64
		for sequence := uint64(1); sequence <= 64; sequence++ {
			if sequence != applied+1 {
				b.Fatalf("sequence = %d, applied = %d", sequence, applied)
			}
			applied = sequence
		}
	}
}
