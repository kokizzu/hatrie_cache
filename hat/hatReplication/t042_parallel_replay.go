package hatReplication

import (
	"context"
	"errors"
	"sync"

	"hatrie_cache/hat/hatJournal"
)

const (
	// MaxParallelReplayWorkers bounds the number of replay lanes created by
	// one recovery operation.
	MaxParallelReplayWorkers = 64
	// MaxParallelReplayRecords bounds the caller-owned batch passed to one
	// replay operation.
	MaxParallelReplayRecords      = 1 << 20
	parallelReplayRecordThreshold = 32
)

var (
	// ErrParallelReplayInvalid reports invalid replay options or an oversized
	// record batch.
	ErrParallelReplayInvalid = errors.New("hatReplication: invalid parallel replay options")
)

// ParallelReplayOptions configures ReplayJournalRecordsParallel. Workers set
// to zero selects the serial default. Parallel replay requires Key to return
// the same logical key for records whose relative order must be preserved.
// Apply must be safe to run concurrently for different keys and must not rely
// on a global order between different keys.
type ParallelReplayOptions struct {
	Workers int
	Key     func(hatJournal.Record) string
	Apply   func(context.Context, hatJournal.Record) error
}

// ReplayJournalRecordsParallel replays a bounded record batch. Records with
// the same key are assigned to one lane and applied in their input order;
// different lanes may run concurrently. The default is serial, so existing
// recovery callers do not change behavior unless they opt into Workers > 1.
// The caller is responsible for ensuring that different keys are independent
// for its storage and command semantics.
func ReplayJournalRecordsParallel(ctx context.Context, records []hatJournal.Record, options ParallelReplayOptions) error {
	workers, err := validateParallelReplayOptions(ctx, len(records), options)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(records) == 0 || workers == 1 || len(records) < parallelReplayRecordThreshold {
		return replayJournalRecordsSerial(ctx, records, options.Apply)
	}

	laneForRecord := make([]uint8, len(records))
	laneCounts := make([]int, workers)
	for index, record := range records {
		if err := ctx.Err(); err != nil {
			return err
		}
		lane := parallelReplayLane(options.Key(record), workers)
		laneForRecord[index] = uint8(lane)
		laneCounts[lane]++
	}
	lanes := make([][]int, workers)
	lanePositions := make([]int, workers)
	for lane, count := range laneCounts {
		lanes[lane] = make([]int, count)
	}
	for index, lane := range laneForRecord {
		laneIndex := int(lane)
		lanes[laneIndex][lanePositions[laneIndex]] = index
		lanePositions[laneIndex]++
	}

	parallelContext, cancel := context.WithCancel(ctx)
	defer cancel()
	var waitGroup sync.WaitGroup
	var firstError error
	var firstErrorOnce sync.Once
	for _, lane := range lanes {
		if len(lane) == 0 {
			continue
		}
		lane := lane
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			for _, recordIndex := range lane {
				if err := parallelContext.Err(); err != nil {
					return
				}
				if err := options.Apply(parallelContext, records[recordIndex]); err != nil {
					firstErrorOnce.Do(func() { firstError = err })
					cancel()
					return
				}
			}
		}()
	}
	waitGroup.Wait()
	if firstError != nil {
		return firstError
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

func validateParallelReplayOptions(ctx context.Context, recordCount int, options ParallelReplayOptions) (int, error) {
	if ctx == nil || options.Apply == nil {
		return 0, ErrParallelReplayInvalid
	}
	workers := options.Workers
	if workers == 0 {
		workers = 1
	}
	if workers < 1 || workers > MaxParallelReplayWorkers || recordCount > MaxParallelReplayRecords {
		return 0, ErrParallelReplayInvalid
	}
	if workers > 1 && options.Key == nil {
		return 0, ErrParallelReplayInvalid
	}
	return workers, nil
}

func replayJournalRecordsSerial(ctx context.Context, records []hatJournal.Record, apply func(context.Context, hatJournal.Record) error) error {
	for _, record := range records {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := apply(ctx, record); err != nil {
			return err
		}
	}
	return nil
}

func parallelReplayLane(key string, workers int) int {
	const offset uint64 = 14695981039346656037
	const prime uint64 = 1099511628211
	hash := offset
	for index := 0; index < len(key); index++ {
		hash ^= uint64(key[index])
		hash *= prime
	}
	return int(hash % uint64(workers))
}
