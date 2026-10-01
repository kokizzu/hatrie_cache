package hatJournal

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

const MaxReplayWorkers = 256

// ErrInvalidReplay reports invalid replay options or task input.
var ErrInvalidReplay = errors.New("hatJournal: invalid replay")

// ReplayTask is one already-decoded journal mutation. Tasks must be supplied
// in increasing sequence order. Partition identifies the independent replay
// lane; tasks in one lane always execute serially in input order.
type ReplayTask struct {
	Sequence  uint64
	Partition uint64
	Apply     func(context.Context) error
}

// ReplayOptions controls recovery replay. Workers zero or one preserves the
// serial recovery path. Values greater than one enable bounded concurrency
// across independent partitions.
type ReplayOptions struct {
	Workers int
}

// ParallelReplay applies journal tasks while preserving per-partition order.
// Validation happens before the first task is applied, so malformed input
// cannot leave a partially replayed batch. The default is serial replay.
func ParallelReplay(ctx context.Context, tasks []ReplayTask, options ReplayOptions) error {
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", ErrInvalidReplay)
	}
	if err := validateReplay(tasks, options); err != nil {
		return err
	}
	if len(tasks) < 2 || options.Workers <= 1 || oneReplayPartition(tasks) {
		return replaySerial(ctx, tasks)
	}

	lanes := makeReplayLanes(tasks, options.Workers)
	workers := options.Workers
	if workers > len(lanes) {
		workers = len(lanes)
	}
	return replayLanes(ctx, lanes, workers, tasks)
}

func validateReplay(tasks []ReplayTask, options ReplayOptions) error {
	if options.Workers < 0 || options.Workers > MaxReplayWorkers {
		return fmt.Errorf("%w: workers must be between 0 and %d", ErrInvalidReplay, MaxReplayWorkers)
	}
	var previous uint64
	for index, task := range tasks {
		if task.Sequence == 0 {
			return fmt.Errorf("%w: task %d has zero sequence", ErrInvalidReplay, index)
		}
		if index > 0 && task.Sequence <= previous {
			return fmt.Errorf("%w: task %d sequence %d does not follow %d", ErrInvalidReplay, index, task.Sequence, previous)
		}
		if task.Apply == nil {
			return fmt.Errorf("%w: task %d has no apply function", ErrInvalidReplay, index)
		}
		previous = task.Sequence
	}
	return nil
}

func oneReplayPartition(tasks []ReplayTask) bool {
	partition := tasks[0].Partition
	for _, task := range tasks[1:] {
		if task.Partition != partition {
			return false
		}
	}
	return true
}

func replaySerial(ctx context.Context, tasks []ReplayTask) error {
	for _, task := range tasks {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := task.Apply(ctx); err != nil {
			return replayTaskError(task, err)
		}
	}
	return nil
}

func makeReplayLanes(tasks []ReplayTask, workers int) [][]int {
	type laneInfo struct {
		index int
		count int
	}
	infoByPartition := make(map[uint64]laneInfo, workers)
	partitions := make([]uint64, 0, workers)
	for _, task := range tasks {
		info, ok := infoByPartition[task.Partition]
		if !ok {
			partitions = append(partitions, task.Partition)
		}
		info.count++
		infoByPartition[task.Partition] = info
	}

	lanes := make([][]int, len(partitions))
	for index, partition := range partitions {
		info := infoByPartition[partition]
		info.index = index
		infoByPartition[partition] = info
		lanes[index] = make([]int, 0, info.count)
	}
	for taskIndex, task := range tasks {
		info := infoByPartition[task.Partition]
		lanes[info.index] = append(lanes[info.index], taskIndex)
	}
	return lanes
}

func replayLanes(ctx context.Context, lanes [][]int, workers int, tasks []ReplayTask) error {
	runContext, cancel := context.WithCancel(ctx)
	defer cancel()

	type failureState struct {
		sequence uint64
		err      error
		set      bool
	}
	var failure failureState
	var failureMu sync.Mutex
	recordFailure := func(task ReplayTask, err error) {
		failureMu.Lock()
		defer failureMu.Unlock()
		if !failure.set || task.Sequence < failure.sequence {
			failure.sequence = task.Sequence
			failure.err = err
			failure.set = true
		}
	}

	jobs := make(chan []int)
	var wait sync.WaitGroup
	wait.Add(workers)
	for worker := 0; worker < workers; worker++ {
		go func() {
			defer wait.Done()
			for {
				select {
				case <-runContext.Done():
					return
				case lane, ok := <-jobs:
					if !ok {
						return
					}
					for _, taskIndex := range lane {
						task := tasks[taskIndex]
						if err := runContext.Err(); err != nil {
							return
						}
						if err := task.Apply(runContext); err != nil {
							if runContext.Err() == nil {
								recordFailure(task, err)
								cancel()
							}
							return
						}
					}
				}
			}
		}()
	}
	for _, lane := range lanes {
		select {
		case <-runContext.Done():
			break
		case jobs <- lane:
		}
		if runContext.Err() != nil {
			break
		}
	}
	close(jobs)
	wait.Wait()

	failureMu.Lock()
	defer failureMu.Unlock()
	if failure.set {
		return replayTaskError(ReplayTask{Sequence: failure.sequence}, failure.err)
	}
	return ctx.Err()
}

func replayTaskError(task ReplayTask, err error) error {
	return fmt.Errorf("hatJournal: replay sequence %d: %w", task.Sequence, err)
}
