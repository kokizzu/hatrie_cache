package hatPipeline

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
)

var (
	// ErrParallelReplicaReadTaskInvalid indicates a missing partition, replica,
	// or reader callback.
	ErrParallelReplicaReadTaskInvalid = errors.New("hatPipeline: parallel replica read task is invalid")
	// ErrParallelReplicaReadDuplicatePartition indicates that two replicas were
	// assigned the same partition and would risk duplicate work.
	ErrParallelReplicaReadDuplicatePartition = errors.New("hatPipeline: parallel replica read partition is assigned more than once")
	// ErrParallelReplicaReadConcurrencyInvalid indicates an invalid concurrency
	// limit.
	ErrParallelReplicaReadConcurrencyInvalid = errors.New("hatPipeline: parallel replica read concurrency is invalid")
	// ErrParallelReplicaReadTasksInvalid indicates too many tasks were submitted.
	ErrParallelReplicaReadTasksInvalid = errors.New("hatPipeline: too many parallel replica read tasks")
)

const (
	// DefaultParallelReplicaReadConcurrency bounds default fan-out without
	// creating one goroutine per replica task.
	DefaultParallelReplicaReadConcurrency = 4
	// MaxParallelReplicaReadConcurrency bounds caller-configured fan-out.
	MaxParallelReplicaReadConcurrency = 256
	// MaxParallelReplicaReadTasks bounds result and queue allocation.
	MaxParallelReplicaReadTasks = 4096
)

// ParallelReplicaReadOptions configures RunParallelReplicaReads. A zero
// MaxConcurrency uses DefaultParallelReplicaReadConcurrency; negative values
// are rejected. The feature is opt-in and has no effect on other pipelines.
type ParallelReplicaReadOptions struct {
	MaxConcurrency int
}

// ParallelReplicaReadTask assigns one query partition to exactly one replica.
// Read must honor ctx cancellation. The returned row slice becomes owned by
// the result and is not copied.
type ParallelReplicaReadTask[T any] struct {
	Partition string
	Replica   string
	Read      func(context.Context) ([]T, error)
}

// ParallelReplicaReadResult contains one partition's rows. Results are
// returned in the same order as the input task slice, regardless of completion
// order.
type ParallelReplicaReadResult[T any] struct {
	Partition string
	Replica   string
	Rows      []T
}

// RunParallelReplicaReads executes assigned partition reads with bounded
// concurrency. It cancels remaining work after the first reader error and
// returns no partial result. It rejects duplicate partition assignments so the
// coordinator cannot silently return duplicate rows.
func RunParallelReplicaReads[T any](ctx context.Context, tasks []ParallelReplicaReadTask[T], options ParallelReplicaReadOptions) ([]ParallelReplicaReadResult[T], error) {
	if options.MaxConcurrency < 0 || options.MaxConcurrency > MaxParallelReplicaReadConcurrency {
		return nil, ErrParallelReplicaReadConcurrencyInvalid
	}
	if len(tasks) > MaxParallelReplicaReadTasks {
		return nil, ErrParallelReplicaReadTasksInvalid
	}
	if len(tasks) == 0 {
		return []ParallelReplicaReadResult[T]{}, nil
	}
	normalized := make([]ParallelReplicaReadTask[T], len(tasks))
	seen := make(map[string]struct{}, len(tasks))
	for index, task := range tasks {
		task.Partition = strings.TrimSpace(task.Partition)
		task.Replica = strings.TrimSpace(task.Replica)
		if task.Partition == "" || task.Replica == "" || task.Read == nil {
			return nil, fmt.Errorf("%w at task %d", ErrParallelReplicaReadTaskInvalid, index)
		}
		if _, exists := seen[task.Partition]; exists {
			return nil, fmt.Errorf("%w: %q", ErrParallelReplicaReadDuplicatePartition, task.Partition)
		}
		seen[task.Partition] = struct{}{}
		normalized[index] = task
	}
	if ctx == nil {
		ctx = context.Background()
	}
	workers := options.MaxConcurrency
	if workers == 0 {
		workers = DefaultParallelReplicaReadConcurrency
	}
	if workers > len(normalized) {
		workers = len(normalized)
	}
	if workers == 1 {
		results := make([]ParallelReplicaReadResult[T], len(normalized))
		for index, task := range normalized {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			rows, err := task.Read(ctx)
			if err != nil {
				return nil, fmt.Errorf("parallel replica read partition %q on replica %q (task %d): %w", task.Partition, task.Replica, index, err)
			}
			results[index] = ParallelReplicaReadResult[T]{
				Partition: task.Partition,
				Replica:   task.Replica,
				Rows:      rows,
			}
		}
		return results, nil
	}
	workContext, cancel := context.WithCancel(ctx)
	defer cancel()
	type indexedTask struct {
		index int
		task  ParallelReplicaReadTask[T]
	}
	jobs := make(chan indexedTask)
	results := make([]ParallelReplicaReadResult[T], len(normalized))
	var workersDone sync.WaitGroup
	var errorMu sync.Mutex
	var firstErr error
	recordError := func(index int, task ParallelReplicaReadTask[T], err error) {
		if err == nil {
			return
		}
		errorMu.Lock()
		defer errorMu.Unlock()
		if firstErr != nil {
			return
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			firstErr = ctxErr
		} else {
			firstErr = fmt.Errorf("parallel replica read partition %q on replica %q (task %d): %w", task.Partition, task.Replica, index, err)
		}
		cancel()
	}
	workersDone.Add(workers)
	for range workers {
		go func() {
			defer workersDone.Done()
			for job := range jobs {
				if workContext.Err() != nil {
					continue
				}
				rows, err := job.task.Read(workContext)
				if err != nil {
					recordError(job.index, job.task, err)
					continue
				}
				results[job.index] = ParallelReplicaReadResult[T]{
					Partition: job.task.Partition,
					Replica:   job.task.Replica,
					Rows:      rows,
				}
			}
		}()
	}
	go func() {
		defer close(jobs)
		for index, task := range normalized {
			select {
			case jobs <- indexedTask{index: index, task: task}:
			case <-workContext.Done():
				return
			}
		}
	}()
	workersDone.Wait()
	errorMu.Lock()
	err := firstErr
	errorMu.Unlock()
	if err != nil {
		return nil, err
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, ctxErr
	}
	return results, nil
}
