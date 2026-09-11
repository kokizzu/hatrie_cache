package hatStorage

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

var (
	// ErrCompactionSchedulerNil reports a method call on a nil scheduler.
	ErrCompactionSchedulerNil = errors.New("hatriecache: compaction scheduler is nil")
	// ErrCompactionSchedulerOptionsInvalid reports an invalid scheduler limit.
	ErrCompactionSchedulerOptionsInvalid = errors.New("hatriecache: compaction scheduler options are invalid")
	// ErrCompactionTaskInvalid reports an empty task name or missing callback.
	ErrCompactionTaskInvalid = errors.New("hatriecache: compaction task is invalid")
)

// DefaultCompactionSchedulerMaxConcurrent keeps maintenance serialized unless
// a caller explicitly opts into parallel compaction.
const DefaultCompactionSchedulerMaxConcurrent = 1

// CompactionSchedulerOptions configures caller-driven persistent-shard
// compaction. The scheduler has no timer and does not run in the background.
type CompactionSchedulerOptions struct {
	MaxConcurrent int
}

// CompactionRun summarizes one drain of the currently queued compaction jobs.
// Failed jobs are put back in the queue so a later Run can retry them.
type CompactionRun struct {
	Scheduled int
	Completed int
	Failed    int
}

type compactionTask struct {
	name string
	run  func(context.Context) error
}

// CompactionScheduler coalesces compaction requests by task name and bounds
// maintenance concurrency. It is independent of any particular storage
// engine; callers typically schedule a closure around an engine's Compact
// method.
type CompactionScheduler struct {
	runMu sync.Mutex
	mu    sync.Mutex

	maxConcurrent int
	pending       map[string]func(context.Context) error
	running       map[string]struct{}
	scheduled     uint64
	completed     uint64
	failed        uint64
}

// NewCompactionScheduler validates and creates a compaction scheduler. A zero
// MaxConcurrent selects DefaultCompactionSchedulerMaxConcurrent.
func NewCompactionScheduler(options CompactionSchedulerOptions) (*CompactionScheduler, error) {
	if options.MaxConcurrent < 0 {
		return nil, ErrCompactionSchedulerOptionsInvalid
	}
	if options.MaxConcurrent == 0 {
		options.MaxConcurrent = DefaultCompactionSchedulerMaxConcurrent
	}
	return &CompactionScheduler{
		maxConcurrent: options.MaxConcurrent,
		pending:       make(map[string]func(context.Context) error),
		running:       make(map[string]struct{}),
	}, nil
}

// Schedule requests one compaction for name. Duplicate requests are
// coalesced while the task is queued or running. The bool is false when an
// equivalent request is already pending or executing.
func (scheduler *CompactionScheduler) Schedule(name string, run func(context.Context) error) (bool, error) {
	if scheduler == nil {
		return false, ErrCompactionSchedulerNil
	}
	name = strings.TrimSpace(name)
	if name == "" || run == nil {
		return false, ErrCompactionTaskInvalid
	}
	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()
	if _, exists := scheduler.pending[name]; exists {
		return false, nil
	}
	if _, exists := scheduler.running[name]; exists {
		return false, nil
	}
	scheduler.pending[name] = run
	return true, nil
}

// Pending reports the number of queued tasks that have not started.
func (scheduler *CompactionScheduler) Pending() int {
	if scheduler == nil {
		return 0
	}
	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()
	return len(scheduler.pending)
}

// Run drains the tasks queued at the start of the call. It waits for all
// selected callbacks and returns a combined error if any callback fails.
// Failed callbacks remain queued for a future retry. Concurrent Run calls are
// serialized, while Schedule remains safe during a run.
func (scheduler *CompactionScheduler) Run(ctx context.Context) (CompactionRun, error) {
	if scheduler == nil {
		return CompactionRun{}, ErrCompactionSchedulerNil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	scheduler.runMu.Lock()
	defer scheduler.runMu.Unlock()

	tasks := scheduler.takePending()
	result := CompactionRun{Scheduled: len(tasks)}
	if len(tasks) == 0 {
		return result, nil
	}
	sort.Slice(tasks, func(left, right int) bool {
		return tasks[left].name < tasks[right].name
	})

	workers := scheduler.maxConcurrent
	if workers > len(tasks) {
		workers = len(tasks)
	}
	var next atomic.Int64
	errs := make([]error, len(tasks))
	var wait sync.WaitGroup
	wait.Add(workers)
	for range workers {
		go func() {
			defer wait.Done()
			for {
				index := int(next.Add(1)) - 1
				if index >= len(tasks) {
					return
				}
				errs[index] = tasks[index].run(ctx)
			}
		}()
	}
	wait.Wait()

	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()
	failures := make([]error, 0)
	for index, task := range tasks {
		err := errs[index]
		delete(scheduler.running, task.name)
		if err == nil {
			result.Completed++
			scheduler.completed++
			continue
		}
		result.Failed++
		scheduler.failed++
		if _, alreadyQueued := scheduler.pending[task.name]; !alreadyQueued {
			scheduler.pending[task.name] = task.run
		}
		failures = append(failures, fmt.Errorf("compaction task %q: %w", task.name, err))
	}
	if len(failures) > 0 {
		return result, errors.Join(failures...)
	}
	return result, nil
}

func (scheduler *CompactionScheduler) takePending() []compactionTask {
	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()
	tasks := make([]compactionTask, 0, len(scheduler.pending))
	for name, run := range scheduler.pending {
		tasks = append(tasks, compactionTask{name: name, run: run})
		delete(scheduler.pending, name)
		scheduler.running[name] = struct{}{}
	}
	scheduler.scheduled += uint64(len(tasks))
	return tasks
}
