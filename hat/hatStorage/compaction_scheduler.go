package hatStorage

import (
	"context"
	"errors"
	"fmt"
	"math/bits"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// ErrCompactionSchedulerNil reports a method call on a nil scheduler.
	ErrCompactionSchedulerNil = errors.New("hatriecache: compaction scheduler is nil")
	// ErrCompactionSchedulerOptionsInvalid reports an invalid scheduler limit.
	ErrCompactionSchedulerOptionsInvalid = errors.New("hatriecache: compaction scheduler options are invalid")
	// ErrCompactionTaskInvalid reports an empty task name or missing callback.
	ErrCompactionTaskInvalid = errors.New("hatriecache: compaction task is invalid")
)

// CompactionSelectionPolicy controls how explicitly prioritized compaction
// tasks are ordered when more than one task is ready to run.
type CompactionSelectionPolicy uint8

const (
	// CompactionSelectionPriority preserves the historical priority-first
	// ordering and is the zero-value default.
	CompactionSelectionPriority CompactionSelectionPolicy = iota
	// CompactionSelectionSizeTiered groups tasks by estimated size before
	// applying priority and deterministic tie breakers.
	CompactionSelectionSizeTiered
	// CompactionSelectionTimeAware prefers tasks that have waited longer.
	CompactionSelectionTimeAware
)

// DefaultCompactionSchedulerMaxConcurrent keeps maintenance serialized unless
// a caller explicitly opts into parallel compaction.
const DefaultCompactionSchedulerMaxConcurrent = 1

// CompactionSchedulerOptions configures caller-driven persistent-shard
// compaction. The scheduler has no timer and does not run in the background.
type CompactionSchedulerOptions struct {
	MaxConcurrent       int
	MaxIOBytesPerSecond uint64
	SelectionPolicy     CompactionSelectionPolicy
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

type compactionPendingTask struct {
	run func(context.Context) error
}

type compactionPriorityTask struct {
	priority       int
	run            func(context.Context) error
	estimatedBytes uint64
	enqueuedAt     time.Time
}

// CompactionScheduler coalesces compaction requests by task name and bounds
// maintenance concurrency. It is independent of any particular storage
// engine; callers typically schedule a closure around an engine's Compact
// method.
type CompactionScheduler struct {
	runMu sync.Mutex
	mu    sync.Mutex

	maxConcurrent   int
	selectionPolicy CompactionSelectionPolicy
	pending         map[string]compactionPendingTask
	priorityPending map[string]compactionPriorityTask
	running         map[string]struct{}
	now             func() time.Time
	oldestPending   time.Time
	oldestRunning   time.Time
	scheduled       uint64
	completed       uint64
	failed          uint64
	pendingBytes    uint64
	runningBytes    uint64
	ioState         *compactionSchedulerIOState
}

// NewCompactionScheduler validates and creates a compaction scheduler. A zero
// MaxConcurrent selects DefaultCompactionSchedulerMaxConcurrent.
func NewCompactionScheduler(options CompactionSchedulerOptions) (*CompactionScheduler, error) {
	if options.MaxConcurrent < 0 || options.SelectionPolicy > CompactionSelectionTimeAware {
		return nil, ErrCompactionSchedulerOptionsInvalid
	}
	if options.MaxConcurrent == 0 {
		options.MaxConcurrent = DefaultCompactionSchedulerMaxConcurrent
	}
	return &CompactionScheduler{
		maxConcurrent:   options.MaxConcurrent,
		selectionPolicy: options.SelectionPolicy,
		pending:         make(map[string]compactionPendingTask),
		running:         make(map[string]struct{}),
		now:             time.Now,
		ioState:         newCompactionSchedulerIOState(options.MaxIOBytesPerSecond),
	}, nil
}

// Schedule requests one default-priority compaction for name. Duplicate
// requests are coalesced while the task is queued or running. The bool is
// false when an equivalent request is already pending or executing.
func (scheduler *CompactionScheduler) Schedule(name string, run func(context.Context) error) (bool, error) {
	return scheduler.schedule(name, 0, run)
}

// ScheduleWithIO requests a default-priority compaction with an estimated
// amount of disk work. When MaxIOBytesPerSecond is configured, the estimate is
// used to pace task starts. A zero estimate keeps the task unthrottled.
func (scheduler *CompactionScheduler) ScheduleWithIO(name string, estimatedBytes uint64, run func(context.Context) error) (bool, error) {
	return scheduler.schedule(name, estimatedBytes, run)
}

func (scheduler *CompactionScheduler) schedule(name string, ioBytes uint64, run func(context.Context) error) (bool, error) {
	if scheduler == nil {
		return false, ErrCompactionSchedulerNil
	}
	name = strings.TrimSpace(name)
	if name == "" || run == nil {
		return false, ErrCompactionTaskInvalid
	}
	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()
	if scheduler.priorityPending != nil {
		if _, exists := scheduler.running[name]; exists {
			return false, nil
		}
		if pending, exists := scheduler.priorityPending[name]; exists {
			if ioBytes > pending.estimatedBytes {
				scheduler.pendingBytes = saturatingCompactionBytes(scheduler.pendingBytes, ioBytes-pending.estimatedBytes)
				pending.estimatedBytes = ioBytes
				scheduler.priorityPending[name] = pending
				scheduler.setIOEstimateLocked(name, ioBytes)
			}
			return false, nil
		}
		if scheduler.oldestPending.IsZero() {
			scheduler.oldestPending = scheduler.now()
		}
		scheduler.priorityPending[name] = compactionPriorityTask{
			run:            run,
			estimatedBytes: ioBytes,
			enqueuedAt:     scheduler.now(),
		}
		scheduler.setIOEstimateLocked(name, ioBytes)
		scheduler.pendingBytes = saturatingCompactionBytes(scheduler.pendingBytes, ioBytes)
		return true, nil
	}
	if scheduler.pending == nil {
		scheduler.pending = make(map[string]compactionPendingTask)
	}
	if _, exists := scheduler.pending[name]; exists {
		return false, nil
	}
	if _, exists := scheduler.running[name]; exists {
		return false, nil
	}
	if scheduler.oldestPending.IsZero() {
		scheduler.oldestPending = scheduler.now()
	}
	scheduler.pending[name] = compactionPendingTask{run: run}
	scheduler.setIOEstimateLocked(name, ioBytes)
	scheduler.pendingBytes = saturatingCompactionBytes(scheduler.pendingBytes, ioBytes)
	return true, nil
}

// ScheduleWithPriority requests one compaction with an explicit priority.
// Higher priorities run first; equal priorities retain deterministic name
// ordering. A higher-priority duplicate updates a task that is still queued,
// while the currently running task remains unchanged.
func (scheduler *CompactionScheduler) ScheduleWithPriority(name string, priority int, run func(context.Context) error) (bool, error) {
	return scheduler.scheduleWithPriority(name, priority, 0, run)
}

// ScheduleWithPriorityAndIO requests a prioritized compaction with an
// estimated disk-work cost. A duplicate keeps the larger queued estimate so
// coalescing cannot accidentally under-throttle the eventual task.
func (scheduler *CompactionScheduler) ScheduleWithPriorityAndIO(name string, priority int, estimatedBytes uint64, run func(context.Context) error) (bool, error) {
	return scheduler.scheduleWithPriority(name, priority, estimatedBytes, run)
}

func (scheduler *CompactionScheduler) scheduleWithPriority(name string, priority int, ioBytes uint64, run func(context.Context) error) (bool, error) {
	if priority == 0 {
		return scheduler.schedule(name, ioBytes, run)
	}
	if scheduler == nil {
		return false, ErrCompactionSchedulerNil
	}
	name = strings.TrimSpace(name)
	if name == "" || run == nil {
		return false, ErrCompactionTaskInvalid
	}
	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()
	if scheduler.priorityPending == nil {
		scheduler.priorityPending = make(map[string]compactionPriorityTask, len(scheduler.pending)+1)
		queuedAt := scheduler.oldestPending
		if queuedAt.IsZero() {
			queuedAt = scheduler.now()
		}
		for pendingName, pending := range scheduler.pending {
			scheduler.priorityPending[pendingName] = compactionPriorityTask{
				run:            pending.run,
				estimatedBytes: scheduler.ioEstimateLocked(pendingName),
				enqueuedAt:     queuedAt,
			}
		}
		scheduler.pending = nil
	}
	if _, exists := scheduler.running[name]; exists {
		return false, nil
	}
	if pending, exists := scheduler.priorityPending[name]; exists {
		if priority > pending.priority {
			pending.priority = priority
		}
		if ioBytes > pending.estimatedBytes {
			scheduler.pendingBytes = saturatingCompactionBytes(scheduler.pendingBytes, ioBytes-pending.estimatedBytes)
			pending.estimatedBytes = ioBytes
		}
		scheduler.priorityPending[name] = pending
		scheduler.setIOEstimateLocked(name, ioBytes)
		return false, nil
	}
	if scheduler.oldestPending.IsZero() {
		scheduler.oldestPending = scheduler.now()
	}
	scheduler.priorityPending[name] = compactionPriorityTask{
		priority:       priority,
		run:            run,
		estimatedBytes: ioBytes,
		enqueuedAt:     scheduler.now(),
	}
	scheduler.setIOEstimateLocked(name, ioBytes)
	scheduler.pendingBytes = saturatingCompactionBytes(scheduler.pendingBytes, ioBytes)
	return true, nil
}

// Pending reports the number of queued tasks that have not started.
func (scheduler *CompactionScheduler) Pending() int {
	if scheduler == nil {
		return 0
	}
	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()
	if scheduler.priorityPending != nil {
		pending := 0
		for name := range scheduler.priorityPending {
			if _, running := scheduler.running[name]; !running {
				pending++
			}
		}
		return pending
	}
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

	tasks, prioritized := scheduler.takePending()
	result := CompactionRun{Scheduled: len(tasks)}
	if len(tasks) == 0 {
		return result, nil
	}
	if len(tasks) == 1 {
		return scheduler.finishSingle(tasks[0], scheduler.executeTask(ctx, tasks[0]))
	}
	if prioritized {
		scheduler.mu.Lock()
		selectionPolicy := scheduler.selectionPolicy
		sort.Slice(tasks, func(left, right int) bool {
			return compactionPriorityTaskBefore(
				scheduler.priorityPending[tasks[left].name],
				scheduler.priorityPending[tasks[right].name],
				tasks[left].name,
				tasks[right].name,
				selectionPolicy,
			)
		})
		scheduler.mu.Unlock()
	} else {
		sort.Slice(tasks, func(left, right int) bool {
			return tasks[left].name < tasks[right].name
		})
	}

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
				errs[index] = scheduler.executeTask(ctx, tasks[index])
			}
		}()
	}
	wait.Wait()

	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()
	scheduler.oldestRunning = time.Time{}
	failures := make([]error, 0)
	for index, task := range tasks {
		err := errs[index]
		estimatedBytes := scheduler.ioEstimateLocked(task.name)
		delete(scheduler.running, task.name)
		scheduler.runningBytes = subtractCompactionBytes(scheduler.runningBytes, estimatedBytes)
		_, priorityTask := scheduler.priorityPending[task.name]
		if err == nil {
			if priorityTask {
				delete(scheduler.priorityPending, task.name)
			}
			scheduler.clearIOEstimateLocked(task.name)
			result.Completed++
			scheduler.completed++
			continue
		}
		result.Failed++
		scheduler.failed++
		if priorityTask {
			// The priority entry remains in the queue; only its running marker
			// was removed above.
			if scheduler.oldestPending.IsZero() {
				scheduler.oldestPending = scheduler.now()
			}
			scheduler.pendingBytes = saturatingCompactionBytes(scheduler.pendingBytes, estimatedBytes)
		} else if scheduler.priorityPending != nil {
			if scheduler.oldestPending.IsZero() {
				scheduler.oldestPending = scheduler.now()
			}
			scheduler.priorityPending[task.name] = compactionPriorityTask{run: task.run, estimatedBytes: estimatedBytes}
			scheduler.pendingBytes = saturatingCompactionBytes(scheduler.pendingBytes, estimatedBytes)
		} else if _, alreadyQueued := scheduler.pending[task.name]; !alreadyQueued {
			if scheduler.oldestPending.IsZero() {
				scheduler.oldestPending = scheduler.now()
			}
			scheduler.pending[task.name] = compactionPendingTask{run: task.run}
			scheduler.pendingBytes = saturatingCompactionBytes(scheduler.pendingBytes, estimatedBytes)
		}
		failures = append(failures, fmt.Errorf("compaction task %q: %w", task.name, err))
	}
	if len(scheduler.priorityPending) == 0 {
		scheduler.priorityPending = nil
	}
	if len(failures) > 0 {
		return result, errors.Join(failures...)
	}
	return result, nil
}

func compactionPriorityTaskBefore(left, right compactionPriorityTask, leftName, rightName string, policy CompactionSelectionPolicy) bool {
	switch policy {
	case CompactionSelectionSizeTiered:
		leftTier := compactionSizeTier(left.estimatedBytes)
		rightTier := compactionSizeTier(right.estimatedBytes)
		if leftTier != rightTier {
			return leftTier < rightTier
		}
		if left.estimatedBytes != right.estimatedBytes {
			return compactionEstimatedBytesBefore(left.estimatedBytes, right.estimatedBytes)
		}
		if left.priority != right.priority {
			return left.priority > right.priority
		}
		if !left.enqueuedAt.Equal(right.enqueuedAt) {
			return compactionEnqueuedBefore(left.enqueuedAt, right.enqueuedAt)
		}
	case CompactionSelectionTimeAware:
		if !left.enqueuedAt.Equal(right.enqueuedAt) {
			return compactionEnqueuedBefore(left.enqueuedAt, right.enqueuedAt)
		}
		if left.priority != right.priority {
			return left.priority > right.priority
		}
		if left.estimatedBytes != right.estimatedBytes {
			return compactionEstimatedBytesBefore(left.estimatedBytes, right.estimatedBytes)
		}
	default:
		if left.priority != right.priority {
			return left.priority > right.priority
		}
	}
	return leftName < rightName
}

func compactionSizeTier(estimatedBytes uint64) int {
	if estimatedBytes == 0 {
		return int(^uint(0) >> 1)
	}
	return bits.Len64(estimatedBytes)
}

func compactionEstimatedBytesBefore(left, right uint64) bool {
	if left == 0 {
		return false
	}
	if right == 0 {
		return true
	}
	return left < right
}

func compactionEnqueuedBefore(left, right time.Time) bool {
	if left.IsZero() {
		return false
	}
	if right.IsZero() {
		return true
	}
	return left.Before(right)
}

func (scheduler *CompactionScheduler) finishSingle(task compactionTask, err error) (CompactionRun, error) {
	result := CompactionRun{Scheduled: 1}
	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()
	scheduler.oldestRunning = time.Time{}
	estimatedBytes := scheduler.ioEstimateLocked(task.name)
	delete(scheduler.running, task.name)
	scheduler.runningBytes = subtractCompactionBytes(scheduler.runningBytes, estimatedBytes)
	_, priorityTask := scheduler.priorityPending[task.name]
	if err == nil {
		if priorityTask {
			delete(scheduler.priorityPending, task.name)
		}
		scheduler.clearIOEstimateLocked(task.name)
		result.Completed = 1
		scheduler.completed++
		return result, nil
	}
	result.Failed = 1
	scheduler.failed++
	if priorityTask {
		// The priority entry remains queued after the running marker is removed.
		if scheduler.oldestPending.IsZero() {
			scheduler.oldestPending = scheduler.now()
		}
		scheduler.pendingBytes = saturatingCompactionBytes(scheduler.pendingBytes, estimatedBytes)
	} else if scheduler.priorityPending != nil {
		if scheduler.oldestPending.IsZero() {
			scheduler.oldestPending = scheduler.now()
		}
		scheduler.priorityPending[task.name] = compactionPriorityTask{run: task.run, estimatedBytes: estimatedBytes}
		scheduler.pendingBytes = saturatingCompactionBytes(scheduler.pendingBytes, estimatedBytes)
	} else if _, alreadyQueued := scheduler.pending[task.name]; !alreadyQueued {
		if scheduler.oldestPending.IsZero() {
			scheduler.oldestPending = scheduler.now()
		}
		scheduler.pending[task.name] = compactionPendingTask{run: task.run}
		scheduler.pendingBytes = saturatingCompactionBytes(scheduler.pendingBytes, estimatedBytes)
	}
	if len(scheduler.priorityPending) == 0 {
		scheduler.priorityPending = nil
	}
	return result, errors.Join(fmt.Errorf("compaction task %q: %w", task.name, err))
}

func (scheduler *CompactionScheduler) takePending() ([]compactionTask, bool) {
	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()
	if scheduler.priorityPending != nil {
		if len(scheduler.priorityPending) == 0 {
			return nil, true
		}
		startedAt := scheduler.now()
		tasks := make([]compactionTask, 0, len(scheduler.priorityPending))
		for name, pending := range scheduler.priorityPending {
			if _, alreadyRunning := scheduler.running[name]; alreadyRunning {
				continue
			}
			tasks = append(tasks, compactionTask{name: name, run: pending.run})
			scheduler.running[name] = struct{}{}
			scheduler.pendingBytes = subtractCompactionBytes(scheduler.pendingBytes, pending.estimatedBytes)
			scheduler.runningBytes = saturatingCompactionBytes(scheduler.runningBytes, pending.estimatedBytes)
		}
		scheduler.oldestPending = time.Time{}
		scheduler.oldestRunning = startedAt
		scheduler.scheduled += uint64(len(tasks))
		return tasks, true
	}
	if len(scheduler.pending) == 0 {
		return nil, false
	}
	startedAt := scheduler.now()
	tasks := make([]compactionTask, 0, len(scheduler.pending))
	for name, pending := range scheduler.pending {
		estimatedBytes := scheduler.ioEstimateLocked(name)
		tasks = append(tasks, compactionTask{name: name, run: pending.run})
		delete(scheduler.pending, name)
		scheduler.running[name] = struct{}{}
		scheduler.pendingBytes = subtractCompactionBytes(scheduler.pendingBytes, estimatedBytes)
		scheduler.runningBytes = saturatingCompactionBytes(scheduler.runningBytes, estimatedBytes)
	}
	scheduler.oldestPending = time.Time{}
	scheduler.oldestRunning = startedAt
	scheduler.scheduled += uint64(len(tasks))
	return tasks, false
}

func (scheduler *CompactionScheduler) executeTask(ctx context.Context, task compactionTask) error {
	if scheduler.ioState == nil {
		return task.run(ctx)
	}
	if ioBytes := scheduler.ioEstimate(task.name); ioBytes > 0 {
		if err := scheduler.ioState.throttle.wait(ctx, ioBytes); err != nil {
			return err
		}
	}
	return task.run(ctx)
}

func (scheduler *CompactionScheduler) setIOEstimateLocked(name string, bytes uint64) {
	if bytes == 0 {
		return
	}
	if scheduler.ioState == nil {
		scheduler.ioState = &compactionSchedulerIOState{}
	}
	if scheduler.ioState.estimates == nil {
		scheduler.ioState.estimates = make(map[string]uint64)
	}
	if previous := scheduler.ioState.estimates[name]; bytes > previous {
		scheduler.ioState.estimates[name] = bytes
	}
}

func subtractCompactionBytes(total, bytes uint64) uint64 {
	if bytes >= total {
		return 0
	}
	return total - bytes
}

func (scheduler *CompactionScheduler) ioEstimate(name string) uint64 {
	if scheduler == nil || scheduler.ioState == nil {
		return 0
	}
	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()
	return scheduler.ioEstimateLocked(name)
}

func (scheduler *CompactionScheduler) ioEstimateLocked(name string) uint64 {
	if scheduler == nil || scheduler.ioState == nil || scheduler.ioState.estimates == nil {
		return 0
	}
	return scheduler.ioState.estimates[name]
}

func (scheduler *CompactionScheduler) clearIOEstimateLocked(name string) {
	if scheduler.ioState != nil && scheduler.ioState.estimates != nil {
		delete(scheduler.ioState.estimates, name)
	}
}
