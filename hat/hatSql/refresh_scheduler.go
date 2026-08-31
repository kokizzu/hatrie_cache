package hatSql

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// ManagedRefreshSchedulerOptions controls time for refresh scheduling. Now is
// injectable so callers can deterministically drive maintenance loops.
type ManagedRefreshSchedulerOptions struct {
	Now func() time.Time
	// MaxRunsPerCycle caps due tasks run by one RunDue call. Zero keeps the
	// existing unlimited deterministic behavior.
	MaxRunsPerCycle int
	// MaxCycleDuration bounds cooperative refresh work in one RunDue call.
	// Zero keeps the existing unlimited behavior; callbacks receive a context
	// deadline but remain responsible for honoring cancellation.
	MaxCycleDuration time.Duration
}

// ManagedRefreshRun records one view or rollup refresh attempt.
type ManagedRefreshRun struct {
	Name       string
	StartedAt  time.Time
	FinishedAt time.Time
	Error      string
}

// ManagedRefreshScheduler runs named materialized-view and rollup refreshes
// at fixed intervals. One task never overlaps with itself.
type ManagedRefreshScheduler struct {
	mu               sync.RWMutex
	now              func() time.Time
	maxRunsPerCycle  int
	maxCycleDuration time.Duration
	tasks            map[string]managedRefreshTask
}

type managedRefreshTask struct {
	every   time.Duration
	next    time.Time
	running bool
	run     func(context.Context) error
}

func NewManagedRefreshScheduler(options ManagedRefreshSchedulerOptions) (*ManagedRefreshScheduler, error) {
	if options.MaxRunsPerCycle < 0 {
		return nil, fmt.Errorf("managed refresh max runs per cycle must not be negative")
	}
	if options.MaxCycleDuration < 0 {
		return nil, fmt.Errorf("managed refresh max cycle duration must not be negative")
	}
	if options.Now == nil {
		options.Now = func() time.Time { return time.Now().UTC() }
	}
	return &ManagedRefreshScheduler{
		now:              options.Now,
		maxRunsPerCycle:  options.MaxRunsPerCycle,
		maxCycleDuration: options.MaxCycleDuration,
		tasks:            make(map[string]managedRefreshTask),
	}, nil
}

// AddMaterializedView registers a view refresh using the view's explicit
// dependencies. It does not publish partial data because RefreshChanged keeps
// the existing snapshot on an execution error.
func (scheduler *ManagedRefreshScheduler) AddMaterializedView(name string, views *MaterializedViews, viewName string, resolver SourceResolver, options QueryOptions, every time.Duration) error {
	if views == nil {
		return fmt.Errorf("materialized views are nil")
	}
	viewName = strings.TrimSpace(viewName)
	if viewName == "" {
		return fmt.Errorf("materialized view name is required")
	}
	return scheduler.add(name, every, func(ctx context.Context) error {
		view, exists := views.Get(viewName)
		if !exists {
			return fmt.Errorf("materialized view %q does not exist", viewName)
		}
		_, err := views.RefreshChanged(ctx, view.Status.Dependencies, resolver, options)
		return err
	})
}

// AddRollup registers one caller-supplied rollup refresh. The callback is
// expected to perform an atomic publish or retain its prior result on error.
func (scheduler *ManagedRefreshScheduler) AddRollup(name string, every time.Duration, refresh func(context.Context) error) error {
	if refresh == nil {
		return fmt.Errorf("rollup refresh is required")
	}
	return scheduler.add(name, every, refresh)
}

func (scheduler *ManagedRefreshScheduler) add(name string, every time.Duration, run func(context.Context) error) error {
	if scheduler == nil {
		return fmt.Errorf("managed refresh scheduler is nil")
	}
	name = strings.TrimSpace(name)
	if name == "" || every <= 0 {
		return fmt.Errorf("refresh name and positive interval are required")
	}
	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()
	if _, exists := scheduler.tasks[name]; exists {
		return fmt.Errorf("refresh %q already exists", name)
	}
	scheduler.tasks[name] = managedRefreshTask{every: every, next: scheduler.now(), run: run}
	return nil
}

// RunDue executes due refreshes in deterministic name order.
func (scheduler *ManagedRefreshScheduler) RunDue(ctx context.Context) ([]ManagedRefreshRun, error) {
	if scheduler == nil {
		return nil, fmt.Errorf("managed refresh scheduler is nil")
	}
	now := scheduler.now()
	scheduler.mu.RLock()
	names := make([]string, 0, len(scheduler.tasks))
	for name, task := range scheduler.tasks {
		if !task.running && !task.next.After(now) {
			names = append(names, name)
		}
	}
	scheduler.mu.RUnlock()
	sort.Strings(names)
	runs := make([]ManagedRefreshRun, 0, len(names))
	deadline := now.Add(scheduler.maxCycleDuration)
	for _, name := range names {
		if scheduler.maxRunsPerCycle > 0 && len(runs) >= scheduler.maxRunsPerCycle {
			break
		}
		runContext := ctx
		cancel := func() {}
		if scheduler.maxCycleDuration > 0 {
			remaining := deadline.Sub(scheduler.now())
			if remaining <= 0 {
				break
			}
			runContext, cancel = context.WithTimeout(ctx, remaining)
		}
		run, err := scheduler.run(runContext, name, now)
		cancel()
		if run.Name != "" {
			runs = append(runs, run)
		}
		if err != nil {
			return runs, err
		}
	}
	return runs, nil
}

func (scheduler *ManagedRefreshScheduler) run(ctx context.Context, name string, now time.Time) (ManagedRefreshRun, error) {
	scheduler.mu.Lock()
	task, exists := scheduler.tasks[name]
	if !exists || task.running || task.next.After(now) {
		scheduler.mu.Unlock()
		return ManagedRefreshRun{}, nil
	}
	task.running = true
	task.next = nextJobRun(task.next, task.every, now)
	scheduler.tasks[name] = task
	scheduler.mu.Unlock()

	run := ManagedRefreshRun{Name: name, StartedAt: now}
	err := task.run(ctx)
	run.FinishedAt = scheduler.now()
	if err != nil {
		run.Error = err.Error()
	}
	scheduler.mu.Lock()
	task = scheduler.tasks[name]
	task.running = false
	scheduler.tasks[name] = task
	scheduler.mu.Unlock()
	return run, err
}
