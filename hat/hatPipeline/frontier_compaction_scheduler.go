package hatPipeline

import (
	"context"
	"sync/atomic"
)

// FrontierCompactionScheduler admits compaction tasks only after the named
// frontier and all active retention leases allow the requested boundary. A
// blocked Submit waits without occupying a worker in the underlying Scheduler.
type FrontierCompactionScheduler struct {
	retention *FrontierRetentionRegistry
	scheduler *Scheduler
	ctx       context.Context
	cancel    context.CancelFunc
	policies  atomic.Pointer[frontierCompactionPolicyRegistry]
}

// NewFrontierCompactionScheduler creates a bounded scheduler coupled to a
// frontier retention registry. workers and queueCapacity have the same
// meaning as NewScheduler.
func NewFrontierCompactionScheduler(parent context.Context, retention *FrontierRetentionRegistry, workers, queueCapacity int) (*FrontierCompactionScheduler, error) {
	if retention == nil {
		return nil, ErrFrontierRetentionRegistryNil
	}
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	scheduler, err := NewScheduler(ctx, workers, queueCapacity)
	if err != nil {
		cancel()
		return nil, err
	}
	return &FrontierCompactionScheduler{
		retention: retention,
		scheduler: scheduler,
		ctx:       ctx,
		cancel:    cancel,
	}, nil
}

// SetPolicy installs or replaces the per-frontier outstanding-task policy.
// MaxOutstanding counts both tasks waiting for frontier safety and tasks that
// have been handed to the underlying scheduler. The policy is opt-in and does
// not affect other frontiers.
func (scheduler *FrontierCompactionScheduler) SetPolicy(frontierID string, policy FrontierCompactionPolicy) error {
	if scheduler == nil || scheduler.retention == nil || scheduler.scheduler == nil {
		return ErrSchedulerInvalid
	}
	if frontierID == "" || policy.MaxOutstanding < 1 || policy.MaxOutstanding > maxFrontierCompactionOutstanding {
		return ErrFrontierCompactionPolicyInvalid
	}
	registry := scheduler.policies.Load()
	if registry == nil {
		candidate := newFrontierCompactionPolicyRegistry()
		if scheduler.policies.CompareAndSwap(nil, candidate) {
			registry = candidate
		} else {
			registry = scheduler.policies.Load()
		}
	}
	return registry.set(frontierID, policy)
}

// ClearPolicy removes the policy for one frontier. Tasks already admitted
// under the old policy retain their slots until they finish.
func (scheduler *FrontierCompactionScheduler) ClearPolicy(frontierID string) {
	if scheduler == nil {
		return
	}
	if registry := scheduler.policies.Load(); registry != nil {
		registry.clear(frontierID)
	}
}

// Submit waits until history strictly before boundary is safe to remove, then
// queues task. The wait is cancellable by ctx or Cancel. The task should use
// the same boundary it submitted so the admission check remains meaningful.
func (scheduler *FrontierCompactionScheduler) Submit(ctx context.Context, frontierID string, boundary uint64, task Task) error {
	if scheduler == nil || scheduler.retention == nil || scheduler.scheduler == nil || task == nil {
		return ErrSchedulerInvalid
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := scheduler.ctx.Err(); err != nil {
		return err
	}
	var releasePolicy func()
	policyActive := false
	if policies := scheduler.policies.Load(); policies != nil {
		var err error
		releasePolicy, policyActive, err = policies.acquire(ctx, scheduler.ctx.Done(), frontierID)
		if err != nil {
			return err
		}
	}
	waitCtx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(scheduler.ctx, cancel)
	err := scheduler.retention.WaitUntilSafe(waitCtx, frontierID, boundary)
	stop()
	cancel()
	if err != nil {
		if policyActive {
			releasePolicy()
		}
		return err
	}
	if !policyActive {
		return scheduler.scheduler.Submit(ctx, task)
	}
	submitted := false
	defer func() {
		if !submitted {
			releasePolicy()
		}
	}()
	err = scheduler.scheduler.Submit(ctx, func(taskCtx context.Context) error {
		defer releasePolicy()
		return task(taskCtx)
	})
	if err != nil {
		return err
	}
	submitted = true
	return nil
}

// Cancel cancels queued and running scheduler work. A blocked Submit also
// returns once it observes the cancellation.
func (scheduler *FrontierCompactionScheduler) Cancel() {
	if scheduler == nil {
		return
	}
	if scheduler.cancel != nil {
		scheduler.cancel()
	}
	if scheduler.scheduler != nil {
		scheduler.scheduler.Cancel()
	}
}

// Close stops new submissions and lets already queued tasks drain.
func (scheduler *FrontierCompactionScheduler) Close() {
	if scheduler != nil && scheduler.scheduler != nil {
		scheduler.scheduler.Close()
	}
}

// Wait closes the scheduler, waits for workers, and returns the first task
// error.
func (scheduler *FrontierCompactionScheduler) Wait() error {
	if scheduler == nil || scheduler.scheduler == nil {
		return ErrSchedulerInvalid
	}
	return scheduler.scheduler.Wait()
}
