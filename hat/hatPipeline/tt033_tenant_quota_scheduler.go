package hatPipeline

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
)

var (
	// ErrTenantQuotaSchedulerNil indicates that a method was called on a nil
	// tenant quota scheduler.
	ErrTenantQuotaSchedulerNil = errors.New("hatPipeline: tenant quota scheduler is nil")
	// ErrTenantQuotaTaskInvalid indicates that a nil task was submitted.
	ErrTenantQuotaTaskInvalid = errors.New("hatPipeline: tenant quota task is invalid")
	// ErrTenantQuotaTenantRequired indicates that a tenant identifier was empty.
	ErrTenantQuotaTenantRequired = errors.New("hatPipeline: tenant quota tenant is required")
	// ErrTenantQuotaPolicyInvalid indicates an unsupported quota policy.
	ErrTenantQuotaPolicyInvalid = errors.New("hatPipeline: tenant quota policy is invalid")
	// ErrTenantQuotaQueueFull indicates that a tenant reached its waiting-task
	// budget while all of its concurrency slots were occupied.
	ErrTenantQuotaQueueFull = errors.New("hatPipeline: tenant quota queue is full")
	// ErrTenantQuotaClosed indicates that the scheduler is no longer accepting
	// submissions.
	ErrTenantQuotaClosed = errors.New("hatPipeline: tenant quota scheduler is closed")
)

const (
	// DefaultTenantQuotaMaxConcurrent is the per-tenant active-task default.
	DefaultTenantQuotaMaxConcurrent = 1
	// DefaultTenantQuotaMaxQueued is the per-tenant waiting-task default. A
	// zero default makes overload fail fast instead of creating hidden work.
	DefaultTenantQuotaMaxQueued = 0
	// MaxTenantQuotaMaxConcurrent bounds per-tenant slot allocation.
	MaxTenantQuotaMaxConcurrent = 4096
	// MaxTenantQuotaMaxQueued bounds waiting-task admission.
	MaxTenantQuotaMaxQueued = 1 << 20
)

// TenantQuotaPolicy bounds one tenant's active and waiting tasks. A zero
// MaxConcurrent selects DefaultTenantQuotaMaxConcurrent. MaxQueued may be zero
// to reject work while all active slots are occupied.
type TenantQuotaPolicy struct {
	MaxConcurrent int
	MaxQueued     int
}

// TenantQuotaSchedulerOptions configures an opt-in tenant quota scheduler.
// PolicyFor is called when a tenant first becomes active; its returned policy
// remains in force until that tenant has no active or waiting tasks.
type TenantQuotaSchedulerOptions struct {
	Context       context.Context
	Workers       int
	QueueCapacity int
	DefaultPolicy TenantQuotaPolicy
	PolicyFor     func(string) TenantQuotaPolicy
}

// TenantQuotaTenantStats is a point-in-time snapshot for one active tenant.
type TenantQuotaTenantStats struct {
	RunningTasks  int
	WaitingTasks  int
	MaxConcurrent int
	MaxQueued     int
}

// TenantQuotaSchedulerStats is a point-in-time aggregate snapshot.
type TenantQuotaSchedulerStats struct {
	ActiveTenants int
	RunningTasks  int
	WaitingTasks  int
	AcceptedTasks uint64
	RejectedTasks uint64
}

type tenantQuotaState struct {
	tenant  string
	policy  TenantQuotaPolicy
	running int
	waiting int
	notify  chan struct{}
}

// TenantQuotaScheduler adds per-tenant admission limits to Scheduler. It is
// deliberately opt-in: NewScheduler and existing callers retain their
// current behavior. A tenant can hold at most MaxConcurrent scheduler slots;
// additional submissions wait for a slot until MaxQueued is reached, after
// which they fail fast with ErrTenantQuotaQueueFull.
type TenantQuotaScheduler struct {
	scheduler     *Scheduler
	defaultPolicy TenantQuotaPolicy
	policyFor     func(string) TenantQuotaPolicy
	closeCh       chan struct{}
	closeOnce     sync.Once

	mu         sync.Mutex
	closed     bool
	submitters sync.WaitGroup
	tenants    map[string]*tenantQuotaState

	accepted atomic.Uint64
	rejected atomic.Uint64
}

// NewTenantQuotaScheduler creates a bounded scheduler with tenant-aware
// admission. Workers and QueueCapacity use the same validation and defaults
// as NewScheduler. The quota layer adds no behavior to NewScheduler itself.
func NewTenantQuotaScheduler(options TenantQuotaSchedulerOptions) (*TenantQuotaScheduler, error) {
	defaultPolicy, err := normalizeTenantQuotaPolicy(options.DefaultPolicy)
	if err != nil {
		return nil, err
	}
	scheduler, err := NewScheduler(options.Context, options.Workers, options.QueueCapacity)
	if err != nil {
		return nil, err
	}
	return &TenantQuotaScheduler{
		scheduler:     scheduler,
		defaultPolicy: defaultPolicy,
		policyFor:     options.PolicyFor,
		closeCh:       make(chan struct{}),
		tenants:       make(map[string]*tenantQuotaState),
	}, nil
}

func normalizeTenantQuotaPolicy(policy TenantQuotaPolicy) (TenantQuotaPolicy, error) {
	if policy.MaxConcurrent == 0 {
		policy.MaxConcurrent = DefaultTenantQuotaMaxConcurrent
	}
	if policy.MaxConcurrent < 1 || policy.MaxConcurrent > MaxTenantQuotaMaxConcurrent || policy.MaxQueued < 0 || policy.MaxQueued > MaxTenantQuotaMaxQueued {
		return TenantQuotaPolicy{}, ErrTenantQuotaPolicyInvalid
	}
	return policy, nil
}

// Submit admits one task for tenant. If all of that tenant's slots are busy,
// Submit waits only while its bounded waiting budget has room. The caller
// context cancels both admission and a blocked wait; once the task is accepted
// by the underlying Scheduler, task context behavior follows Scheduler's
// existing shared scheduler context contract.
func (scheduler *TenantQuotaScheduler) Submit(ctx context.Context, tenant string, task Task) error {
	if scheduler == nil || scheduler.scheduler == nil {
		return ErrTenantQuotaSchedulerNil
	}
	if task == nil {
		scheduler.rejected.Add(1)
		return ErrTenantQuotaTaskInvalid
	}
	tenant = strings.TrimSpace(tenant)
	if tenant == "" {
		scheduler.rejected.Add(1)
		return ErrTenantQuotaTenantRequired
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		scheduler.rejected.Add(1)
		return err
	}
	if err := scheduler.scheduler.ctx.Err(); err != nil {
		scheduler.rejected.Add(1)
		return err
	}
	if err := scheduler.beginSubmit(); err != nil {
		scheduler.rejected.Add(1)
		return err
	}
	defer scheduler.submitters.Done()

	state, err := scheduler.reserve(ctx, tenant)
	if err != nil {
		scheduler.rejected.Add(1)
		return err
	}
	wrapped := func(taskContext context.Context) error {
		defer scheduler.releaseActive(state)
		return task(taskContext)
	}
	if err := scheduler.scheduler.Submit(ctx, wrapped); err != nil {
		scheduler.releaseActive(state)
		scheduler.rejected.Add(1)
		return err
	}
	scheduler.accepted.Add(1)
	return nil
}

func (scheduler *TenantQuotaScheduler) beginSubmit() error {
	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()
	if scheduler.closed {
		return ErrTenantQuotaClosed
	}
	scheduler.submitters.Add(1)
	return nil
}

func (scheduler *TenantQuotaScheduler) reserve(ctx context.Context, tenant string) (*tenantQuotaState, error) {
	scheduler.mu.Lock()
	if scheduler.closed {
		scheduler.mu.Unlock()
		return nil, ErrTenantQuotaClosed
	}
	state := scheduler.tenants[tenant]
	if state == nil {
		scheduler.mu.Unlock()
		policy, err := scheduler.policy(tenant)
		if err != nil {
			return nil, err
		}
		scheduler.mu.Lock()
		if scheduler.closed {
			scheduler.mu.Unlock()
			return nil, ErrTenantQuotaClosed
		}
		state = scheduler.tenants[tenant]
		if state == nil {
			state = &tenantQuotaState{tenant: tenant, policy: policy, notify: make(chan struct{})}
			scheduler.tenants[tenant] = state
		}
	}
	if state.running < state.policy.MaxConcurrent {
		state.running++
		scheduler.mu.Unlock()
		return state, nil
	} else if state.waiting < state.policy.MaxQueued {
		state.waiting++
	} else {
		scheduler.mu.Unlock()
		return nil, ErrTenantQuotaQueueFull
	}
	scheduler.mu.Unlock()

	for {
		scheduler.mu.Lock()
		if scheduler.closed {
			state.waiting--
			scheduler.notifyStateLocked(state)
			scheduler.removeIdleLocked(state)
			scheduler.mu.Unlock()
			return nil, ErrTenantQuotaClosed
		}
		if state.running < state.policy.MaxConcurrent {
			state.waiting--
			state.running++
			scheduler.mu.Unlock()
			return state, nil
		}
		notify := state.notify
		scheduler.mu.Unlock()

		select {
		case <-ctx.Done():
			scheduler.releaseWaiting(state)
			return nil, ctx.Err()
		case <-scheduler.scheduler.ctx.Done():
			scheduler.releaseWaiting(state)
			return nil, scheduler.scheduler.ctx.Err()
		case <-scheduler.closeCh:
			scheduler.releaseWaiting(state)
			return nil, ErrTenantQuotaClosed
		case <-notify:
		}
	}
}

func (scheduler *TenantQuotaScheduler) policy(tenant string) (TenantQuotaPolicy, error) {
	policy := scheduler.defaultPolicy
	if scheduler.policyFor != nil {
		policy = scheduler.policyFor(tenant)
	}
	return normalizeTenantQuotaPolicy(policy)
}

func (scheduler *TenantQuotaScheduler) releaseActive(state *tenantQuotaState) {
	scheduler.mu.Lock()
	state.running--
	scheduler.notifyStateLocked(state)
	scheduler.removeIdleLocked(state)
	scheduler.mu.Unlock()
}

func (scheduler *TenantQuotaScheduler) releaseWaiting(state *tenantQuotaState) {
	scheduler.mu.Lock()
	if state.waiting > 0 {
		state.waiting--
	}
	scheduler.notifyStateLocked(state)
	scheduler.removeIdleLocked(state)
	scheduler.mu.Unlock()
}

func (scheduler *TenantQuotaScheduler) notifyStateLocked(state *tenantQuotaState) {
	close(state.notify)
	state.notify = make(chan struct{})
}

func (scheduler *TenantQuotaScheduler) removeIdleLocked(state *tenantQuotaState) {
	if state.running == 0 && state.waiting == 0 && scheduler.tenants[state.tenant] == state {
		delete(scheduler.tenants, state.tenant)
	}
}

// Close rejects new submissions and lets already accepted tasks drain.
func (scheduler *TenantQuotaScheduler) Close() {
	if scheduler == nil || scheduler.scheduler == nil {
		return
	}
	scheduler.closeOnce.Do(func() {
		scheduler.mu.Lock()
		scheduler.closed = true
		close(scheduler.closeCh)
		scheduler.mu.Unlock()
		scheduler.scheduler.Close()
	})
}

// Cancel stops queued and running scheduler work. Call Wait to join workers
// and release quota leases for tasks discarded from the underlying queue.
func (scheduler *TenantQuotaScheduler) Cancel() {
	if scheduler != nil && scheduler.scheduler != nil {
		scheduler.scheduler.Cancel()
	}
}

// Wait closes the scheduler, joins workers, and returns the first task or
// scheduler-context error. Any queued task discarded by cancellation is
// released before Wait returns.
func (scheduler *TenantQuotaScheduler) Wait() error {
	if scheduler == nil || scheduler.scheduler == nil {
		return ErrTenantQuotaSchedulerNil
	}
	scheduler.Close()
	scheduler.submitters.Wait()
	err := scheduler.scheduler.Wait()
	scheduler.mu.Lock()
	scheduler.tenants = make(map[string]*tenantQuotaState)
	scheduler.mu.Unlock()
	return err
}

// Stats returns aggregate active and waiting work. It does not include tasks
// that have already released their quota lease.
func (scheduler *TenantQuotaScheduler) Stats() TenantQuotaSchedulerStats {
	if scheduler == nil {
		return TenantQuotaSchedulerStats{}
	}
	scheduler.mu.Lock()
	stats := TenantQuotaSchedulerStats{ActiveTenants: len(scheduler.tenants)}
	for _, state := range scheduler.tenants {
		stats.RunningTasks += state.running
		stats.WaitingTasks += state.waiting
	}
	scheduler.mu.Unlock()
	stats.AcceptedTasks = scheduler.accepted.Load()
	stats.RejectedTasks = scheduler.rejected.Load()
	return stats
}

// TenantStats returns the active snapshot for tenant. The second result is
// false after the tenant has no active or waiting work.
func (scheduler *TenantQuotaScheduler) TenantStats(tenant string) (TenantQuotaTenantStats, bool) {
	if scheduler == nil {
		return TenantQuotaTenantStats{}, false
	}
	tenant = strings.TrimSpace(tenant)
	scheduler.mu.Lock()
	state, ok := scheduler.tenants[tenant]
	if !ok {
		scheduler.mu.Unlock()
		return TenantQuotaTenantStats{}, false
	}
	stats := TenantQuotaTenantStats{
		RunningTasks:  state.running,
		WaitingTasks:  state.waiting,
		MaxConcurrent: state.policy.MaxConcurrent,
		MaxQueued:     state.policy.MaxQueued,
	}
	scheduler.mu.Unlock()
	return stats, true
}
