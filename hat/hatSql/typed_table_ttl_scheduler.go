package hatSql

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	DefaultTypedTableTTLSchedulerPollInterval = time.Second
	DefaultTypedTableTTLSchedulerMaxTables    = 64
	MaxTypedTableTTLSchedulerMaxTables        = 100_000
)

var (
	ErrTypedTableTTLSchedulerNil      = errors.New("typed table TTL scheduler is nil")
	ErrTypedTableTTLSchedulerClosed   = errors.New("typed table TTL scheduler is closed")
	ErrTypedTableTTLSchedulerStarted  = errors.New("typed table TTL scheduler has already started")
	ErrTypedTableTTLSchedulerExists   = errors.New("typed table TTL scheduler entry already exists")
	ErrTypedTableTTLSchedulerNotFound = errors.New("typed table TTL scheduler entry was not found")
	ErrTypedTableTTLSchedulerInvalid  = errors.New("typed table TTL scheduler options are invalid")
)

// TypedTableTTLSchedulerOptions controls one shared maintenance loop for
// registered tables. The zero value is safe and does not start a goroutine.
type TypedTableTTLSchedulerOptions struct {
	PollInterval      time.Duration
	MaxTablesPerCycle int
	Now               func() time.Time
	OnRun             func(TypedTableTTLRun)
}

// TypedTableTTLRun records one table maintenance pass. Expired is the number
// of rows converted into DELETE changes during the pass.
type TypedTableTTLRun struct {
	Name           string
	StartedAt      time.Time
	FinishedAt     time.Time
	Expired        int
	ExpiredColumns int
	Error          string
}

// TypedTableTTLScheduler is an explicit, one-goroutine reaper for registered
// TypedTable values. It never starts automatically and never creates one
// goroutine per table.
type TypedTableTTLScheduler struct {
	mu      sync.RWMutex
	options TypedTableTTLSchedulerOptions
	tables  map[string]*TypedTable
	status  map[string]TypedTableTTLRun

	wake    chan struct{}
	ctx     context.Context
	cancel  context.CancelFunc
	started bool
	closed  bool
	worker  sync.WaitGroup
}

func NewTypedTableTTLScheduler(options TypedTableTTLSchedulerOptions) (*TypedTableTTLScheduler, error) {
	if options.PollInterval < 0 || options.MaxTablesPerCycle < 0 || options.MaxTablesPerCycle > MaxTypedTableTTLSchedulerMaxTables {
		return nil, ErrTypedTableTTLSchedulerInvalid
	}
	if options.PollInterval == 0 {
		options.PollInterval = DefaultTypedTableTTLSchedulerPollInterval
	}
	if options.MaxTablesPerCycle == 0 {
		options.MaxTablesPerCycle = DefaultTypedTableTTLSchedulerMaxTables
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	return &TypedTableTTLScheduler{
		options: options,
		tables:  make(map[string]*TypedTable),
		status:  make(map[string]TypedTableTTLRun),
		wake:    make(chan struct{}, 1),
	}, nil
}

// Register adds a table to the shared TTL pass. Registration does not start
// maintenance and is safe before or after Start.
func (scheduler *TypedTableTTLScheduler) Register(name string, table *TypedTable) error {
	if scheduler == nil {
		return ErrTypedTableTTLSchedulerNil
	}
	name = strings.TrimSpace(name)
	if name == "" || table == nil {
		return ErrTypedTableTTLSchedulerInvalid
	}
	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()
	if scheduler.closed {
		return ErrTypedTableTTLSchedulerClosed
	}
	if _, exists := scheduler.tables[name]; exists {
		return ErrTypedTableTTLSchedulerExists
	}
	scheduler.tables[name] = table
	scheduler.status[name] = TypedTableTTLRun{Name: name}
	scheduler.signalWakeLocked()
	return nil
}

// Unregister removes a table and its last-run status.
func (scheduler *TypedTableTTLScheduler) Unregister(name string) error {
	if scheduler == nil {
		return ErrTypedTableTTLSchedulerNil
	}
	name = strings.TrimSpace(name)
	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()
	if _, exists := scheduler.tables[name]; !exists {
		return ErrTypedTableTTLSchedulerNotFound
	}
	delete(scheduler.tables, name)
	delete(scheduler.status, name)
	scheduler.signalWakeLocked()
	return nil
}

// Start begins the one background polling goroutine. A caller can use RunOnce
// without Start for deterministic maintenance or tests.
func (scheduler *TypedTableTTLScheduler) Start(ctx context.Context) error {
	if scheduler == nil {
		return ErrTypedTableTTLSchedulerNil
	}
	if ctx == nil {
		return ErrTypedTableTTLSchedulerInvalid
	}
	scheduler.mu.Lock()
	if scheduler.closed {
		scheduler.mu.Unlock()
		return ErrTypedTableTTLSchedulerClosed
	}
	if scheduler.started {
		scheduler.mu.Unlock()
		return ErrTypedTableTTLSchedulerStarted
	}
	scheduler.ctx, scheduler.cancel = context.WithCancel(ctx)
	scheduler.started = true
	scheduler.worker.Add(1)
	scheduler.mu.Unlock()
	go scheduler.runLoop()
	return nil
}

// RunOnce purges all currently registered tables, up to the configured table
// bound, using one coherent scheduler-clock timestamp. Expired rows are
// deleted, while expired column values are masked and physically cleared.
func (scheduler *TypedTableTTLScheduler) RunOnce(ctx context.Context) ([]TypedTableTTLRun, error) {
	if scheduler == nil {
		return nil, ErrTypedTableTTLSchedulerNil
	}
	if ctx == nil {
		return nil, ErrTypedTableTTLSchedulerInvalid
	}
	scheduler.mu.RLock()
	if scheduler.closed {
		scheduler.mu.RUnlock()
		return nil, ErrTypedTableTTLSchedulerClosed
	}
	names := make([]string, 0, len(scheduler.tables))
	for name := range scheduler.tables {
		names = append(names, name)
	}
	sort.Strings(names)
	if len(names) > scheduler.options.MaxTablesPerCycle {
		names = names[:scheduler.options.MaxTablesPerCycle]
	}
	tables := make([]*TypedTable, len(names))
	for index, name := range names {
		tables[index] = scheduler.tables[name]
	}
	now := scheduler.options.Now()
	scheduler.mu.RUnlock()

	runs := make([]TypedTableTTLRun, 0, len(names))
	for index, name := range names {
		if err := ctx.Err(); err != nil {
			return runs, err
		}
		run := TypedTableTTLRun{Name: name, StartedAt: now}
		changes, err := tables[index].PurgeExpired(now)
		run.Expired = len(changes)
		if err != nil {
			run.Error = err.Error()
		}
		if err == nil {
			columnChanges, columnErr := tables[index].PurgeExpiredColumns(now)
			run.ExpiredColumns = len(columnChanges)
			if columnErr != nil {
				err = columnErr
				run.Error = columnErr.Error()
			}
		}
		run.FinishedAt = scheduler.options.Now()
		scheduler.recordRun(run, tables[index])
		runs = append(runs, run)
		scheduler.notify(run)
		if err != nil {
			return runs, err
		}
	}
	return runs, nil
}

// Status returns the latest pass for a registered table.
func (scheduler *TypedTableTTLScheduler) Status(name string) (TypedTableTTLRun, bool) {
	if scheduler == nil {
		return TypedTableTTLRun{}, false
	}
	name = strings.TrimSpace(name)
	scheduler.mu.RLock()
	defer scheduler.mu.RUnlock()
	status, ok := scheduler.status[name]
	return status, ok
}

// Snapshot returns registered table statuses in deterministic name order.
func (scheduler *TypedTableTTLScheduler) Snapshot() []TypedTableTTLRun {
	if scheduler == nil {
		return nil
	}
	scheduler.mu.RLock()
	names := make([]string, 0, len(scheduler.tables))
	for name := range scheduler.tables {
		names = append(names, name)
	}
	sort.Strings(names)
	statuses := make([]TypedTableTTLRun, 0, len(names))
	for _, name := range names {
		statuses = append(statuses, scheduler.status[name])
	}
	scheduler.mu.RUnlock()
	return statuses
}

// Close stops maintenance and is idempotent. It waits for a current purge
// pass to return before completing.
func (scheduler *TypedTableTTLScheduler) Close() error {
	if scheduler == nil {
		return ErrTypedTableTTLSchedulerNil
	}
	scheduler.mu.Lock()
	if !scheduler.closed {
		scheduler.closed = true
		if scheduler.cancel != nil {
			scheduler.cancel()
		}
		scheduler.signalWakeLocked()
	}
	scheduler.mu.Unlock()
	scheduler.worker.Wait()
	return nil
}

func (scheduler *TypedTableTTLScheduler) runLoop() {
	defer scheduler.worker.Done()
	timer := time.NewTimer(scheduler.options.PollInterval)
	defer timer.Stop()
	for {
		scheduler.mu.RLock()
		ctx := scheduler.ctx
		scheduler.mu.RUnlock()
		select {
		case <-ctx.Done():
			return
		case <-scheduler.wake:
		case <-timer.C:
		}
		if _, err := scheduler.RunOnce(ctx); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, ErrTypedTableTTLSchedulerClosed) {
				return
			}
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(scheduler.options.PollInterval)
	}
}

func (scheduler *TypedTableTTLScheduler) recordRun(run TypedTableTTLRun, table *TypedTable) {
	scheduler.mu.Lock()
	if current, ok := scheduler.tables[run.Name]; ok && current == table {
		scheduler.status[run.Name] = run
	}
	scheduler.mu.Unlock()
}

func (scheduler *TypedTableTTLScheduler) notify(run TypedTableTTLRun) {
	if scheduler.options.OnRun == nil {
		return
	}
	defer func() {
		_ = recover()
	}()
	scheduler.options.OnRun(run)
}

func (scheduler *TypedTableTTLScheduler) signalWakeLocked() {
	select {
	case scheduler.wake <- struct{}{}:
	default:
	}
}
