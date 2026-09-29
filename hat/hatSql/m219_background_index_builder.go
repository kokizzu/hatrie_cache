package hatSql

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
)

var (
	ErrSQLBackgroundIndexBuilderNil           = errors.New("background index builder is nil")
	ErrSQLBackgroundIndexBuilderNameRequired  = errors.New("background index builder name is required")
	ErrSQLBackgroundIndexBuilderApplyRequired = errors.New("background index builder apply callback is required")
	ErrSQLBackgroundIndexBuilderContextNil    = errors.New("background index builder context is nil")
	ErrSQLBackgroundIndexBuilderStarted       = errors.New("background index builder already started")
	ErrSQLBackgroundIndexBuilderNotStarted    = errors.New("background index builder has not started")
)

// SQLBackgroundIndexBuildState describes the lifecycle of one background
// index build. A builder becomes ready only after all batches have applied and
// its optional publish callback has succeeded.
type SQLBackgroundIndexBuildState string

const (
	SQLBackgroundIndexBuildQueued   SQLBackgroundIndexBuildState = "queued"
	SQLBackgroundIndexBuildRunning  SQLBackgroundIndexBuildState = "running"
	SQLBackgroundIndexBuildReady    SQLBackgroundIndexBuildState = "ready"
	SQLBackgroundIndexBuildFailed   SQLBackgroundIndexBuildState = "failed"
	SQLBackgroundIndexBuildCanceled SQLBackgroundIndexBuildState = "canceled"
)

// SQLBackgroundIndexBuildBatch is one bounded unit of index construction.
// Frontier is the source logical time covered after Updates are applied and
// must never move backward within one builder.
type SQLBackgroundIndexBuildBatch struct {
	Frontier uint64
	Updates  []DifferentialRow
}

// SQLBackgroundIndexBuildDefinition supplies staged work and its callbacks.
// Apply should mutate private staging state. Publish is called once, after all
// batches succeed, to atomically make that state visible; it may be nil when
// the caller does not need a separate publication step.
type SQLBackgroundIndexBuildDefinition struct {
	Name    string
	Batches []SQLBackgroundIndexBuildBatch
	Apply   func([]DifferentialRow) error
	Publish func() error
}

// SQLBackgroundIndexBuildOptions controls the initial source frontier. A zero
// target frontier derives from the last batch, or equals InitialFrontier when
// the build has no batches. CloneInputs is disabled by default so callers can
// build directly from an immutable source snapshot without doubling row-map
// memory; enable it when the source may be mutated before the build finishes.
type SQLBackgroundIndexBuildOptions struct {
	InitialFrontier uint64
	TargetFrontier  uint64
	CloneInputs     bool
}

// SQLBackgroundIndexBuildStatus is a race-free snapshot of build progress.
// ProcessedRows and AppliedBatches describe committed staging work; the
// BuildFrontier advances only after the corresponding Apply callback returns.
type SQLBackgroundIndexBuildStatus struct {
	Name           string
	State          SQLBackgroundIndexBuildState
	BuildFrontier  uint64
	TargetFrontier uint64
	ProcessedRows  int
	TotalRows      int
	AppliedBatches int
	LastError      string
}

// SQLBackgroundIndexBuilder applies bounded batches on one background
// goroutine and exposes a monotone build frontier. It is deliberately
// arrangement-agnostic so callers can stage a point lookup, ordered index, or
// another importable SQL data structure behind the same lifecycle.
type SQLBackgroundIndexBuilder struct {
	mu        sync.RWMutex
	name      string
	batches   []SQLBackgroundIndexBuildBatch
	apply     func([]DifferentialRow) error
	publish   func() error
	status    SQLBackgroundIndexBuildStatus
	started   bool
	done      chan struct{}
	resultErr error
}

// NewSQLBackgroundIndexBuilder validates and copies a background build
// definition. Batch slices are copied; row maps are cloned only when
// options.CloneInputs is enabled.
func NewSQLBackgroundIndexBuilder(definition SQLBackgroundIndexBuildDefinition, options SQLBackgroundIndexBuildOptions) (*SQLBackgroundIndexBuilder, error) {
	name := strings.TrimSpace(definition.Name)
	if name == "" {
		return nil, ErrSQLBackgroundIndexBuilderNameRequired
	}
	if definition.Apply == nil {
		return nil, ErrSQLBackgroundIndexBuilderApplyRequired
	}
	if options.TargetFrontier != 0 && options.TargetFrontier < options.InitialFrontier {
		return nil, fmt.Errorf("background index target frontier %d precedes initial frontier %d", options.TargetFrontier, options.InitialFrontier)
	}
	batches := make([]SQLBackgroundIndexBuildBatch, len(definition.Batches))
	previousFrontier := options.InitialFrontier
	totalRows := 0
	for index, batch := range definition.Batches {
		if batch.Frontier < previousFrontier {
			return nil, fmt.Errorf("background index batch %d frontier %d regresses from %d", index, batch.Frontier, previousFrontier)
		}
		batches[index] = copySQLBackgroundIndexBuildBatch(batch, options.CloneInputs)
		previousFrontier = batch.Frontier
		totalRows += len(batch.Updates)
	}
	targetFrontier := options.TargetFrontier
	if targetFrontier == 0 {
		targetFrontier = previousFrontier
	}
	if targetFrontier < previousFrontier {
		return nil, fmt.Errorf("background index target frontier %d precedes final batch frontier %d", targetFrontier, previousFrontier)
	}
	if len(batches) > 0 && options.TargetFrontier != 0 && targetFrontier != previousFrontier {
		return nil, fmt.Errorf("background index target frontier %d does not match final batch frontier %d", targetFrontier, previousFrontier)
	}
	return &SQLBackgroundIndexBuilder{
		name:    name,
		batches: batches,
		apply:   definition.Apply,
		publish: definition.Publish,
		status: SQLBackgroundIndexBuildStatus{
			Name:           name,
			State:          SQLBackgroundIndexBuildQueued,
			BuildFrontier:  options.InitialFrontier,
			TargetFrontier: targetFrontier,
			TotalRows:      totalRows,
		},
	}, nil
}

// Status returns an immutable progress snapshot. It is safe to call while the
// background worker is applying a batch.
func (builder *SQLBackgroundIndexBuilder) Status() SQLBackgroundIndexBuildStatus {
	if builder == nil {
		return SQLBackgroundIndexBuildStatus{}
	}
	builder.mu.RLock()
	status := builder.status
	builder.mu.RUnlock()
	return status
}

// Start launches the build worker and returns immediately. Start is accepted
// exactly once; callers should use Wait to observe the terminal result.
func (builder *SQLBackgroundIndexBuilder) Start(ctx context.Context) error {
	if builder == nil {
		return ErrSQLBackgroundIndexBuilderNil
	}
	if ctx == nil {
		return ErrSQLBackgroundIndexBuilderContextNil
	}
	builder.mu.Lock()
	defer builder.mu.Unlock()
	if builder.started {
		return ErrSQLBackgroundIndexBuilderStarted
	}
	builder.started = true
	builder.done = make(chan struct{})
	builder.status.State = SQLBackgroundIndexBuildRunning
	go builder.run(ctx)
	return nil
}

// Wait blocks until the build reaches a terminal state or waitCtx is
// canceled. Canceling waitCtx does not cancel the build; cancel the context
// passed to Start for that purpose.
func (builder *SQLBackgroundIndexBuilder) Wait(waitCtx context.Context) error {
	if builder == nil {
		return ErrSQLBackgroundIndexBuilderNil
	}
	if waitCtx == nil {
		return ErrSQLBackgroundIndexBuilderContextNil
	}
	builder.mu.RLock()
	started := builder.started
	done := builder.done
	builder.mu.RUnlock()
	if !started {
		return ErrSQLBackgroundIndexBuilderNotStarted
	}
	select {
	case <-done:
		builder.mu.RLock()
		err := builder.resultErr
		builder.mu.RUnlock()
		return err
	case <-waitCtx.Done():
		return waitCtx.Err()
	}
}

func (builder *SQLBackgroundIndexBuilder) run(ctx context.Context) {
	for index, batch := range builder.batches {
		if err := ctx.Err(); err != nil {
			builder.finish(SQLBackgroundIndexBuildCanceled, err)
			return
		}
		if err := builder.apply(batch.Updates); err != nil {
			builder.finish(SQLBackgroundIndexBuildFailed, fmt.Errorf("apply background index batch %d: %w", index, err))
			return
		}
		builder.mu.Lock()
		builder.batches[index].Updates = nil
		builder.status.BuildFrontier = batch.Frontier
		builder.status.ProcessedRows += len(batch.Updates)
		builder.status.AppliedBatches++
		builder.mu.Unlock()
	}
	if err := ctx.Err(); err != nil {
		builder.finish(SQLBackgroundIndexBuildCanceled, err)
		return
	}
	if len(builder.batches) == 0 {
		builder.mu.Lock()
		builder.status.BuildFrontier = builder.status.TargetFrontier
		builder.mu.Unlock()
	}
	if builder.publish != nil {
		if err := builder.publish(); err != nil {
			builder.finish(SQLBackgroundIndexBuildFailed, fmt.Errorf("publish background index: %w", err))
			return
		}
	}
	builder.finish(SQLBackgroundIndexBuildReady, nil)
}

func (builder *SQLBackgroundIndexBuilder) finish(state SQLBackgroundIndexBuildState, err error) {
	builder.mu.Lock()
	builder.status.State = state
	if err != nil {
		builder.status.LastError = err.Error()
	}
	builder.resultErr = err
	close(builder.done)
	builder.mu.Unlock()
}

func copySQLBackgroundIndexBuildBatch(batch SQLBackgroundIndexBuildBatch, cloneInputs bool) SQLBackgroundIndexBuildBatch {
	updates := append([]DifferentialRow(nil), batch.Updates...)
	if cloneInputs {
		updates = cloneSQLBackgroundIndexUpdates(updates)
	}
	return SQLBackgroundIndexBuildBatch{
		Frontier: batch.Frontier,
		Updates:  updates,
	}
}

func cloneSQLBackgroundIndexUpdates(updates []DifferentialRow) []DifferentialRow {
	if len(updates) == 0 {
		return nil
	}
	cloned := make([]DifferentialRow, len(updates))
	for index, update := range updates {
		cloned[index] = cloneSQLBackgroundIndexUpdate(update)
	}
	return cloned
}

func cloneSQLBackgroundIndexUpdate(update DifferentialRow) DifferentialRow {
	update.Row = cloneDifferentialRow(update.Row)
	return update
}
