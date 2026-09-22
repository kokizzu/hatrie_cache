package hatSql

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

var (
	ErrMaterializedViewPointLookupBuildInProgress = errors.New("materialized view point lookup index build is in progress")
	ErrMaterializedViewPointLookupBuildStale      = errors.New("materialized view point lookup index build snapshot is stale")
	ErrMaterializedViewPointLookupBuildCanceled   = errors.New("materialized view point lookup index build canceled")
)

const materializedViewPointLookupBuildProgressInterval = 64

// MaterializedViewPointLookupBuildState describes the lifecycle of an
// asynchronous point lookup index build.
type MaterializedViewPointLookupBuildState string

const (
	MaterializedViewPointLookupBuildStateBuilding MaterializedViewPointLookupBuildState = "building"
	MaterializedViewPointLookupBuildStateReady    MaterializedViewPointLookupBuildState = "ready"
	MaterializedViewPointLookupBuildStateFailed   MaterializedViewPointLookupBuildState = "failed"
	MaterializedViewPointLookupBuildStateCanceled MaterializedViewPointLookupBuildState = "canceled"
)

// MaterializedViewPointLookupBuildStatus is a consistent progress snapshot.
// Frontier is the number of source rows fully incorporated into the private
// index. The public index is published only after Frontier reaches TotalRows.
type MaterializedViewPointLookupBuildStatus struct {
	IndexName    string
	ViewName     string
	ViewRevision uint64
	State        MaterializedViewPointLookupBuildState
	Frontier     uint64
	TotalRows    uint64
	StartedAt    time.Time
	FinishedAt   time.Time
	Err          string
}

// MaterializedViewPointLookupBuild is a handle for one asynchronous point
// lookup index build. Its methods are safe for concurrent use.
type MaterializedViewPointLookupBuild struct {
	state *materializedViewPointLookupBuildState
}

type materializedViewPointLookupBuildState struct {
	mu     sync.RWMutex
	status MaterializedViewPointLookupBuildStatus
	err    error
	done   chan struct{}
	cancel context.CancelFunc

	indexName string
	viewName  string
}

// Status returns the latest consistent build progress snapshot.
func (build *MaterializedViewPointLookupBuild) Status() MaterializedViewPointLookupBuildStatus {
	if build == nil || build.state == nil {
		return MaterializedViewPointLookupBuildStatus{}
	}
	status, _ := build.state.snapshot()
	return status
}

// Wait waits for the build to reach a terminal state. A caller context
// timeout only interrupts the wait; it does not cancel the build.
func (build *MaterializedViewPointLookupBuild) Wait(ctx context.Context) (MaterializedViewPointLookupBuildStatus, error) {
	if build == nil || build.state == nil {
		return MaterializedViewPointLookupBuildStatus{}, fmt.Errorf("materialized view point lookup build is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-build.state.done:
		status, err := build.state.snapshot()
		return status, err
	case <-ctx.Done():
		status, _ := build.state.snapshot()
		return status, ctx.Err()
	}
}

// Cancel requests cooperative cancellation. A key function that blocks must
// return before the build can observe this request.
func (build *MaterializedViewPointLookupBuild) Cancel() {
	if build == nil || build.state == nil || build.state.cancel == nil {
		return
	}
	build.state.cancel()
}

func (state *materializedViewPointLookupBuildState) snapshot() (MaterializedViewPointLookupBuildStatus, error) {
	state.mu.RLock()
	status, err := state.status, state.err
	state.mu.RUnlock()
	return status, err
}

func (state *materializedViewPointLookupBuildState) setFrontier(frontier uint64) {
	state.mu.Lock()
	if state.status.State == MaterializedViewPointLookupBuildStateBuilding && frontier > state.status.Frontier {
		state.status.Frontier = frontier
	}
	state.mu.Unlock()
}

func (state *materializedViewPointLookupBuildState) finish(next MaterializedViewPointLookupBuildState, err error) {
	state.mu.Lock()
	if state.status.State != MaterializedViewPointLookupBuildStateBuilding {
		state.mu.Unlock()
		return
	}
	state.status.State = next
	state.err = err
	if err != nil {
		state.status.Err = err.Error()
	}
	state.status.FinishedAt = time.Now().UTC()
	close(state.done)
	state.mu.Unlock()
}

// StartPointLookupIndexBuild starts an off-lock build against the current
// immutable view snapshot. The index becomes visible atomically only if that
// view still has the same generation when the build completes.
func (views *MaterializedViews) StartPointLookupIndexBuild(ctx context.Context, definition MaterializedViewPointLookupDefinition) (*MaterializedViewPointLookupBuild, error) {
	if views == nil {
		return nil, fmt.Errorf("materialized views are nil")
	}
	if ctx == nil {
		return nil, fmt.Errorf("materialized view point lookup build context is nil")
	}
	definition, err := normalizeMaterializedViewPointLookupDefinition(definition)
	if err != nil {
		return nil, err
	}

	buildContext, cancel := context.WithCancel(ctx)
	views.mu.Lock()
	if _, exists := views.pointLookups[definition.Name]; exists {
		views.mu.Unlock()
		cancel()
		return nil, fmt.Errorf("%w: %q", ErrMaterializedViewPointLookupIndexExists, definition.Name)
	}
	if build, exists := views.pointLookupBuilds[definition.Name]; exists && build != nil {
		views.mu.Unlock()
		cancel()
		return nil, fmt.Errorf("%w: %q", ErrMaterializedViewPointLookupBuildInProgress, definition.Name)
	}
	view, exists := views.views[definition.ViewName]
	if !exists {
		views.mu.Unlock()
		cancel()
		return nil, fmt.Errorf("%w: %q", ErrMaterializedViewPointLookupViewMissing, definition.ViewName)
	}
	if views.pointLookupBuilds == nil {
		views.pointLookupBuilds = make(map[string]*MaterializedViewPointLookupBuild)
	}
	status := MaterializedViewPointLookupBuildStatus{
		IndexName:    definition.Name,
		ViewName:     definition.ViewName,
		ViewRevision: view.snapshot.Status.Revision,
		State:        MaterializedViewPointLookupBuildStateBuilding,
		TotalRows:    uint64(len(view.snapshot.Result.Rows)),
		StartedAt:    time.Now().UTC(),
	}
	state := &materializedViewPointLookupBuildState{
		status:    status,
		done:      make(chan struct{}),
		cancel:    cancel,
		indexName: definition.Name,
		viewName:  definition.ViewName,
	}
	build := &MaterializedViewPointLookupBuild{state: state}
	views.pointLookupBuilds[definition.Name] = build
	viewRevision := view.snapshot.Status.Revision
	viewGeneration := view.generation
	result := view.snapshot.Result
	views.mu.Unlock()

	go views.runPointLookupIndexBuild(buildContext, build, definition, viewRevision, viewGeneration, result)
	return build, nil
}

func (views *MaterializedViews) runPointLookupIndexBuild(ctx context.Context, build *MaterializedViewPointLookupBuild, definition MaterializedViewPointLookupDefinition, viewRevision, viewGeneration uint64, result QueryResult) {
	defer build.Cancel()
	index, err := buildMaterializedViewPointLookupWithProgress(ctx, definition, result, build.state.setFrontier)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			views.finishPointLookupIndexBuild(build, MaterializedViewPointLookupBuildStateCanceled, fmt.Errorf("%w: %v", ErrMaterializedViewPointLookupBuildCanceled, err))
		} else {
			views.finishPointLookupIndexBuild(build, MaterializedViewPointLookupBuildStateFailed, err)
		}
		return
	}
	if err := ctx.Err(); err != nil {
		views.finishPointLookupIndexBuild(build, MaterializedViewPointLookupBuildStateCanceled, fmt.Errorf("%w: %v", ErrMaterializedViewPointLookupBuildCanceled, err))
		return
	}

	views.mu.Lock()
	active := views.pointLookupBuilds[definition.Name]
	if active != build {
		views.mu.Unlock()
		build.state.finish(MaterializedViewPointLookupBuildStateCanceled, ErrMaterializedViewPointLookupBuildCanceled)
		return
	}
	if err := ctx.Err(); err != nil {
		delete(views.pointLookupBuilds, definition.Name)
		views.mu.Unlock()
		build.state.finish(MaterializedViewPointLookupBuildStateCanceled, fmt.Errorf("%w: %v", ErrMaterializedViewPointLookupBuildCanceled, err))
		return
	}
	view, exists := views.views[definition.ViewName]
	if !exists || view.snapshot.Status.Revision != viewRevision || view.generation != viewGeneration {
		delete(views.pointLookupBuilds, definition.Name)
		views.mu.Unlock()
		build.state.finish(MaterializedViewPointLookupBuildStateFailed, fmt.Errorf("%w: view %q changed during build", ErrMaterializedViewPointLookupBuildStale, definition.ViewName))
		return
	}
	if _, exists := views.pointLookups[definition.Name]; exists {
		delete(views.pointLookupBuilds, definition.Name)
		views.mu.Unlock()
		build.state.finish(MaterializedViewPointLookupBuildStateFailed, fmt.Errorf("%w: %q", ErrMaterializedViewPointLookupIndexExists, definition.Name))
		return
	}
	views.pointLookups[definition.Name] = index
	delete(views.pointLookupBuilds, definition.Name)
	views.mu.Unlock()
	build.state.finish(MaterializedViewPointLookupBuildStateReady, nil)
}

func (views *MaterializedViews) finishPointLookupIndexBuild(build *MaterializedViewPointLookupBuild, state MaterializedViewPointLookupBuildState, err error) {
	views.mu.Lock()
	if views.pointLookupBuilds[build.state.indexName] == build {
		delete(views.pointLookupBuilds, build.state.indexName)
	}
	views.mu.Unlock()
	build.state.finish(state, err)
}

func normalizeMaterializedViewPointLookupDefinition(definition MaterializedViewPointLookupDefinition) (MaterializedViewPointLookupDefinition, error) {
	definition.Name = strings.TrimSpace(definition.Name)
	definition.ViewName = strings.TrimSpace(definition.ViewName)
	if definition.Name == "" {
		return MaterializedViewPointLookupDefinition{}, fmt.Errorf("materialized view point lookup index name is required")
	}
	if definition.ViewName == "" {
		return MaterializedViewPointLookupDefinition{}, fmt.Errorf("materialized view point lookup view name is required")
	}
	if definition.Key == nil {
		return MaterializedViewPointLookupDefinition{}, fmt.Errorf("materialized view point lookup key function is required")
	}
	return definition, nil
}

func buildMaterializedViewPointLookupWithProgress(ctx context.Context, definition MaterializedViewPointLookupDefinition, result QueryResult, progress func(uint64)) (materializedViewPointLookup, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	index := materializedViewPointLookup{
		definition: definition,
		columns:    append([]string(nil), result.Columns...),
		rows:       make(map[string][]Row, len(result.Rows)),
	}
	for rowIndex, row := range result.Rows {
		select {
		case <-ctx.Done():
			return materializedViewPointLookup{}, ctx.Err()
		default:
		}
		key, err := definition.Key(row)
		if err != nil {
			return materializedViewPointLookup{}, fmt.Errorf("materialized view point lookup %q row %d: %w", definition.Name, rowIndex, err)
		}
		select {
		case <-ctx.Done():
			return materializedViewPointLookup{}, ctx.Err()
		default:
		}
		index.rows[key] = append(index.rows[key], row)
		if progress != nil && (rowIndex+1 == len(result.Rows) || (rowIndex+1)%materializedViewPointLookupBuildProgressInterval == 0) {
			progress(uint64(rowIndex + 1))
		}
	}
	return index, nil
}
