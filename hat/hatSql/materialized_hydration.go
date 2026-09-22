package hatSql

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

var (
	// ErrMaterializedViewHydrationInProgress indicates that a view cannot be
	// transitioned because its hydration is still running.
	ErrMaterializedViewHydrationInProgress = errors.New("materialized view hydration in progress")
	// ErrMaterializedViewHydrationNotCold indicates that Hydrate was requested
	// for a view that is already ready or is otherwise not cold.
	ErrMaterializedViewHydrationNotCold = errors.New("materialized view is not cold")
	// ErrMaterializedViewHydrationNotReady indicates that a view cannot serve
	// reads or incremental refreshes until hydration reaches ready.
	ErrMaterializedViewHydrationNotReady = errors.New("materialized view is not ready")
)

// MaterializedViewHydrationState describes whether a maintained view has no
// usable snapshot, is building one, or may serve reads.
type MaterializedViewHydrationState string

const (
	MaterializedViewHydrationStateCold      MaterializedViewHydrationState = "cold"
	MaterializedViewHydrationStateHydrating MaterializedViewHydrationState = "hydrating"
	MaterializedViewHydrationStateReady     MaterializedViewHydrationState = "ready"
)

func materializedViewHydrationState(status MaterializedViewStatus) MaterializedViewHydrationState {
	if status.HydrationState == "" {
		// Treat statuses created before M223 as ready for compatibility.
		return MaterializedViewHydrationStateReady
	}
	return status.HydrationState
}

func materializedViewIsReady(view materializedView) bool {
	return materializedViewHydrationState(view.snapshot.Status) == MaterializedViewHydrationStateReady
}

// CreateCold registers a maintained view without evaluating its query. The
// returned view is not readable until Hydrate publishes its first snapshot.
func (views *MaterializedViews) CreateCold(definition MaterializedViewDefinition) (MaterializedViewStatus, error) {
	definition, err := normalizeMaterializedViewDefinition(definition)
	if err != nil {
		return MaterializedViewStatus{}, err
	}
	if views == nil {
		return MaterializedViewStatus{}, fmt.Errorf("materialized views are nil")
	}

	views.mu.Lock()
	defer views.mu.Unlock()
	if views.views == nil {
		views.views = make(map[string]materializedView)
	}
	if views.dependents == nil {
		views.dependents = make(map[string][]string)
	}
	if _, exists := views.views[definition.Name]; exists {
		return MaterializedViewStatus{}, fmt.Errorf("materialized view %q already exists", definition.Name)
	}
	status := MaterializedViewStatus{
		Name:           definition.Name,
		Dependencies:   append([]string(nil), definition.Dependencies...),
		HydrationState: MaterializedViewHydrationStateCold,
	}
	views.nextGeneration++
	views.views[definition.Name] = materializedView{
		definition: definition,
		snapshot: MaterializedView{
			Status: status,
		},
		generation: views.nextGeneration,
	}
	for _, dependency := range definition.Dependencies {
		views.dependents[dependency] = append(views.dependents[dependency], definition.Name)
	}
	return cloneMaterializedViewStatus(status), nil
}

// MarkMaterializedViewCold withdraws a view's retained result and point reads
// until a subsequent Hydrate completes. Existing point-lookup reader leases
// retain their own snapshots, while new lookups are fenced by state.
func (views *MaterializedViews) MarkMaterializedViewCold(name string) error {
	if views == nil {
		return fmt.Errorf("materialized views are nil")
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return fmt.Errorf("materialized view name is required")
	}

	views.mu.Lock()
	defer views.mu.Unlock()
	view, exists := views.views[name]
	if !exists {
		return fmt.Errorf("materialized view %q does not exist", name)
	}
	state := materializedViewHydrationState(view.snapshot.Status)
	if state == MaterializedViewHydrationStateHydrating {
		return fmt.Errorf("%w: %q", ErrMaterializedViewHydrationInProgress, name)
	}
	if state == MaterializedViewHydrationStateCold {
		return nil
	}

	for _, build := range views.pointLookupBuilds {
		if build != nil && build.state.viewName == name {
			build.Cancel()
		}
	}
	views.nextGeneration++
	view.generation = views.nextGeneration
	view.snapshot.Result = QueryResult{}
	view.snapshot.Status.Revision = 0
	view.snapshot.Status.RefreshedAt = time.Time{}
	view.snapshot.Status.IdempotencyKeys = nil
	view.snapshot.Status.HydrationState = MaterializedViewHydrationStateCold
	view.snapshot.Status.HydrationProgress = MaterializedViewHydrationProgress{}
	view.hydrationProgress = nil
	view.sourceVersions = nil
	views.rows -= view.storedRows
	views.bytes -= view.storedBytes
	view.storedRows = 0
	view.storedBytes = 0
	views.views[name] = view
	return nil
}

// Hydrate evaluates a cold view outside the registry lock and atomically
// publishes its first snapshot. While the query is running, Get exposes the
// hydrating state and no read or incremental refresh may use the view.
func (views *MaterializedViews) Hydrate(ctx context.Context, name string, resolver SourceResolver, options QueryOptions) (MaterializedViewStatus, error) {
	if views == nil {
		return MaterializedViewStatus{}, fmt.Errorf("materialized views are nil")
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return MaterializedViewStatus{}, fmt.Errorf("materialized view name is required")
	}
	views.mu.RLock()
	view, exists := views.views[name]
	if !exists {
		views.mu.RUnlock()
		return MaterializedViewStatus{}, fmt.Errorf("materialized view %q does not exist", name)
	}
	state := materializedViewHydrationState(view.snapshot.Status)
	switch state {
	case MaterializedViewHydrationStateHydrating:
		views.mu.RUnlock()
		return MaterializedViewStatus{}, fmt.Errorf("%w: %q", ErrMaterializedViewHydrationInProgress, name)
	case MaterializedViewHydrationStateReady:
		views.mu.RUnlock()
		return MaterializedViewStatus{}, fmt.Errorf("%w: %q", ErrMaterializedViewHydrationNotCold, name)
	case MaterializedViewHydrationStateCold:
	default:
		views.mu.RUnlock()
		return MaterializedViewStatus{}, fmt.Errorf("%w: %q has invalid state %q", ErrMaterializedViewHydrationNotCold, name, state)
	}
	definition := view.definition
	views.mu.RUnlock()

	estimatedWork, estimateAvailable, estimateExact := estimateMaterializedViewHydrationWork(resolver, definition.Dependencies)

	views.mu.Lock()
	view, exists = views.views[name]
	if !exists {
		views.mu.Unlock()
		return MaterializedViewStatus{}, fmt.Errorf("materialized view %q does not exist", name)
	}
	state = materializedViewHydrationState(view.snapshot.Status)
	switch state {
	case MaterializedViewHydrationStateHydrating:
		views.mu.Unlock()
		return MaterializedViewStatus{}, fmt.Errorf("%w: %q", ErrMaterializedViewHydrationInProgress, name)
	case MaterializedViewHydrationStateReady:
		views.mu.Unlock()
		return MaterializedViewStatus{}, fmt.Errorf("%w: %q", ErrMaterializedViewHydrationNotCold, name)
	case MaterializedViewHydrationStateCold:
		progress := newMaterializedViewHydrationProgressState(estimatedWork, estimateAvailable, estimateExact)
		view.snapshot.Status.HydrationState = MaterializedViewHydrationStateHydrating
		view.snapshot.Status.HydrationProgress = progress.snapshot()
		view.hydrationProgress = progress
		generation := view.generation
		views.views[name] = view
		views.mu.Unlock()
		return views.hydrateMaterializedView(withMaterializedViewHydrationProgress(ctx, progress), name, generation, view, resolver, options, progress)
	default:
		views.mu.Unlock()
		return MaterializedViewStatus{}, fmt.Errorf("%w: %q has invalid state %q", ErrMaterializedViewHydrationNotCold, name, state)
	}
}

func (views *MaterializedViews) hydrateMaterializedView(ctx context.Context, name string, generation uint64, view materializedView, resolver SourceResolver, options QueryOptions, progress *materializedViewHydrationProgressState) (MaterializedViewStatus, error) {
	result, sourceVersions, err := executeMaterializedViewQuery(ctx, view.definition.Query, view.definition.Dependencies, resolver, options)
	if err != nil {
		return views.finishMaterializedViewHydrationFailure(name, generation, fmt.Errorf("hydrate materialized view %q: %w", name, err))
	}
	progress.complete()
	result = cloneQueryResult(result)
	storedRows := len(result.Rows)
	storedBytes := views.storageBytes(result)

	views.mu.Lock()
	defer views.mu.Unlock()
	current, exists := views.views[name]
	if !exists {
		return MaterializedViewStatus{}, fmt.Errorf("materialized view %q disappeared during hydration", name)
	}
	if current.generation != generation || materializedViewHydrationState(current.snapshot.Status) != MaterializedViewHydrationStateHydrating {
		return MaterializedViewStatus{}, fmt.Errorf("%w: view %q changed during hydration", ErrMaterializedViewHydrationNotReady, name)
	}
	if err := views.checkStorageBudget(views.rows-current.storedRows+storedRows, views.bytes-current.storedBytes+storedBytes); err != nil {
		current.snapshot.Status.HydrationState = MaterializedViewHydrationStateCold
		current.snapshot.Status.HydrationProgress = MaterializedViewHydrationProgress{}
		current.hydrationProgress = nil
		views.views[name] = current
		return cloneMaterializedViewStatus(current.snapshot.Status), err
	}
	previousStoredRows := current.storedRows
	previousStoredBytes := current.storedBytes

	lookupUpdates := make(map[string]materializedViewPointLookup)
	for indexName, index := range views.pointLookups {
		if index.definition.ViewName != name {
			continue
		}
		refreshed, err := buildMaterializedViewPointLookup(index.definition, result)
		if err != nil {
			current.snapshot.Status.HydrationState = MaterializedViewHydrationStateCold
			current.snapshot.Status.HydrationProgress = MaterializedViewHydrationProgress{}
			current.hydrationProgress = nil
			views.views[name] = current
			return cloneMaterializedViewStatus(current.snapshot.Status), fmt.Errorf("hydrate materialized view point lookup %q: %w", indexName, err)
		}
		lookupUpdates[indexName] = refreshed
	}

	views.nextGeneration++
	current.generation = views.nextGeneration
	current.snapshot.Result = result
	current.snapshot.Status.Revision = 1
	current.snapshot.Status.RefreshedAt = time.Now().UTC()
	current.snapshot.Status.IdempotencyKeys = nil
	current.snapshot.Status.HydrationState = MaterializedViewHydrationStateReady
	current.snapshot.Status.HydrationProgress = progress.snapshot()
	current.hydrationProgress = nil
	current.sourceVersions = sourceVersions
	current.collation = normalizedMaterializedViewCollation(options.Collation)
	current.storedRows = storedRows
	current.storedBytes = storedBytes
	views.rows += storedRows - previousStoredRows
	views.bytes += storedBytes - previousStoredBytes
	for indexName, index := range lookupUpdates {
		views.pointLookups[indexName] = index
	}
	views.views[name] = current
	return cloneMaterializedViewStatus(current.snapshot.Status), nil
}

func (views *MaterializedViews) finishMaterializedViewHydrationFailure(name string, generation uint64, err error) (MaterializedViewStatus, error) {
	views.mu.Lock()
	defer views.mu.Unlock()
	view, exists := views.views[name]
	if !exists || view.generation != generation || materializedViewHydrationState(view.snapshot.Status) != MaterializedViewHydrationStateHydrating {
		return MaterializedViewStatus{}, err
	}
	view.snapshot.Status.HydrationState = MaterializedViewHydrationStateCold
	view.snapshot.Status.HydrationProgress = MaterializedViewHydrationProgress{}
	view.hydrationProgress = nil
	views.views[name] = view
	return cloneMaterializedViewStatus(view.snapshot.Status), err
}
