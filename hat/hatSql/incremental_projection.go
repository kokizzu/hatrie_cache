package hatSql

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
)

// ProjectionChange identifies one committed source mutation in an ordered log.
// Dependency must match a MaterializedViewDefinition dependency.
type ProjectionChange struct {
	Sequence   uint64
	Dependency string
}

// ProjectionCheckpointStore persists a runner checkpoint. Stores should make
// SaveProjectionCheckpoint durable before returning. Reapplying a successfully
// refreshed batch after a failed save is safe and intentionally at-least-once.
type ProjectionCheckpointStore interface {
	LoadProjectionCheckpoint(ctx context.Context, name string) (sequence uint64, found bool, err error)
	SaveProjectionCheckpoint(ctx context.Context, name string, sequence uint64) error
}

// IncrementalProjectionRunnerOptions controls ordered materialized-view
// refreshes. Enabled defaults to false so creating a runner has no query or
// journal-read cost until an application explicitly enables it.
type IncrementalProjectionRunnerOptions struct {
	Name            string
	Enabled         bool
	CheckpointStore ProjectionCheckpointStore
}

// ProjectionRun reports one processed change batch.
type ProjectionRun struct {
	Enabled         bool
	FromSequence    uint64
	ThroughSequence uint64
	Changes         int
	Dependencies    []string
	IdempotencyKeys []string
	Refreshed       []MaterializedViewStatus
}

// IncrementalProjectionRunner advances materialized views from a strictly
// ordered change log. It coalesces each batch into one source-consistent view
// refresh, rather than assuming arbitrary CACHE document replacements expose
// safe row-level before/after deltas.
type IncrementalProjectionRunner struct {
	mu       sync.Mutex
	views    *MaterializedViews
	resolver SourceResolver
	options  QueryOptions
	config   IncrementalProjectionRunnerOptions

	checkpoint uint64
	status     ProjectionRefreshStatus
}

// NewIncrementalProjectionRunner creates an optional ordered projection
// consumer. A durable checkpoint is loaded only when the runner is enabled.
func NewIncrementalProjectionRunner(views *MaterializedViews, resolver SourceResolver, options QueryOptions, config IncrementalProjectionRunnerOptions) (*IncrementalProjectionRunner, error) {
	if views == nil {
		return nil, fmt.Errorf("incremental projection views are nil")
	}
	if resolver == nil {
		return nil, fmt.Errorf("incremental projection resolver is nil")
	}
	config.Name = strings.TrimSpace(config.Name)
	if config.Name == "" {
		return nil, fmt.Errorf("incremental projection name is required")
	}
	runner := &IncrementalProjectionRunner{views: views, resolver: resolver, options: options, config: config}
	runner.status = newProjectionRefreshStatus(config.Name, config.Enabled, runner.checkpoint)
	if !config.Enabled || config.CheckpointStore == nil {
		return runner, nil
	}
	checkpoint, found, err := config.CheckpointStore.LoadProjectionCheckpoint(context.Background(), config.Name)
	if err != nil {
		return nil, fmt.Errorf("load incremental projection checkpoint %q: %w", config.Name, err)
	}
	if found {
		runner.checkpoint = checkpoint
		runner.status.AppliedSequence = checkpoint
		runner.status.ObservedSequence = checkpoint
	}
	return runner, nil
}

// Enabled reports whether Apply will process log changes.
func (runner *IncrementalProjectionRunner) Enabled() bool {
	return runner != nil && runner.config.Enabled
}

// Checkpoint returns the last sequence whose source-consistent refresh and
// optional checkpoint save completed successfully.
func (runner *IncrementalProjectionRunner) Checkpoint() uint64 {
	if runner == nil {
		return 0
	}
	runner.mu.Lock()
	defer runner.mu.Unlock()
	return runner.checkpoint
}

// Apply processes a contiguous log batch. Changes at or below the checkpoint
// are ignored, which makes replay idempotent. New changes must be contiguous;
// callers must rebuild from a trusted snapshot rather than silently skipping a
// missing mutation.
func (runner *IncrementalProjectionRunner) Apply(ctx context.Context, changes []ProjectionChange) (ProjectionRun, error) {
	return runner.apply(ctx, changes, nil)
}

// ApplyWithIdempotencyKeys processes a contiguous log batch and carries one
// optional idempotency key per change. Pass nil when the source has no keys;
// a non-empty key slice must have the same length as changes.
func (runner *IncrementalProjectionRunner) ApplyWithIdempotencyKeys(ctx context.Context, changes []ProjectionChange, idempotencyKeys []string) (ProjectionRun, error) {
	return runner.apply(ctx, changes, idempotencyKeys)
}

func (runner *IncrementalProjectionRunner) apply(ctx context.Context, changes []ProjectionChange, idempotencyKeys []string) (ProjectionRun, error) {
	if runner == nil {
		return ProjectionRun{}, fmt.Errorf("incremental projection runner is nil")
	}
	if len(idempotencyKeys) != 0 && len(idempotencyKeys) != len(changes) {
		return ProjectionRun{}, fmt.Errorf("incremental projection idempotency keys must match change count")
	}
	runner.mu.Lock()
	defer runner.mu.Unlock()

	runner.ensureProjectionRefreshStatusLocked()
	run := ProjectionRun{Enabled: runner.config.Enabled, FromSequence: runner.checkpoint, ThroughSequence: runner.checkpoint}
	if !runner.config.Enabled {
		return run, nil
	}

	observed := runner.status.ObservedSequence
	for _, change := range changes {
		if change.Sequence > observed {
			observed = change.Sequence
		}
	}
	runner.observeProjectionSequenceLocked(observed)

	dependencies := make(map[string]struct{}, len(changes))
	expected := runner.checkpoint
	for index, change := range changes {
		if change.Sequence <= runner.checkpoint {
			continue
		}
		if expected == ^uint64(0) || change.Sequence != expected+1 {
			err := fmt.Errorf("incremental projection %q expected sequence %d, got %d", runner.config.Name, expected+1, change.Sequence)
			runner.markProjectionRefreshFailureLocked(observed, err)
			return run, err
		}
		dependency := strings.TrimSpace(change.Dependency)
		if dependency == "" {
			err := fmt.Errorf("incremental projection %q sequence %d has an empty dependency", runner.config.Name, change.Sequence)
			runner.markProjectionRefreshFailureLocked(observed, err)
			return run, err
		}
		expected = change.Sequence
		run.Changes++
		if len(idempotencyKeys) != 0 {
			if key := strings.TrimSpace(idempotencyKeys[index]); key != "" {
				run.IdempotencyKeys = append(run.IdempotencyKeys, key)
			}
		}
		dependencies[dependency] = struct{}{}
	}
	if run.Changes == 0 {
		return run, nil
	}

	run.ThroughSequence = expected
	run.Dependencies = make([]string, 0, len(dependencies))
	for dependency := range dependencies {
		run.Dependencies = append(run.Dependencies, dependency)
	}
	sort.Strings(run.Dependencies)
	var refreshed []MaterializedViewStatus
	var err error
	if len(run.IdempotencyKeys) == 0 {
		refreshed, err = runner.views.RefreshChanged(ctx, run.Dependencies, runner.resolver, runner.options)
	} else {
		refreshed, err = runner.views.RefreshChangedWithMetadata(ctx, run.Dependencies, runner.resolver, runner.options, MaterializedViewRefreshMetadata{IdempotencyKeys: run.IdempotencyKeys})
	}
	if err != nil {
		run = ProjectionRun{Enabled: runner.config.Enabled, FromSequence: runner.checkpoint, ThroughSequence: runner.checkpoint}
		err = fmt.Errorf("refresh incremental projection %q: %w", runner.config.Name, err)
		runner.markProjectionRefreshFailureLocked(observed, err)
		return run, err
	}
	if runner.config.CheckpointStore != nil {
		if err := runner.config.CheckpointStore.SaveProjectionCheckpoint(ctx, runner.config.Name, expected); err != nil {
			run = ProjectionRun{Enabled: runner.config.Enabled, FromSequence: runner.checkpoint, ThroughSequence: runner.checkpoint}
			err = fmt.Errorf("save incremental projection checkpoint %q: %w", runner.config.Name, err)
			runner.markProjectionRefreshFailureLocked(observed, err)
			return run, err
		}
	}
	runner.checkpoint = expected
	runner.markProjectionRefreshSuccessLocked(expected)
	run.Refreshed = cloneProjectionStatuses(refreshed)
	return run, nil
}

// Rebuild refreshes selected dependencies from a trusted current source
// snapshot and adopts throughSequence as the recovered log boundary. Use it
// after a journal-retention gap only after recovering the source snapshot to
// that same boundary. It is disabled by default with the rest of the runner.
func (runner *IncrementalProjectionRunner) Rebuild(ctx context.Context, dependencies []string, throughSequence uint64) (ProjectionRun, error) {
	if runner == nil {
		return ProjectionRun{}, fmt.Errorf("incremental projection runner is nil")
	}
	runner.mu.Lock()
	defer runner.mu.Unlock()

	runner.ensureProjectionRefreshStatusLocked()
	run := ProjectionRun{Enabled: runner.config.Enabled, FromSequence: runner.checkpoint, ThroughSequence: runner.checkpoint}
	if !runner.config.Enabled {
		return run, nil
	}
	if throughSequence < runner.checkpoint {
		return run, fmt.Errorf("incremental projection %q cannot rebuild through sequence %d before checkpoint %d", runner.config.Name, throughSequence, runner.checkpoint)
	}
	normalized, err := normalizeProjectionDependencies(dependencies)
	if err != nil {
		return run, err
	}
	runner.observeProjectionSequenceLocked(throughSequence)
	refreshed, err := runner.views.RefreshChanged(ctx, normalized, runner.resolver, runner.options)
	if err != nil {
		err = fmt.Errorf("rebuild incremental projection %q: %w", runner.config.Name, err)
		runner.markProjectionRefreshFailureLocked(throughSequence, err)
		return run, err
	}
	if runner.config.CheckpointStore != nil {
		if err := runner.config.CheckpointStore.SaveProjectionCheckpoint(ctx, runner.config.Name, throughSequence); err != nil {
			err = fmt.Errorf("save rebuilt incremental projection checkpoint %q: %w", runner.config.Name, err)
			runner.markProjectionRefreshFailureLocked(throughSequence, err)
			return run, err
		}
	}
	runner.checkpoint = throughSequence
	runner.markProjectionRefreshSuccessLocked(throughSequence)
	run.ThroughSequence = throughSequence
	run.Dependencies = normalized
	run.Refreshed = cloneProjectionStatuses(refreshed)
	return run, nil
}

func normalizeProjectionDependencies(dependencies []string) ([]string, error) {
	if len(dependencies) == 0 {
		return nil, fmt.Errorf("incremental projection dependencies are required")
	}
	seen := make(map[string]struct{}, len(dependencies))
	normalized := make([]string, 0, len(dependencies))
	for _, dependency := range dependencies {
		dependency = strings.TrimSpace(dependency)
		if dependency == "" {
			return nil, fmt.Errorf("incremental projection dependency is required")
		}
		if _, exists := seen[dependency]; exists {
			continue
		}
		seen[dependency] = struct{}{}
		normalized = append(normalized, dependency)
	}
	sort.Strings(normalized)
	return normalized, nil
}

func cloneProjectionStatuses(statuses []MaterializedViewStatus) []MaterializedViewStatus {
	cloned := make([]MaterializedViewStatus, len(statuses))
	for index, status := range statuses {
		cloned[index] = cloneMaterializedViewStatus(status)
	}
	return cloned
}
