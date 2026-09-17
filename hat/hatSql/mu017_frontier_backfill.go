package hatSql

import (
	"context"
	"fmt"
)

// BackfillAtFrontier refreshes the runner's maintained views from one
// immutable source snapshot after every configured partition reaches frontier.
// It advances the checkpoint only after the snapshot refresh and optional
// durable checkpoint save succeed; the next live Apply may therefore begin at
// frontier+1 without replaying the backfill boundary.
func (runner *IncrementalProjectionRunner) BackfillAtFrontier(ctx context.Context, dependencies []string, barrier *SQLSourceFrontierBarrier, frontier uint64) (ProjectionRun, error) {
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
	if frontier < runner.checkpoint {
		return run, fmt.Errorf("incremental projection %q cannot backfill through sequence %d before checkpoint %d", runner.config.Name, frontier, runner.checkpoint)
	}
	normalized, err := normalizeProjectionDependencies(dependencies)
	if err != nil {
		return run, err
	}
	snapshotResolver, release, err := BeginSQLFrontierSnapshot(ctx, runner.resolver, barrier, frontier)
	if err != nil {
		runner.markProjectionRefreshFailureLocked(runner.status.ObservedSequence, err)
		return run, fmt.Errorf("backfill incremental projection %q: %w", runner.config.Name, err)
	}
	if release != nil {
		defer release()
	}
	runner.observeProjectionSequenceLocked(frontier)
	refreshed, err := runner.views.RefreshChanged(ctx, normalized, snapshotResolver, runner.options)
	if err != nil {
		err = fmt.Errorf("refresh frontier backfill for incremental projection %q: %w", runner.config.Name, err)
		runner.markProjectionRefreshFailureLocked(frontier, err)
		return run, err
	}
	if runner.config.CheckpointStore != nil {
		if err := runner.config.CheckpointStore.SaveProjectionCheckpoint(ctx, runner.config.Name, frontier); err != nil {
			err = fmt.Errorf("save frontier backfill checkpoint %q: %w", runner.config.Name, err)
			runner.markProjectionRefreshFailureLocked(frontier, err)
			return run, err
		}
	}
	runner.checkpoint = frontier
	runner.markProjectionRefreshSuccessLocked(frontier)
	run.ThroughSequence = frontier
	run.Dependencies = normalized
	run.Refreshed = cloneProjectionStatuses(refreshed)
	return run, nil
}
