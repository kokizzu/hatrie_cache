package hatCache

import (
	"context"
	"fmt"
	"strings"

	"hatrie_cache/hat/hatSql"
)

// SQLJournalTailer is implemented by CommandJournal and permits a projection
// runner to consume a bounded ordered mutation tail.
type SQLJournalTailer interface {
	Tail(afterSequence uint64, limit int) (CommandJournalTail, error)
}

// SQLJournalProjectionRunner adapts CommandJournal records into portable SQL
// projection changes. It never reads a journal while the underlying runner is
// disabled.
type SQLJournalProjectionRunner struct {
	runner *hatSql.IncrementalProjectionRunner
}

// NewSQLJournalProjectionRunner creates an optional journal-driven SQL
// materialized-view runner. It is disabled unless options.Enabled is true.
func NewSQLJournalProjectionRunner(views *hatSql.MaterializedViews, resolver hatSql.SourceResolver, options hatSql.QueryOptions, config hatSql.IncrementalProjectionRunnerOptions) (*SQLJournalProjectionRunner, error) {
	runner, err := hatSql.NewIncrementalProjectionRunner(views, resolver, options, config)
	if err != nil {
		return nil, err
	}
	return &SQLJournalProjectionRunner{runner: runner}, nil
}

// Enabled reports whether RunOnce will read and apply journal records.
func (runner *SQLJournalProjectionRunner) Enabled() bool {
	return runner != nil && runner.runner != nil && runner.runner.Enabled()
}

// Checkpoint returns the last fully applied journal sequence.
func (runner *SQLJournalProjectionRunner) Checkpoint() uint64 {
	if runner == nil || runner.runner == nil {
		return 0
	}
	return runner.runner.Checkpoint()
}

// Rebuild refreshes dependencies from a source snapshot that has already been
// recovered through throughSequence, then adopts that sequence as the journal
// checkpoint.
func (runner *SQLJournalProjectionRunner) Rebuild(ctx context.Context, dependencies []string, throughSequence uint64) (hatSql.ProjectionRun, error) {
	if runner == nil || runner.runner == nil {
		return hatSql.ProjectionRun{}, fmt.Errorf("SQL journal projection runner is nil")
	}
	return runner.runner.Rebuild(ctx, dependencies, throughSequence)
}

// RunOnce consumes at most limit journal records and coalesces their affected
// CACHE keys into one source-consistent materialized-view refresh.
func (runner *SQLJournalProjectionRunner) RunOnce(ctx context.Context, journal SQLJournalTailer, limit int) (hatSql.ProjectionRun, error) {
	if runner == nil || runner.runner == nil {
		return hatSql.ProjectionRun{}, fmt.Errorf("SQL journal projection runner is nil")
	}
	if !runner.runner.Enabled() {
		return runner.runner.Apply(ctx, nil)
	}
	if journal == nil {
		return hatSql.ProjectionRun{}, fmt.Errorf("SQL journal projection journal is nil")
	}
	tail, err := journal.Tail(runner.runner.Checkpoint(), limit)
	if err != nil {
		return hatSql.ProjectionRun{}, err
	}
	changes := make([]hatSql.ProjectionChange, 0, len(tail.Entries))
	for _, entry := range tail.Entries {
		key := strings.TrimSpace(entry.Request.Key)
		if key == "" {
			return hatSql.ProjectionRun{}, fmt.Errorf("SQL journal projection record %d has an empty key", entry.Sequence)
		}
		changes = append(changes, hatSql.ProjectionChange{Sequence: entry.Sequence, Dependency: key})
	}
	return runner.runner.Apply(ctx, changes)
}
