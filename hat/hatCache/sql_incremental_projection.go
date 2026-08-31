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

// SQLJournalProjectionWatermarker is optionally implemented by a journal that
// can retain records until a named projection has applied them.
type SQLJournalProjectionWatermarker interface {
	SetProjectionWatermark(name string, sequence uint64) error
	RemoveProjectionWatermark(name string) bool
}

// SQLJournalProjectionRunnerOptions configures a journal-driven SQL
// projection. ProtectJournalRetention defaults to false so the existing
// constructor and opt-in projection runner keep their current retention
// behavior unless an application explicitly enables protection.
type SQLJournalProjectionRunnerOptions struct {
	Incremental             hatSql.IncrementalProjectionRunnerOptions
	ProtectJournalRetention bool
}

// SQLJournalProjectionRunner adapts CommandJournal records into portable SQL
// projection changes. It never reads a journal while the underlying runner is
// disabled.
type SQLJournalProjectionRunner struct {
	runner                  *hatSql.IncrementalProjectionRunner
	retentionName           string
	protectJournalRetention bool
}

// NewSQLJournalProjectionRunner creates an optional journal-driven SQL
// materialized-view runner. It is disabled unless options.Enabled is true.
func NewSQLJournalProjectionRunner(views *hatSql.MaterializedViews, resolver hatSql.SourceResolver, options hatSql.QueryOptions, config hatSql.IncrementalProjectionRunnerOptions) (*SQLJournalProjectionRunner, error) {
	return newSQLJournalProjectionRunner(views, resolver, options, config, false)
}

// NewSQLJournalProjectionRunnerWithOptions creates a journal-driven SQL
// projection with optional retention protection. Protection is off by default
// and requires the supplied journal to implement SQLJournalProjectionWatermarker.
func NewSQLJournalProjectionRunnerWithOptions(views *hatSql.MaterializedViews, resolver hatSql.SourceResolver, options hatSql.QueryOptions, config SQLJournalProjectionRunnerOptions) (*SQLJournalProjectionRunner, error) {
	return newSQLJournalProjectionRunner(views, resolver, options, config.Incremental, config.ProtectJournalRetention)
}

func newSQLJournalProjectionRunner(views *hatSql.MaterializedViews, resolver hatSql.SourceResolver, options hatSql.QueryOptions, config hatSql.IncrementalProjectionRunnerOptions, protectJournalRetention bool) (*SQLJournalProjectionRunner, error) {
	runner, err := hatSql.NewIncrementalProjectionRunner(views, resolver, options, config)
	if err != nil {
		return nil, err
	}
	return &SQLJournalProjectionRunner{
		runner:                  runner,
		retentionName:           strings.TrimSpace(config.Name),
		protectJournalRetention: protectJournalRetention,
	}, nil
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

// RemoveJournalRetention removes this runner's opt-in journal retention
// watermark. Call it when a protected runner is permanently stopped; it never
// changes retention for a legacy or unprotected runner.
func (runner *SQLJournalProjectionRunner) RemoveJournalRetention(journal SQLJournalTailer) bool {
	if runner == nil || !runner.protectJournalRetention {
		return false
	}
	watermarker, ok := journal.(SQLJournalProjectionWatermarker)
	return ok && watermarker.RemoveProjectionWatermark(runner.retentionName)
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
	checkpoint := runner.runner.Checkpoint()
	var watermarker SQLJournalProjectionWatermarker
	if runner.protectJournalRetention {
		var ok bool
		watermarker, ok = journal.(SQLJournalProjectionWatermarker)
		if !ok {
			return hatSql.ProjectionRun{}, fmt.Errorf("SQL journal projection retention requires a journal watermarker")
		}
		if err := watermarker.SetProjectionWatermark(runner.retentionName, checkpoint); err != nil {
			return hatSql.ProjectionRun{}, err
		}
	}
	tail, err := journal.Tail(checkpoint, limit)
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
	run, err := runner.runner.Apply(ctx, changes)
	if err != nil {
		return hatSql.ProjectionRun{}, err
	}
	if watermarker != nil {
		if err := watermarker.SetProjectionWatermark(runner.retentionName, run.ThroughSequence); err != nil {
			return hatSql.ProjectionRun{}, err
		}
	}
	return run, nil
}
