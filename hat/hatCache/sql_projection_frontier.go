package hatCache

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"

	"hatrie_cache/hat/hatSql"
)

// SQLProjectionRetentionFrontier coordinates journal retention for a group of
// projections that must all finish a batch before the journal may compact it.
// It is opt-in: legacy projection runners retain their independent behavior.
type SQLProjectionRetentionFrontier struct {
	mu          sync.Mutex
	runMu       sync.Mutex
	name        string
	sources     []string
	checkpoints map[string]uint64
	checkpoint  uint64
}

// NewSQLProjectionRetentionFrontier creates a frontier for an exact, nonempty
// set of source names. Source names are normalized and kept sorted so callers
// receive deterministic snapshots regardless of construction order.
func NewSQLProjectionRetentionFrontier(name string, sources []string) (*SQLProjectionRetentionFrontier, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return nil, fmt.Errorf("SQL projection retention frontier name is required")
	}
	if len(sources) == 0 {
		return nil, fmt.Errorf("SQL projection retention frontier %q requires at least one source", name)
	}
	checkpoints := make(map[string]uint64, len(sources))
	for _, source := range sources {
		source = strings.TrimSpace(source)
		if source == "" {
			return nil, fmt.Errorf("SQL projection retention frontier %q has an empty source", name)
		}
		if _, exists := checkpoints[source]; exists {
			return nil, fmt.Errorf("SQL projection retention frontier %q has duplicate source %q", name, source)
		}
		checkpoints[source] = 0
	}
	normalizedSources := make([]string, 0, len(checkpoints))
	for source := range checkpoints {
		normalizedSources = append(normalizedSources, source)
	}
	sort.Strings(normalizedSources)
	return &SQLProjectionRetentionFrontier{name: name, sources: normalizedSources, checkpoints: checkpoints}, nil
}

// Begin records the last fully committed frontier in journal before a caller
// starts a coordinated batch. If a participant fails, no Commit call is made
// and compaction remains pinned to this stable checkpoint.
func (frontier *SQLProjectionRetentionFrontier) Begin(journal SQLJournalTailer) error {
	if frontier == nil {
		return fmt.Errorf("SQL projection retention frontier is nil")
	}
	frontier.mu.Lock()
	defer frontier.mu.Unlock()
	return frontier.setJournalWatermark(journal, frontier.checkpoint)
}

// Commit atomically adopts a complete source checkpoint set and advances the
// journal watermark to its minimum sequence. Every configured source must be
// present and nondecreasing; incomplete or regressing batches leave both the
// in-memory frontier and journal retention unchanged.
func (frontier *SQLProjectionRetentionFrontier) Commit(journal SQLJournalTailer, checkpoints map[string]uint64) error {
	if frontier == nil {
		return fmt.Errorf("SQL projection retention frontier is nil")
	}
	frontier.mu.Lock()
	defer frontier.mu.Unlock()
	if len(checkpoints) != len(frontier.sources) {
		return fmt.Errorf("SQL projection retention frontier %q requires checkpoints for all %d sources", frontier.name, len(frontier.sources))
	}
	minimum := uint64(0)
	for index, source := range frontier.sources {
		sequence, exists := checkpoints[source]
		if !exists {
			return fmt.Errorf("SQL projection retention frontier %q is missing source %q", frontier.name, source)
		}
		if sequence < frontier.checkpoints[source] {
			return fmt.Errorf("SQL projection retention frontier %q source %q regressed from %d to %d", frontier.name, source, frontier.checkpoints[source], sequence)
		}
		if index == 0 || sequence < minimum {
			minimum = sequence
		}
	}
	for source := range checkpoints {
		if _, exists := frontier.checkpoints[source]; !exists {
			return fmt.Errorf("SQL projection retention frontier %q has unknown source %q", frontier.name, source)
		}
	}
	if minimum < frontier.checkpoint {
		return fmt.Errorf("SQL projection retention frontier %q regressed from %d to %d", frontier.name, frontier.checkpoint, minimum)
	}
	if err := frontier.setJournalWatermark(journal, minimum); err != nil {
		return err
	}
	for source, sequence := range checkpoints {
		frontier.checkpoints[source] = sequence
	}
	frontier.checkpoint = minimum
	return nil
}

// RunOnce runs one unprotected projection runner for every configured source
// and commits their shared retention frontier only after all runners succeed.
// Each map key must exactly match a configured source. Runners that maintain
// their own retention watermark are rejected because they could advance journal
// compaction before this coordinated batch is fully complete.
func (frontier *SQLProjectionRetentionFrontier) RunOnce(ctx context.Context, journal SQLJournalTailer, limit int, runners map[string]*SQLJournalProjectionRunner) (SQLProjectionRetentionRun, error) {
	if frontier == nil {
		return SQLProjectionRetentionRun{}, fmt.Errorf("SQL projection retention frontier is nil")
	}
	frontier.runMu.Lock()
	defer frontier.runMu.Unlock()
	frontier.mu.Lock()
	sources := append([]string(nil), frontier.sources...)
	frontier.mu.Unlock()
	if len(runners) != len(sources) {
		return SQLProjectionRetentionRun{}, fmt.Errorf("SQL projection retention frontier %q requires runners for all %d sources", frontier.name, len(sources))
	}
	for _, source := range sources {
		runner, exists := runners[source]
		if !exists || runner == nil {
			return SQLProjectionRetentionRun{}, fmt.Errorf("SQL projection retention frontier %q is missing runner for source %q", frontier.name, source)
		}
		if runner.protectJournalRetention {
			return SQLProjectionRetentionRun{}, fmt.Errorf("SQL projection retention frontier %q source %q must use an unprotected runner", frontier.name, source)
		}
	}
	for source := range runners {
		found := false
		for _, configured := range sources {
			if configured == source {
				found = true
				break
			}
		}
		if !found {
			return SQLProjectionRetentionRun{}, fmt.Errorf("SQL projection retention frontier %q has unknown runner source %q", frontier.name, source)
		}
	}
	if err := frontier.Begin(journal); err != nil {
		return SQLProjectionRetentionRun{}, err
	}
	run := SQLProjectionRetentionRun{Runs: make(map[string]hatSql.ProjectionRun, len(sources))}
	checkpoints := make(map[string]uint64, len(sources))
	for _, source := range sources {
		projectionRun, err := runners[source].RunOnce(ctx, journal, limit)
		if err != nil {
			return SQLProjectionRetentionRun{}, fmt.Errorf("SQL projection retention frontier %q source %q: %w", frontier.name, source, err)
		}
		run.Runs[source] = projectionRun
		checkpoints[source] = runners[source].Checkpoint()
	}
	if err := frontier.Commit(journal, checkpoints); err != nil {
		return SQLProjectionRetentionRun{}, err
	}
	run.ThroughSequence = frontier.Checkpoint()
	return run, nil
}

// Checkpoint returns the highest journal sequence all configured sources have
// fully applied and committed through this frontier.
func (frontier *SQLProjectionRetentionFrontier) Checkpoint() uint64 {
	if frontier == nil {
		return 0
	}
	frontier.mu.Lock()
	defer frontier.mu.Unlock()
	return frontier.checkpoint
}

// SourceCheckpoints returns a deterministic copy of each source's committed
// progress. It is safe to use for recovery status and operator diagnostics.
func (frontier *SQLProjectionRetentionFrontier) SourceCheckpoints() []SQLProjectionSourceCheckpoint {
	if frontier == nil {
		return nil
	}
	frontier.mu.Lock()
	defer frontier.mu.Unlock()
	checkpoints := make([]SQLProjectionSourceCheckpoint, 0, len(frontier.sources))
	for _, source := range frontier.sources {
		checkpoints = append(checkpoints, SQLProjectionSourceCheckpoint{Source: source, Sequence: frontier.checkpoints[source]})
	}
	return checkpoints
}

// Remove stops this frontier from pinning journal retention. Call it only once
// the coordinated projections are permanently stopped or have been replaced.
func (frontier *SQLProjectionRetentionFrontier) Remove(journal SQLJournalTailer) bool {
	if frontier == nil {
		return false
	}
	watermarker, ok := journal.(SQLJournalProjectionWatermarker)
	return ok && watermarker.RemoveProjectionWatermark(frontier.name)
}

func (frontier *SQLProjectionRetentionFrontier) setJournalWatermark(journal SQLJournalTailer, sequence uint64) error {
	if journal == nil {
		return fmt.Errorf("SQL projection retention frontier %q journal is nil", frontier.name)
	}
	watermarker, ok := journal.(SQLJournalProjectionWatermarker)
	if !ok {
		return fmt.Errorf("SQL projection retention frontier %q requires a journal watermarker", frontier.name)
	}
	return watermarker.SetProjectionWatermark(frontier.name, sequence)
}

// SQLProjectionSourceCheckpoint reports one source's committed sequence in a
// SQLProjectionRetentionFrontier.
type SQLProjectionSourceCheckpoint struct {
	Source   string `json:"source"`
	Sequence uint64 `json:"sequence"`
}

// SQLProjectionRetentionRun reports one successful coordinated projection
// batch. ThroughSequence is the sequence every configured source has applied.
type SQLProjectionRetentionRun struct {
	ThroughSequence uint64                          `json:"through_sequence"`
	Runs            map[string]hatSql.ProjectionRun `json:"runs"`
}
