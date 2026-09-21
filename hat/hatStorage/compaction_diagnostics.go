package hatStorage

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
	"unicode/utf8"
)

const (
	DefaultCompactionDiagnosticsMaxArrangements         = 256
	DefaultCompactionDiagnosticsHistoryPerArrangement   = 8
	DefaultCompactionDiagnosticsMaxArrangementNameBytes = 128

	maxCompactionDiagnosticsArrangements          = 4096
	maxCompactionDiagnosticsHistoryPerArrangement = 1024
	maxCompactionDiagnosticsNameBytes             = 4096
)

var (
	ErrCompactionDiagnosticsInvalid           = errors.New("invalid compaction diagnostics input")
	ErrCompactionDiagnosticsAlreadyRegistered = errors.New("compaction arrangement already registered")
	ErrCompactionDiagnosticsCapacity          = errors.New("compaction diagnostics capacity reached")
	ErrCompactionDiagnosticsUnregistered      = errors.New("compaction arrangement is not registered")
)

// CompactionOutcome describes whether one compaction completed successfully.
type CompactionOutcome uint8

const (
	CompactionSucceeded CompactionOutcome = iota
	CompactionFailed
)

// CompactionDiagnosticsOptions bounds the memory retained by a diagnostics
// registry. Zero values select the documented defaults.
type CompactionDiagnosticsOptions struct {
	MaxArrangements         int
	HistoryPerArrangement   int
	MaxArrangementNameBytes int
}

// CompactionObservation is one caller-supplied compaction measurement.
// CompactionDebtBytes is deliberately caller-defined because storage engines
// differ in how they count obsolete, overlapping, or queued bytes.
type CompactionObservation struct {
	Arrangement         string
	LogicalBytes        uint64
	PhysicalBytes       uint64
	CompactionDebtBytes uint64
	InputBytes          uint64
	OutputBytes         uint64
	Duration            time.Duration
	Outcome             CompactionOutcome
}

// CompactionSample is a detached historical observation returned by a
// diagnostics snapshot.
type CompactionSample struct {
	Sequence            uint64            `json:"sequence"`
	LogicalBytes        uint64            `json:"logical_bytes"`
	PhysicalBytes       uint64            `json:"physical_bytes"`
	CompactionDebtBytes uint64            `json:"compaction_debt_bytes"`
	InputBytes          uint64            `json:"input_bytes"`
	OutputBytes         uint64            `json:"output_bytes"`
	Duration            time.Duration     `json:"duration"`
	Outcome             CompactionOutcome `json:"outcome"`
}

// CompactionArrangementDiagnostics is a deterministic, detached summary for
// one registered arrangement.
type CompactionArrangementDiagnostics struct {
	Arrangement           string             `json:"arrangement"`
	TotalObservations     uint64             `json:"total_observations"`
	SuccessfulCompactions uint64             `json:"successful_compactions"`
	FailedCompactions     uint64             `json:"failed_compactions"`
	Last                  CompactionSample   `json:"last"`
	History               []CompactionSample `json:"history"`
}

type compactionDiagnosticsEntry struct {
	arrangement                string
	totalObservations          uint64
	successfulCompactions      uint64
	failedCompactions          uint64
	totalInputBytes            uint64
	totalOutputBytes           uint64
	currentCompactionDebtBytes uint64
	last                       CompactionSample
	history                    []CompactionSample
	nextHistoryIndex           int
	historyCount               int
}

// CompactionDiagnostics is an opt-in bounded registry of per-arrangement
// compaction history. Register all expected arrangements before recording to
// keep the steady-state Record path allocation-free.
type CompactionDiagnostics struct {
	mu                      sync.RWMutex
	maxArrangements         int
	historyPerArrangement   int
	maxArrangementNameBytes int
	entries                 map[string]*compactionDiagnosticsEntry
	nextSequence            uint64
}

// NewCompactionDiagnostics creates a bounded per-arrangement registry.
func NewCompactionDiagnostics(options CompactionDiagnosticsOptions) (*CompactionDiagnostics, error) {
	if options.MaxArrangements < 0 || options.HistoryPerArrangement < 0 || options.MaxArrangementNameBytes < 0 {
		return nil, ErrCompactionDiagnosticsInvalid
	}
	if options.MaxArrangements == 0 {
		options.MaxArrangements = DefaultCompactionDiagnosticsMaxArrangements
	}
	if options.HistoryPerArrangement == 0 {
		options.HistoryPerArrangement = DefaultCompactionDiagnosticsHistoryPerArrangement
	}
	if options.MaxArrangementNameBytes == 0 {
		options.MaxArrangementNameBytes = DefaultCompactionDiagnosticsMaxArrangementNameBytes
	}
	if options.MaxArrangements > maxCompactionDiagnosticsArrangements ||
		options.HistoryPerArrangement > maxCompactionDiagnosticsHistoryPerArrangement ||
		options.MaxArrangementNameBytes > maxCompactionDiagnosticsNameBytes {
		return nil, ErrCompactionDiagnosticsInvalid
	}
	return &CompactionDiagnostics{
		maxArrangements:         options.MaxArrangements,
		historyPerArrangement:   options.HistoryPerArrangement,
		maxArrangementNameBytes: options.MaxArrangementNameBytes,
		entries:                 make(map[string]*compactionDiagnosticsEntry, options.MaxArrangements),
	}, nil
}

// Register reserves one arrangement label. Registration is explicit so an
// unexpected label cannot silently grow the registry or evict useful history.
func (diagnostics *CompactionDiagnostics) Register(arrangement string) error {
	if diagnostics == nil {
		return ErrCompactionDiagnosticsInvalid
	}
	if err := diagnostics.validateArrangement(arrangement); err != nil {
		return err
	}
	diagnostics.mu.Lock()
	defer diagnostics.mu.Unlock()
	if _, exists := diagnostics.entries[arrangement]; exists {
		return ErrCompactionDiagnosticsAlreadyRegistered
	}
	if len(diagnostics.entries) >= diagnostics.maxArrangements {
		return ErrCompactionDiagnosticsCapacity
	}
	diagnostics.entries[arrangement] = &compactionDiagnosticsEntry{
		arrangement: arrangement,
		history:     make([]CompactionSample, diagnostics.historyPerArrangement),
	}
	return nil
}

// Unregister removes one arrangement and releases its retained history.
func (diagnostics *CompactionDiagnostics) Unregister(arrangement string) bool {
	if diagnostics == nil {
		return false
	}
	diagnostics.mu.Lock()
	defer diagnostics.mu.Unlock()
	if _, exists := diagnostics.entries[arrangement]; !exists {
		return false
	}
	delete(diagnostics.entries, arrangement)
	return true
}

// Record appends one bounded observation to a registered arrangement.
func (diagnostics *CompactionDiagnostics) Record(observation CompactionObservation) error {
	if diagnostics == nil {
		return ErrCompactionDiagnosticsInvalid
	}
	if err := diagnostics.validateObservation(observation); err != nil {
		return err
	}
	diagnostics.mu.Lock()
	defer diagnostics.mu.Unlock()
	entry, exists := diagnostics.entries[observation.Arrangement]
	if !exists {
		return ErrCompactionDiagnosticsUnregistered
	}
	if diagnostics.nextSequence == ^uint64(0) {
		return fmt.Errorf("%w: sequence exhausted", ErrCompactionDiagnosticsCapacity)
	}
	diagnostics.nextSequence++
	sample := CompactionSample{
		Sequence:            diagnostics.nextSequence,
		LogicalBytes:        observation.LogicalBytes,
		PhysicalBytes:       observation.PhysicalBytes,
		CompactionDebtBytes: observation.CompactionDebtBytes,
		InputBytes:          observation.InputBytes,
		OutputBytes:         observation.OutputBytes,
		Duration:            observation.Duration,
		Outcome:             observation.Outcome,
	}
	entry.last = sample
	entry.totalObservations = saturatingCompactionDiagnosticsIncrement(entry.totalObservations)
	entry.totalInputBytes = saturatingCompactionDiagnosticsAdd(entry.totalInputBytes, observation.InputBytes)
	entry.totalOutputBytes = saturatingCompactionDiagnosticsAdd(entry.totalOutputBytes, observation.OutputBytes)
	entry.currentCompactionDebtBytes = observation.CompactionDebtBytes
	if observation.Outcome == CompactionSucceeded {
		entry.successfulCompactions = saturatingCompactionDiagnosticsIncrement(entry.successfulCompactions)
	} else {
		entry.failedCompactions = saturatingCompactionDiagnosticsIncrement(entry.failedCompactions)
	}
	if entry.historyCount < len(entry.history) {
		entry.history[entry.historyCount] = sample
		entry.historyCount++
	} else if len(entry.history) > 0 {
		entry.history[entry.nextHistoryIndex] = sample
		entry.nextHistoryIndex = (entry.nextHistoryIndex + 1) % len(entry.history)
	}
	return nil
}

// Snapshot returns sorted detached summaries. Mutating the result cannot
// change the registry or future snapshots.
func (diagnostics *CompactionDiagnostics) Snapshot() []CompactionArrangementDiagnostics {
	if diagnostics == nil {
		return nil
	}
	diagnostics.mu.RLock()
	defer diagnostics.mu.RUnlock()
	arrangements := make([]string, 0, len(diagnostics.entries))
	for arrangement := range diagnostics.entries {
		arrangements = append(arrangements, arrangement)
	}
	sort.Strings(arrangements)
	snapshots := make([]CompactionArrangementDiagnostics, 0, len(arrangements))
	for _, arrangement := range arrangements {
		entry := diagnostics.entries[arrangement]
		snapshot := CompactionArrangementDiagnostics{
			Arrangement:           entry.arrangement,
			TotalObservations:     entry.totalObservations,
			SuccessfulCompactions: entry.successfulCompactions,
			FailedCompactions:     entry.failedCompactions,
			Last:                  entry.last,
			History:               diagnostics.copyHistory(entry),
		}
		snapshots = append(snapshots, snapshot)
	}
	return snapshots
}

// History returns the oldest-to-newest detached history for one arrangement.
func (diagnostics *CompactionDiagnostics) History(arrangement string) ([]CompactionSample, error) {
	if diagnostics == nil {
		return nil, ErrCompactionDiagnosticsInvalid
	}
	if err := diagnostics.validateArrangement(arrangement); err != nil {
		return nil, err
	}
	diagnostics.mu.RLock()
	defer diagnostics.mu.RUnlock()
	entry, exists := diagnostics.entries[arrangement]
	if !exists {
		return nil, ErrCompactionDiagnosticsUnregistered
	}
	return diagnostics.copyHistory(entry), nil
}

// Len returns the number of registered arrangements.
func (diagnostics *CompactionDiagnostics) Len() int {
	if diagnostics == nil {
		return 0
	}
	diagnostics.mu.RLock()
	defer diagnostics.mu.RUnlock()
	return len(diagnostics.entries)
}

func (diagnostics *CompactionDiagnostics) validateArrangement(arrangement string) error {
	if arrangement == "" || len(arrangement) > diagnostics.maxArrangementNameBytes || !utf8.ValidString(arrangement) {
		return ErrCompactionDiagnosticsInvalid
	}
	return nil
}

func (diagnostics *CompactionDiagnostics) validateObservation(observation CompactionObservation) error {
	if err := diagnostics.validateArrangement(observation.Arrangement); err != nil {
		return err
	}
	if observation.Duration < 0 || observation.Outcome > CompactionFailed {
		return ErrCompactionDiagnosticsInvalid
	}
	return nil
}

func (diagnostics *CompactionDiagnostics) copyHistory(entry *compactionDiagnosticsEntry) []CompactionSample {
	if entry.historyCount == 0 {
		return nil
	}
	history := make([]CompactionSample, entry.historyCount)
	if entry.historyCount < len(entry.history) {
		copy(history, entry.history[:entry.historyCount])
		return history
	}
	for index := range history {
		history[index] = entry.history[(entry.nextHistoryIndex+index)%len(entry.history)]
	}
	return history
}

func saturatingCompactionDiagnosticsIncrement(value uint64) uint64 {
	return saturatingCompactionDiagnosticsAdd(value, 1)
}

func saturatingCompactionDiagnosticsAdd(left, right uint64) uint64 {
	if ^uint64(0)-left < right {
		return ^uint64(0)
	}
	return left + right
}
