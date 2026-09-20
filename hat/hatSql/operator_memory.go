package hatSql

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
)

// ErrSQLOperatorMemoryLimit indicates that an observed operator exceeded the
// per-operator limit configured on its tracker.
var ErrSQLOperatorMemoryLimit = errors.New("hatSql: SQL operator memory limit exceeded")

// ErrSQLOperatorMemoryInvalid indicates malformed tracker input.
var ErrSQLOperatorMemoryInvalid = errors.New("hatSql: invalid SQL operator memory sample")

// SQLOperatorMemoryStats is the bounded, privacy-safe memory view for one
// SQL operator. A tracker is intended to be created for one query execution.
type SQLOperatorMemoryStats struct {
	Operator     string `json:"operator"`
	CurrentBytes int    `json:"current_bytes"`
	PeakBytes    int    `json:"peak_bytes"`
	Samples      uint64 `json:"samples"`
	Rejected     uint64 `json:"rejected"`
	LimitBytes   int    `json:"limit_bytes,omitempty"`
}

type sqlOperatorMemoryState struct {
	current  int
	peak     int
	samples  uint64
	rejected uint64
}

// SQLOperatorMemoryTracker records estimated retained working bytes for
// operators that opt into SQLQueryOptions.OperatorMemoryTracker. A positive
// limit applies independently to each operator; zero records without a cap.
// The tracker is safe to inspect from a concurrent observer, but callers
// should use one tracker per query so CurrentBytes has unambiguous ownership.
type SQLOperatorMemoryTracker struct {
	mu         sync.Mutex
	limitBytes int
	operators  map[string]sqlOperatorMemoryState
}

// NewSQLOperatorMemoryTracker creates an opt-in per-operator tracker.
func NewSQLOperatorMemoryTracker(limitBytes int) (*SQLOperatorMemoryTracker, error) {
	if limitBytes < 0 {
		return nil, fmt.Errorf("%w: limit cannot be negative", ErrSQLOperatorMemoryInvalid)
	}
	return &SQLOperatorMemoryTracker{
		limitBytes: limitBytes,
		operators:  make(map[string]sqlOperatorMemoryState),
	}, nil
}

// Observe records the current estimated retained bytes for one operator. A
// rejected sample is retained in the snapshot and returns a typed sentinel
// error so callers can enforce a local budget without parsing text.
func (tracker *SQLOperatorMemoryTracker) Observe(operator string, currentBytes int) error {
	if tracker == nil {
		return nil
	}
	operator = strings.TrimSpace(operator)
	if operator == "" || currentBytes < 0 {
		return fmt.Errorf("%w: operator and non-negative bytes are required", ErrSQLOperatorMemoryInvalid)
	}
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	state := tracker.operators[operator]
	state.samples++
	if currentBytes > state.peak {
		state.peak = currentBytes
	}
	if tracker.limitBytes > 0 && currentBytes > tracker.limitBytes {
		state.rejected++
		state.current = 0
		tracker.operators[operator] = state
		return fmt.Errorf("%w: operator %q observed %d bytes, maximum %d", ErrSQLOperatorMemoryLimit, operator, currentBytes, tracker.limitBytes)
	}
	state.current = currentBytes
	tracker.operators[operator] = state
	return nil
}

// Release marks the operator as no longer retaining its observed working
// set. Peak and sample counters remain available for the query report.
func (tracker *SQLOperatorMemoryTracker) Release(operator string) {
	if tracker == nil {
		return
	}
	operator = strings.TrimSpace(operator)
	if operator == "" {
		return
	}
	tracker.mu.Lock()
	state, exists := tracker.operators[operator]
	if exists {
		state.current = 0
		tracker.operators[operator] = state
	}
	tracker.mu.Unlock()
}

// Snapshot returns deterministic independent copies of all observed operator
// counters.
func (tracker *SQLOperatorMemoryTracker) Snapshot() []SQLOperatorMemoryStats {
	if tracker == nil {
		return nil
	}
	tracker.mu.Lock()
	result := make([]SQLOperatorMemoryStats, 0, len(tracker.operators))
	for operator, state := range tracker.operators {
		result = append(result, SQLOperatorMemoryStats{
			Operator:     operator,
			CurrentBytes: state.current,
			PeakBytes:    state.peak,
			Samples:      state.samples,
			Rejected:     state.rejected,
			LimitBytes:   tracker.limitBytes,
		})
	}
	tracker.mu.Unlock()
	sort.Slice(result, func(left, right int) bool { return result[left].Operator < result[right].Operator })
	return result
}
