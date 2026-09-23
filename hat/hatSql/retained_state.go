package hatSql

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
)

const (
	// DefaultSQLRetainedStateMaxFrontiers bounds the number of exact logical
	// frontiers retained by a state when the caller leaves it unspecified.
	DefaultSQLRetainedStateMaxFrontiers = 64
	// DefaultSQLRetainedStateMaxSources bounds distinct source/name pairs.
	DefaultSQLRetainedStateMaxSources = 1024
	// DefaultSQLRetainedStateMaxRows bounds the total rows in the current view.
	DefaultSQLRetainedStateMaxRows = 1_000_000
)

var (
	ErrSQLRetainedStateNil                 = errors.New("hatSql: retained state is nil")
	ErrSQLRetainedStateOptionInvalid       = errors.New("hatSql: retained state option is invalid")
	ErrSQLRetainedStateFrontierInvalid     = errors.New("hatSql: retained state frontier is invalid")
	ErrSQLRetainedStateFrontierRegression  = errors.New("hatSql: retained state frontier moved backwards")
	ErrSQLRetainedStateFrontierUnavailable = errors.New("hatSql: retained state frontier is unavailable")
	ErrSQLRetainedStateSourceInvalid       = errors.New("hatSql: retained state source is invalid")
	ErrSQLRetainedStateDuplicateSource     = errors.New("hatSql: retained state source is duplicated")
	ErrSQLRetainedStateLimit               = errors.New("hatSql: retained state limit exceeded")
)

// SQLRetainedStateOptions bounds the in-memory exact historical views kept by
// SQLRetainedState. Zero selects the documented bounded defaults.
type SQLRetainedStateOptions struct {
	MaxFrontiers int
	MaxSources   int
	MaxRows      int
}

type normalizedSQLRetainedStateOptions struct {
	maxFrontiers int
	maxSources   int
	maxRows      int
}

// SQLRetainedStateSource replaces one named SQL source at a new logical
// frontier. Sources omitted from a publish remain unchanged.
type SQLRetainedStateSource struct {
	Name string
	Key  string
	Rows []Row
}

// SQLRetainedStateSnapshot is immutable metadata about retained state. It
// does not expose source rows or internal snapshot maps.
type SQLRetainedStateSnapshot struct {
	EarliestFrontier uint64
	LatestFrontier   uint64
	HistoryFrontiers int
	Sources          int
	Rows             int
}

type sqlRetainedStateSourceKey struct {
	name string
	key  string
}

type sqlRetainedStateHistory struct {
	frontier uint64
	sources  map[sqlRetainedStateSourceKey][]Row
	rows     int
}

// SQLRetainedState is a bounded, copy-on-write logical source state. Each
// published frontier owns an immutable source index; unchanged source row
// slices are shared between adjacent versions, while changed input rows are
// copied at the publish boundary.
//
// It implements SQLSourceResolver, HistoricalSourceResolver,
// SQLFrontierSnapshotProvider, and SQLSourceFrontierResolver, so it can be
// passed directly to the SQL query APIs.
type SQLRetainedState struct {
	mu      sync.RWMutex
	options normalizedSQLRetainedStateOptions
	current map[sqlRetainedStateSourceKey][]Row
	history []sqlRetainedStateHistory
	latest  uint64
	rows    int
}

// NewSQLRetainedState creates an empty retained state. Frontier zero is the
// immutable empty initial view; the first published frontier must be positive.
func NewSQLRetainedState(options SQLRetainedStateOptions) (*SQLRetainedState, error) {
	normalized, err := normalizeSQLRetainedStateOptions(options)
	if err != nil {
		return nil, err
	}
	return &SQLRetainedState{
		options: normalized,
		current: make(map[sqlRetainedStateSourceKey][]Row),
	}, nil
}

// Publish atomically records source replacements at a strictly newer logical
// frontier. On validation failure neither current state nor history changes.
func (state *SQLRetainedState) Publish(frontier uint64, sources []SQLRetainedStateSource) error {
	if state == nil {
		return ErrSQLRetainedStateNil
	}
	if frontier == 0 {
		return ErrSQLRetainedStateFrontierInvalid
	}
	normalized, err := normalizeSQLRetainedStateSources(sources)
	if err != nil {
		return err
	}

	state.mu.Lock()
	defer state.mu.Unlock()
	if frontier <= state.latest {
		return fmt.Errorf("%w: got %d after %d", ErrSQLRetainedStateFrontierRegression, frontier, state.latest)
	}
	next := make(map[sqlRetainedStateSourceKey][]Row, len(state.current)+len(normalized))
	for key, rows := range state.current {
		next[key] = rows
	}
	for _, source := range normalized {
		key := sqlRetainedStateSourceKey{name: source.Name, key: source.Key}
		next[key] = cloneSQLRows(source.Rows)
	}
	if len(next) > state.options.maxSources {
		return fmt.Errorf("%w: sources=%d max=%d", ErrSQLRetainedStateLimit, len(next), state.options.maxSources)
	}
	totalRows := retainedStateRowCount(next)
	if totalRows > state.options.maxRows {
		return fmt.Errorf("%w: rows=%d max=%d", ErrSQLRetainedStateLimit, totalRows, state.options.maxRows)
	}

	state.current = next
	state.latest = frontier
	state.rows = totalRows
	state.history = append(state.history, sqlRetainedStateHistory{
		frontier: frontier,
		sources:  next,
		rows:     totalRows,
	})
	if overflow := len(state.history) - state.options.maxFrontiers; overflow > 0 {
		state.history = state.history[overflow:]
	}
	return nil
}

// ResolveSQLSource returns an independent copy of the current source rows.
func (state *SQLRetainedState) ResolveSQLSource(name, key string) ([]Row, error) {
	if state == nil {
		return nil, ErrSQLRetainedStateNil
	}
	sourceKey, err := normalizeSQLRetainedStateSourceKey(name, key)
	if err != nil {
		return nil, err
	}
	state.mu.RLock()
	rows := state.current[sourceKey]
	state.mu.RUnlock()
	return cloneSQLRows(rows), nil
}

// BorrowSQLSource supplies an immutable internal version to the SQL executor.
// Callers must not mutate or retain the returned rows; public resolver calls
// should use ResolveSQLSource when an independent copy is required.
func (state *SQLRetainedState) BorrowSQLSource(name, key string) ([]Row, bool, error) {
	if state == nil {
		return nil, false, ErrSQLRetainedStateNil
	}
	sourceKey, err := normalizeSQLRetainedStateSourceKey(name, key)
	if err != nil {
		return nil, false, err
	}
	state.mu.RLock()
	rows, found := state.current[sourceKey]
	state.mu.RUnlock()
	return rows, found, nil
}

// ResolveSQLSourceAt returns an independent copy of one source at an exact
// retained frontier.
func (state *SQLRetainedState) ResolveSQLSourceAt(name, key string, frontier uint64) ([]Row, error) {
	resolver, release, err := state.BeginSQLSnapshotAt(context.Background(), frontier)
	if err != nil {
		return nil, err
	}
	if release != nil {
		defer release()
	}
	return resolver.ResolveSQLSource(name, key)
}

// BeginSQLSnapshotAt opens an immutable resolver bound to frontier. Frontier
// zero is the empty initial view; nonzero frontiers must still be retained.
func (state *SQLRetainedState) BeginSQLSnapshotAt(ctx context.Context, frontier uint64) (SQLSourceResolver, func(), error) {
	if state == nil {
		return nil, nil, ErrSQLRetainedStateNil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	state.mu.RLock()
	var sources map[sqlRetainedStateSourceKey][]Row
	if frontier == 0 {
		sources = make(map[sqlRetainedStateSourceKey][]Row)
	} else {
		index := sort.Search(len(state.history), func(index int) bool {
			return state.history[index].frontier >= frontier
		})
		if index < len(state.history) && state.history[index].frontier == frontier {
			sources = state.history[index].sources
		}
	}
	state.mu.RUnlock()
	if sources == nil {
		return nil, nil, fmt.Errorf("%w: %d", ErrSQLRetainedStateFrontierUnavailable, frontier)
	}
	return &sqlRetainedStateSnapshotResolver{sources: sources, frontier: frontier}, func() {}, nil
}

// SQLSourceFrontier reports the newest published frontier. An absent source is
// a valid empty source once the state has a published frontier.
func (state *SQLRetainedState) SQLSourceFrontier(name, key string) (frontier uint64, ready, available bool, err error) {
	if state == nil {
		return 0, false, false, ErrSQLRetainedStateNil
	}
	if _, err := normalizeSQLRetainedStateSourceKey(name, key); err != nil {
		return 0, false, false, err
	}
	state.mu.RLock()
	defer state.mu.RUnlock()
	if state.latest == 0 {
		return 0, false, true, nil
	}
	return state.latest, true, true, nil
}

// Snapshot returns immutable retained-state metadata.
func (state *SQLRetainedState) Snapshot() SQLRetainedStateSnapshot {
	if state == nil {
		return SQLRetainedStateSnapshot{}
	}
	state.mu.RLock()
	defer state.mu.RUnlock()
	snapshot := SQLRetainedStateSnapshot{
		LatestFrontier:   state.latest,
		HistoryFrontiers: len(state.history),
		Sources:          len(state.current),
		Rows:             state.rows,
	}
	if len(state.history) > 0 {
		snapshot.EarliestFrontier = state.history[0].frontier
	}
	return snapshot
}

type sqlRetainedStateSnapshotResolver struct {
	sources  map[sqlRetainedStateSourceKey][]Row
	frontier uint64
}

func (resolver *sqlRetainedStateSnapshotResolver) ResolveSQLSource(name, key string) ([]Row, error) {
	if resolver == nil {
		return nil, ErrSQLRetainedStateNil
	}
	sourceKey, err := normalizeSQLRetainedStateSourceKey(name, key)
	if err != nil {
		return nil, err
	}
	return cloneSQLRows(resolver.sources[sourceKey]), nil
}

func (resolver *sqlRetainedStateSnapshotResolver) BorrowSQLSource(name, key string) ([]Row, bool, error) {
	if resolver == nil {
		return nil, false, ErrSQLRetainedStateNil
	}
	sourceKey, err := normalizeSQLRetainedStateSourceKey(name, key)
	if err != nil {
		return nil, false, err
	}
	rows, found := resolver.sources[sourceKey]
	return rows, found, nil
}

func (resolver *sqlRetainedStateSnapshotResolver) SQLSourceFrontier(name, key string) (frontier uint64, ready, available bool, err error) {
	if resolver == nil {
		return 0, false, false, ErrSQLRetainedStateNil
	}
	if _, err := normalizeSQLRetainedStateSourceKey(name, key); err != nil {
		return 0, false, false, err
	}
	return resolver.frontier, true, true, nil
}

func normalizeSQLRetainedStateOptions(options SQLRetainedStateOptions) (normalizedSQLRetainedStateOptions, error) {
	if options.MaxFrontiers < 0 || options.MaxSources < 0 || options.MaxRows < 0 {
		return normalizedSQLRetainedStateOptions{}, ErrSQLRetainedStateOptionInvalid
	}
	if options.MaxFrontiers == 0 {
		options.MaxFrontiers = DefaultSQLRetainedStateMaxFrontiers
	}
	if options.MaxSources == 0 {
		options.MaxSources = DefaultSQLRetainedStateMaxSources
	}
	if options.MaxRows == 0 {
		options.MaxRows = DefaultSQLRetainedStateMaxRows
	}
	return normalizedSQLRetainedStateOptions{
		maxFrontiers: options.MaxFrontiers,
		maxSources:   options.MaxSources,
		maxRows:      options.MaxRows,
	}, nil
}

func normalizeSQLRetainedStateSources(sources []SQLRetainedStateSource) ([]SQLRetainedStateSource, error) {
	if len(sources) == 0 {
		return nil, nil
	}
	normalized := make([]SQLRetainedStateSource, len(sources))
	seen := make(map[sqlRetainedStateSourceKey]struct{}, len(sources))
	for index, source := range sources {
		key, err := normalizeSQLRetainedStateSourceKey(source.Name, source.Key)
		if err != nil {
			return nil, err
		}
		if _, found := seen[key]; found {
			return nil, fmt.Errorf("%w: %s/%s", ErrSQLRetainedStateDuplicateSource, key.name, key.key)
		}
		seen[key] = struct{}{}
		normalized[index] = SQLRetainedStateSource{Name: key.name, Key: key.key, Rows: source.Rows}
	}
	return normalized, nil
}

func normalizeSQLRetainedStateSourceKey(name, key string) (sqlRetainedStateSourceKey, error) {
	name = strings.TrimSpace(name)
	key = strings.TrimSpace(key)
	if name == "" || key == "" {
		return sqlRetainedStateSourceKey{}, ErrSQLRetainedStateSourceInvalid
	}
	return sqlRetainedStateSourceKey{name: name, key: key}, nil
}

func retainedStateRowCount(sources map[sqlRetainedStateSourceKey][]Row) int {
	count := 0
	for _, rows := range sources {
		count += len(rows)
	}
	return count
}
