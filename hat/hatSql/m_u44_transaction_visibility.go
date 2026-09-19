package hatSql

import (
	"errors"
	"sort"
	"strings"
	"sync"
)

const (
	// DefaultSQLDataflowVisibilityMaxDataflows bounds the number of named
	// dataflows tracked by a coordinator when no limit is configured.
	DefaultSQLDataflowVisibilityMaxDataflows = 1024
)

var (
	// ErrSQLDataflowVisibilityNil reports a nil coordinator.
	ErrSQLDataflowVisibilityNil = errors.New("SQL dataflow visibility coordinator is nil")
	// ErrSQLDataflowVisibilityInvalid reports malformed names or snapshots.
	ErrSQLDataflowVisibilityInvalid = errors.New("SQL dataflow visibility input is invalid")
	// ErrSQLDataflowVisibilityDuplicate reports duplicate names in one batch or snapshot.
	ErrSQLDataflowVisibilityDuplicate = errors.New("SQL dataflow visibility name is duplicated")
	// ErrSQLDataflowVisibilityUnknown reports a name that was not registered.
	ErrSQLDataflowVisibilityUnknown = errors.New("SQL dataflow visibility name is unknown")
	// ErrSQLDataflowVisibilityCapacity reports a configured dataflow limit.
	ErrSQLDataflowVisibilityCapacity = errors.New("SQL dataflow visibility capacity reached")
	// ErrSQLDataflowVisibilityNotReady reports a mixed-version observation.
	ErrSQLDataflowVisibilityNotReady = errors.New("SQL dataflow visibility is not at one common version")
	// ErrSQLDataflowVisibilityStale reports a token that no longer matches the current version.
	ErrSQLDataflowVisibilityStale = errors.New("SQL dataflow visibility token is stale")
	// ErrSQLDataflowVisibilityOverflow reports exhausted logical versions.
	ErrSQLDataflowVisibilityOverflow = errors.New("SQL dataflow visibility version exhausted")
)

// SQLDataflowVisibilityOptions configures a visibility coordinator.
//
// A zero MaxDataflows uses DefaultSQLDataflowVisibilityMaxDataflows. The
// coordinator is opt-in: constructing one does not change any SQL execution
// or materialized-view behavior until the caller uses its methods.
type SQLDataflowVisibilityOptions struct {
	MaxDataflows int
}

// SQLDataflowVisibilityToken identifies one common version and the dataflows
// that were observed at that version.
//
// Dataflows is sorted and owned by the token. Callers may retain or modify a
// token without changing coordinator state.
type SQLDataflowVisibilityToken struct {
	Version   uint64
	Dataflows []string
}

// SQLDataflowVisibilityEntry is one dataflow version in a snapshot.
type SQLDataflowVisibilityEntry struct {
	Name    string
	Version uint64
}

// SQLDataflowVisibilitySnapshot is an atomic, deterministic coordinator
// checkpoint. It records no rows or payloads.
type SQLDataflowVisibilitySnapshot struct {
	NextVersion uint64
	Dataflows   []SQLDataflowVisibilityEntry
}

// SQLDataflowVisibilityCoordinator assigns a shared logical version to an
// explicitly supplied batch of maintained dataflows.
//
// Publish is a publication barrier, not a SQL transaction: callers must
// finish preparing each named view before publishing the batch. Acquire and
// Check only validate that the named views still have one common current
// version; they do not retain historical row snapshots.
type SQLDataflowVisibilityCoordinator struct {
	mu           sync.RWMutex
	maxDataflows int
	nextVersion  uint64
	dataflows    map[string]uint64
}

// NewSQLDataflowVisibilityCoordinator creates a bounded visibility
// coordinator.
func NewSQLDataflowVisibilityCoordinator(options SQLDataflowVisibilityOptions) (*SQLDataflowVisibilityCoordinator, error) {
	if options.MaxDataflows < 0 {
		return nil, ErrSQLDataflowVisibilityInvalid
	}
	maxDataflows := options.MaxDataflows
	if maxDataflows == 0 {
		maxDataflows = DefaultSQLDataflowVisibilityMaxDataflows
	}
	return &SQLDataflowVisibilityCoordinator{
		maxDataflows: maxDataflows,
		dataflows:    make(map[string]uint64),
	}, nil
}

// Register adds a dataflow at version zero. Registering an existing name is
// idempotent and does not reset its current version.
func (coordinator *SQLDataflowVisibilityCoordinator) Register(name string) error {
	if coordinator == nil {
		return ErrSQLDataflowVisibilityNil
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return ErrSQLDataflowVisibilityInvalid
	}

	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	if _, found := coordinator.dataflows[name]; found {
		return nil
	}
	if len(coordinator.dataflows) >= coordinator.maxDataflowLimitLocked() {
		return ErrSQLDataflowVisibilityCapacity
	}
	if coordinator.dataflows == nil {
		coordinator.dataflows = make(map[string]uint64)
	}
	coordinator.dataflows[name] = 0
	return nil
}

// Publish atomically advances every named dataflow to one new logical
// version. Names are normalized, sorted, and required to be distinct.
func (coordinator *SQLDataflowVisibilityCoordinator) Publish(names []string) (SQLDataflowVisibilityToken, error) {
	if coordinator == nil {
		return SQLDataflowVisibilityToken{}, ErrSQLDataflowVisibilityNil
	}
	normalized, err := normalizeSQLDataflowVisibilityNames(names, coordinator.maxDataflowLimit())
	if err != nil {
		return SQLDataflowVisibilityToken{}, err
	}

	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	for _, name := range normalized {
		if _, found := coordinator.dataflows[name]; !found {
			return SQLDataflowVisibilityToken{}, ErrSQLDataflowVisibilityUnknown
		}
	}
	if coordinator.nextVersion == ^uint64(0) {
		return SQLDataflowVisibilityToken{}, ErrSQLDataflowVisibilityOverflow
	}
	coordinator.nextVersion++
	for _, name := range normalized {
		coordinator.dataflows[name] = coordinator.nextVersion
	}
	return SQLDataflowVisibilityToken{Version: coordinator.nextVersion, Dataflows: normalized}, nil
}

// Acquire returns a token only when all named dataflows currently share one
// version. Version zero is valid for newly registered dataflows.
func (coordinator *SQLDataflowVisibilityCoordinator) Acquire(names []string) (SQLDataflowVisibilityToken, error) {
	if coordinator == nil {
		return SQLDataflowVisibilityToken{}, ErrSQLDataflowVisibilityNil
	}
	normalized, err := normalizeSQLDataflowVisibilityNames(names, coordinator.maxDataflowLimit())
	if err != nil {
		return SQLDataflowVisibilityToken{}, err
	}

	coordinator.mu.RLock()
	defer coordinator.mu.RUnlock()
	version := uint64(0)
	for index, name := range normalized {
		current, found := coordinator.dataflows[name]
		if !found {
			return SQLDataflowVisibilityToken{}, ErrSQLDataflowVisibilityUnknown
		}
		if index == 0 {
			version = current
			continue
		}
		if current != version {
			return SQLDataflowVisibilityToken{}, ErrSQLDataflowVisibilityNotReady
		}
	}
	return SQLDataflowVisibilityToken{Version: version, Dataflows: normalized}, nil
}

// Check verifies that a token still describes the exact current version of
// each named dataflow. A later publish makes the token stale.
func (coordinator *SQLDataflowVisibilityCoordinator) Check(token SQLDataflowVisibilityToken) error {
	if coordinator == nil {
		return ErrSQLDataflowVisibilityNil
	}
	normalized, err := normalizeSQLDataflowVisibilityNames(token.Dataflows, coordinator.maxDataflowLimit())
	if err != nil {
		return err
	}

	coordinator.mu.RLock()
	defer coordinator.mu.RUnlock()
	for _, name := range normalized {
		current, found := coordinator.dataflows[name]
		if !found {
			return ErrSQLDataflowVisibilityUnknown
		}
		if current != token.Version {
			return ErrSQLDataflowVisibilityStale
		}
	}
	return nil
}

// Version returns the current version for one registered dataflow.
func (coordinator *SQLDataflowVisibilityCoordinator) Version(name string) (uint64, bool) {
	if coordinator == nil {
		return 0, false
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return 0, false
	}
	coordinator.mu.RLock()
	defer coordinator.mu.RUnlock()
	version, found := coordinator.dataflows[name]
	return version, found
}

// Snapshot returns a deterministic, independently owned checkpoint.
func (coordinator *SQLDataflowVisibilityCoordinator) Snapshot() SQLDataflowVisibilitySnapshot {
	if coordinator == nil {
		return SQLDataflowVisibilitySnapshot{}
	}
	coordinator.mu.RLock()
	defer coordinator.mu.RUnlock()
	dataflows := make([]SQLDataflowVisibilityEntry, 0, len(coordinator.dataflows))
	for name, version := range coordinator.dataflows {
		dataflows = append(dataflows, SQLDataflowVisibilityEntry{Name: name, Version: version})
	}
	sort.Slice(dataflows, func(left, right int) bool {
		return dataflows[left].Name < dataflows[right].Name
	})
	return SQLDataflowVisibilitySnapshot{NextVersion: coordinator.nextVersion, Dataflows: dataflows}
}

// Restore atomically replaces coordinator state after validating names,
// versions, duplicates, and the configured capacity.
func (coordinator *SQLDataflowVisibilityCoordinator) Restore(snapshot SQLDataflowVisibilitySnapshot) error {
	if coordinator == nil {
		return ErrSQLDataflowVisibilityNil
	}
	if len(snapshot.Dataflows) > coordinator.maxDataflowLimit() {
		return ErrSQLDataflowVisibilityCapacity
	}
	replacement := make(map[string]uint64, len(snapshot.Dataflows))
	for _, entry := range snapshot.Dataflows {
		name := strings.TrimSpace(entry.Name)
		if name == "" || entry.Version > snapshot.NextVersion {
			return ErrSQLDataflowVisibilityInvalid
		}
		if _, found := replacement[name]; found {
			return ErrSQLDataflowVisibilityDuplicate
		}
		replacement[name] = entry.Version
	}

	coordinator.mu.Lock()
	coordinator.dataflows = replacement
	coordinator.nextVersion = snapshot.NextVersion
	coordinator.mu.Unlock()
	return nil
}

func (coordinator *SQLDataflowVisibilityCoordinator) maxDataflowLimit() int {
	coordinator.mu.RLock()
	defer coordinator.mu.RUnlock()
	return coordinator.maxDataflowLimitLocked()
}

func (coordinator *SQLDataflowVisibilityCoordinator) maxDataflowLimitLocked() int {
	if coordinator.maxDataflows > 0 {
		return coordinator.maxDataflows
	}
	return DefaultSQLDataflowVisibilityMaxDataflows
}

func normalizeSQLDataflowVisibilityNames(names []string, limit int) ([]string, error) {
	if len(names) == 0 {
		return nil, ErrSQLDataflowVisibilityInvalid
	}
	if len(names) > limit {
		return nil, ErrSQLDataflowVisibilityCapacity
	}
	normalized := make([]string, len(names))
	for index, name := range names {
		name = strings.TrimSpace(name)
		if name == "" {
			return nil, ErrSQLDataflowVisibilityInvalid
		}
		normalized[index] = name
	}
	sort.Strings(normalized)
	for index := 1; index < len(normalized); index++ {
		if normalized[index] == normalized[index-1] {
			return nil, ErrSQLDataflowVisibilityDuplicate
		}
	}
	return normalized, nil
}
