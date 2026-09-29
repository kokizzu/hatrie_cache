package hatSql

import (
	"errors"
	"reflect"
	"sort"
	"strings"
	"sync"
)

var (
	ErrSQLIndexRetirementRegistryNil   = errors.New("SQL index retirement registry is nil")
	ErrSQLIndexRetirementNameRequired  = errors.New("SQL index retirement name is required")
	ErrSQLIndexRetirementIndexRequired = errors.New("SQL index retirement index is required")
	ErrSQLIndexRetirementDuplicate     = errors.New("SQL index retirement name already exists")
	ErrSQLIndexRetirementNotFound      = errors.New("SQL index retirement name was not found")
	ErrSQLIndexRetirementDraining      = errors.New("SQL index is draining and rejects new readers")
)

// SQLIndexRetirementState describes whether an index accepts new readers.
type SQLIndexRetirementState string

const (
	SQLIndexRetirementActive   SQLIndexRetirementState = "active"
	SQLIndexRetirementDraining SQLIndexRetirementState = "draining"
	SQLIndexRetirementRemoved  SQLIndexRetirementState = "removed"
)

// SQLIndexRetirementStatus is an immutable registry snapshot for one index.
// A draining index remains present until Readers reaches zero.
type SQLIndexRetirementStatus struct {
	Name    string
	State   SQLIndexRetirementState
	Readers int
}

// SQLIndexRetirementResult reports a retirement or final release. Index is
// returned only when Removed is true, allowing the caller to close or discard
// caller-owned resources after no dependent reader can use them.
type SQLIndexRetirementResult struct {
	Name    string
	State   SQLIndexRetirementState
	Readers int
	Removed bool
	Index   any
}

type sqlIndexRetirementEntry struct {
	name    string
	index   any
	state   SQLIndexRetirementState
	readers int
}

// SQLIndexRetirementRegistry coordinates safe removal of caller-owned SQL
// indexes. It never starts a worker and never closes an index itself.
type SQLIndexRetirementRegistry struct {
	mu      sync.Mutex
	entries map[string]*sqlIndexRetirementEntry
}

// SQLIndexReaderLease pins one registered index for a dependent reader.
// Release it when the reader no longer touches Index().
type SQLIndexReaderLease struct {
	mu       sync.Mutex
	registry *SQLIndexRetirementRegistry
	entry    *sqlIndexRetirementEntry
	released bool
}

// NewSQLIndexRetirementRegistry creates an empty index retirement registry.
func NewSQLIndexRetirementRegistry() *SQLIndexRetirementRegistry {
	return &SQLIndexRetirementRegistry{entries: make(map[string]*sqlIndexRetirementEntry)}
}

// Register adds an active index under name. A removed name may be registered
// again after the caller has handled the detached object returned by Retire or
// the final reader Release.
func (registry *SQLIndexRetirementRegistry) Register(name string, index any) error {
	if registry == nil {
		return ErrSQLIndexRetirementRegistryNil
	}
	name, err := normalizeSQLIndexRetirementName(name)
	if err != nil {
		return err
	}
	if sqlIndexRetirementNil(index) {
		return ErrSQLIndexRetirementIndexRequired
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if _, exists := registry.entries[name]; exists {
		return ErrSQLIndexRetirementDuplicate
	}
	registry.entries[name] = &sqlIndexRetirementEntry{name: name, index: index, state: SQLIndexRetirementActive}
	return nil
}

// Acquire returns a reader lease for an active index. Retiring indexes reject
// new readers so removal cannot race a newly admitted dependent query.
func (registry *SQLIndexRetirementRegistry) Acquire(name string) (*SQLIndexReaderLease, error) {
	if registry == nil {
		return nil, ErrSQLIndexRetirementRegistryNil
	}
	name, err := normalizeSQLIndexRetirementName(name)
	if err != nil {
		return nil, err
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	entry, exists := registry.entries[name]
	if !exists {
		return nil, ErrSQLIndexRetirementNotFound
	}
	if entry.state != SQLIndexRetirementActive {
		return nil, ErrSQLIndexRetirementDraining
	}
	entry.readers++
	return &SQLIndexReaderLease{registry: registry, entry: entry}, nil
}

// Retire stops new readers. It removes an idle index immediately; otherwise it
// returns a draining result and the final lease Release performs removal.
func (registry *SQLIndexRetirementRegistry) Retire(name string) (SQLIndexRetirementResult, error) {
	if registry == nil {
		return SQLIndexRetirementResult{}, ErrSQLIndexRetirementRegistryNil
	}
	name, err := normalizeSQLIndexRetirementName(name)
	if err != nil {
		return SQLIndexRetirementResult{}, err
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	entry, exists := registry.entries[name]
	if !exists {
		return SQLIndexRetirementResult{}, ErrSQLIndexRetirementNotFound
	}
	if entry.state == SQLIndexRetirementDraining {
		return SQLIndexRetirementResult{Name: name, State: entry.state, Readers: entry.readers}, nil
	}
	entry.state = SQLIndexRetirementDraining
	if entry.readers > 0 {
		return SQLIndexRetirementResult{Name: name, State: entry.state, Readers: entry.readers}, nil
	}
	return registry.removeLocked(entry), nil
}

// Status returns the current active or draining status for name.
func (registry *SQLIndexRetirementRegistry) Status(name string) (SQLIndexRetirementStatus, bool) {
	if registry == nil {
		return SQLIndexRetirementStatus{}, false
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return SQLIndexRetirementStatus{}, false
	}
	registry.mu.Lock()
	entry, exists := registry.entries[name]
	if !exists {
		registry.mu.Unlock()
		return SQLIndexRetirementStatus{}, false
	}
	status := SQLIndexRetirementStatus{Name: entry.name, State: entry.state, Readers: entry.readers}
	registry.mu.Unlock()
	return status, true
}

// Statuses returns active and draining indexes in deterministic name order.
func (registry *SQLIndexRetirementRegistry) Statuses() []SQLIndexRetirementStatus {
	if registry == nil {
		return nil
	}
	registry.mu.Lock()
	statuses := make([]SQLIndexRetirementStatus, 0, len(registry.entries))
	for _, entry := range registry.entries {
		statuses = append(statuses, SQLIndexRetirementStatus{Name: entry.name, State: entry.state, Readers: entry.readers})
	}
	registry.mu.Unlock()
	sort.Slice(statuses, func(left, right int) bool { return statuses[left].Name < statuses[right].Name })
	return statuses
}

// Index returns the pinned caller-owned index, or nil after Release.
func (lease *SQLIndexReaderLease) Index() any {
	if lease == nil {
		return nil
	}
	lease.mu.Lock()
	defer lease.mu.Unlock()
	if lease.released || lease.entry == nil {
		return nil
	}
	return lease.entry.index
}

// Release unpins the reader. The first release is accepted; repeated releases
// return false. When this is the final reader of a draining index, Removed is
// true and Index contains the detached object.
func (lease *SQLIndexReaderLease) Release() (SQLIndexRetirementResult, bool) {
	if lease == nil {
		return SQLIndexRetirementResult{}, false
	}
	lease.mu.Lock()
	if lease.released || lease.registry == nil || lease.entry == nil {
		lease.mu.Unlock()
		return SQLIndexRetirementResult{}, false
	}
	lease.released = true
	registry, entry := lease.registry, lease.entry
	lease.mu.Unlock()

	registry.mu.Lock()
	defer registry.mu.Unlock()
	if entry.readers <= 0 {
		return SQLIndexRetirementResult{}, false
	}
	entry.readers--
	if entry.state == SQLIndexRetirementDraining && entry.readers == 0 {
		return registry.removeLocked(entry), true
	}
	return SQLIndexRetirementResult{Name: entry.name, State: entry.state, Readers: entry.readers}, true
}

func (registry *SQLIndexRetirementRegistry) removeLocked(entry *sqlIndexRetirementEntry) SQLIndexRetirementResult {
	delete(registry.entries, entry.name)
	index := entry.index
	entry.index = nil
	entry.state = SQLIndexRetirementRemoved
	return SQLIndexRetirementResult{Name: entry.name, State: SQLIndexRetirementRemoved, Removed: true, Index: index}
}

func normalizeSQLIndexRetirementName(name string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return "", ErrSQLIndexRetirementNameRequired
	}
	return name, nil
}

func sqlIndexRetirementNil(index any) bool {
	if index == nil {
		return true
	}
	value := reflect.ValueOf(index)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
