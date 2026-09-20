package hatSql

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
)

var (
	ErrSQLAggregateCombinatorInvalid         = errors.New("hatSql: invalid aggregate combinator")
	ErrSQLAggregateCombinatorExists          = errors.New("hatSql: aggregate combinator already registered")
	ErrSQLAggregateCombinatorMissing         = errors.New("hatSql: aggregate combinator is not registered")
	ErrSQLAggregateCombinatorNotRetractable  = errors.New("hatSql: aggregate combinator does not support retraction")
	ErrSQLAggregateCombinatorNotSerializable = errors.New("hatSql: aggregate combinator does not support binary serialization")
)

// SQLAggregateState is the reusable state, merge, and finalize contract for a
// SQL aggregate. A state can be built on one worker, merged into another, and
// finalized only after all partial states have been combined.
type SQLAggregateState interface {
	Add(value interface{}) error
	Merge(other SQLAggregateState) error
	Finalize() (interface{}, error)
}

// SQLRetractableAggregateState extends SQLAggregateState with removal support.
// Implementations must make Retract(value) the inverse of Add(value) for the
// values they accept. The base SQLAggregateState stays unchanged so existing
// aggregate implementations remain source-compatible.
type SQLRetractableAggregateState interface {
	SQLAggregateState
	Retract(value interface{}) error
}

// SQLSerializableAggregateState extends SQLAggregateState with a binary
// snapshot contract. The byte representation is owned by the implementation
// and must be stable enough for the caller's persistence or transfer scope.
type SQLSerializableAggregateState interface {
	SQLAggregateState
	MarshalBinary() ([]byte, error)
	UnmarshalBinary(data []byte) error
}

// SQLAggregateFactory constructs an independent aggregate state.
type SQLAggregateFactory func() SQLAggregateState

// SQLAggregateCombinator names a state factory registered with a
// SQLAggregateCombinatorRegistry.
type SQLAggregateCombinator struct {
	Name    string
	Factory SQLAggregateFactory
}

// NewSQLAggregateCombinator validates and normalizes a named aggregate state
// factory. Names are case-insensitive and stored in uppercase.
func NewSQLAggregateCombinator(name string, factory SQLAggregateFactory) (SQLAggregateCombinator, error) {
	name = normalizeSQLAggregateCombinatorName(name)
	if name == "" || factory == nil {
		return SQLAggregateCombinator{}, fmt.Errorf("%w: name and factory are required", ErrSQLAggregateCombinatorInvalid)
	}
	return SQLAggregateCombinator{Name: name, Factory: factory}, nil
}

// NewState creates an independent state from the combinator factory.
func (combinator SQLAggregateCombinator) NewState() (SQLAggregateState, error) {
	if normalizeSQLAggregateCombinatorName(combinator.Name) == "" || combinator.Factory == nil {
		return nil, ErrSQLAggregateCombinatorInvalid
	}
	state := combinator.Factory()
	if isNilSQLAggregateState(state) {
		return nil, fmt.Errorf("%w: factory returned nil state", ErrSQLAggregateCombinatorInvalid)
	}
	return state, nil
}

// NewRetractableState creates a state only when the factory advertises
// retraction support. A missing capability is reported explicitly instead of
// silently falling back to a slower or incorrect recomputation path.
func (combinator SQLAggregateCombinator) NewRetractableState() (SQLRetractableAggregateState, error) {
	state, err := combinator.NewState()
	if err != nil {
		return nil, err
	}
	retractable, ok := state.(SQLRetractableAggregateState)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrSQLAggregateCombinatorNotRetractable, combinator.Name)
	}
	return retractable, nil
}

// NewSerializableState creates a state only when the factory advertises
// binary snapshot support.
func (combinator SQLAggregateCombinator) NewSerializableState() (SQLSerializableAggregateState, error) {
	state, err := combinator.NewState()
	if err != nil {
		return nil, err
	}
	serializable, ok := state.(SQLSerializableAggregateState)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrSQLAggregateCombinatorNotSerializable, combinator.Name)
	}
	return serializable, nil
}

// NewTransactionalState creates an opt-in snapshot-backed state. The
// registered aggregate must implement SQLSerializableAggregateState.
func (combinator SQLAggregateCombinator) NewTransactionalState() (*SQLTransactionalAggregateState, error) {
	state, err := combinator.NewSerializableState()
	if err != nil {
		return nil, err
	}
	return NewSQLTransactionalAggregateState(state)
}

// NewTransactionalRetractableState creates an opt-in snapshot-backed state
// with truthful retraction capability discovery.
func (combinator SQLAggregateCombinator) NewTransactionalRetractableState() (*SQLTransactionalRetractableAggregateState, error) {
	state, err := combinator.NewRetractableState()
	if err != nil {
		return nil, err
	}
	return NewSQLTransactionalRetractableAggregateState(state)
}

// SQLAggregateCombinatorRegistry stores named aggregate state factories. The
// registry is safe for concurrent registration and lookup; returned states are
// independent and are not shared by the registry.
type SQLAggregateCombinatorRegistry struct {
	mu          sync.RWMutex
	combinators map[string]SQLAggregateCombinator
}

// NewSQLAggregateCombinatorRegistry returns an empty aggregate registry.
func NewSQLAggregateCombinatorRegistry() *SQLAggregateCombinatorRegistry {
	return &SQLAggregateCombinatorRegistry{combinators: make(map[string]SQLAggregateCombinator)}
}

// Register adds a named combinator and rejects duplicate names.
func (registry *SQLAggregateCombinatorRegistry) Register(combinator SQLAggregateCombinator) error {
	if registry == nil {
		return ErrSQLAggregateCombinatorInvalid
	}
	name := normalizeSQLAggregateCombinatorName(combinator.Name)
	if name == "" || combinator.Factory == nil {
		return ErrSQLAggregateCombinatorInvalid
	}
	combinator.Name = name
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.combinators == nil {
		registry.combinators = make(map[string]SQLAggregateCombinator)
	}
	if _, exists := registry.combinators[name]; exists {
		return fmt.Errorf("%w: %s", ErrSQLAggregateCombinatorExists, name)
	}
	registry.combinators[name] = combinator
	return nil
}

// NewState resolves a name and creates a fresh aggregate state.
func (registry *SQLAggregateCombinatorRegistry) NewState(name string) (SQLAggregateState, error) {
	if registry == nil {
		return nil, ErrSQLAggregateCombinatorMissing
	}
	name = normalizeSQLAggregateCombinatorName(name)
	registry.mu.RLock()
	combinator, ok := registry.combinators[name]
	registry.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrSQLAggregateCombinatorMissing, name)
	}
	return combinator.NewState()
}

// NewRetractableState resolves a name and creates a fresh retractable state.
// It returns ErrSQLAggregateCombinatorNotRetractable when the registered
// factory only implements the base aggregate contract.
func (registry *SQLAggregateCombinatorRegistry) NewRetractableState(name string) (SQLRetractableAggregateState, error) {
	if registry == nil {
		return nil, ErrSQLAggregateCombinatorMissing
	}
	name = normalizeSQLAggregateCombinatorName(name)
	registry.mu.RLock()
	combinator, ok := registry.combinators[name]
	registry.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrSQLAggregateCombinatorMissing, name)
	}
	return combinator.NewRetractableState()
}

// NewSerializableState resolves a name and creates a fresh serializable
// state. It returns ErrSQLAggregateCombinatorNotSerializable when the
// registered factory only implements the base aggregate contract.
func (registry *SQLAggregateCombinatorRegistry) NewSerializableState(name string) (SQLSerializableAggregateState, error) {
	if registry == nil {
		return nil, ErrSQLAggregateCombinatorMissing
	}
	name = normalizeSQLAggregateCombinatorName(name)
	registry.mu.RLock()
	combinator, ok := registry.combinators[name]
	registry.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrSQLAggregateCombinatorMissing, name)
	}
	return combinator.NewSerializableState()
}

// NewTransactionalState resolves a serializable aggregate and wraps it with
// opt-in snapshot-backed transactions.
func (registry *SQLAggregateCombinatorRegistry) NewTransactionalState(name string) (*SQLTransactionalAggregateState, error) {
	if registry == nil {
		return nil, ErrSQLAggregateCombinatorMissing
	}
	name = normalizeSQLAggregateCombinatorName(name)
	registry.mu.RLock()
	combinator, ok := registry.combinators[name]
	registry.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrSQLAggregateCombinatorMissing, name)
	}
	return combinator.NewTransactionalState()
}

// NewTransactionalRetractableState resolves a retractable, serializable
// aggregate and wraps it with opt-in snapshot-backed transactions.
func (registry *SQLAggregateCombinatorRegistry) NewTransactionalRetractableState(name string) (*SQLTransactionalRetractableAggregateState, error) {
	if registry == nil {
		return nil, ErrSQLAggregateCombinatorMissing
	}
	name = normalizeSQLAggregateCombinatorName(name)
	registry.mu.RLock()
	combinator, ok := registry.combinators[name]
	registry.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrSQLAggregateCombinatorMissing, name)
	}
	return combinator.NewTransactionalRetractableState()
}

// Names returns registered combinator names in deterministic order.
func (registry *SQLAggregateCombinatorRegistry) Names() []string {
	if registry == nil {
		return nil
	}
	registry.mu.RLock()
	names := make([]string, 0, len(registry.combinators))
	for name := range registry.combinators {
		names = append(names, name)
	}
	registry.mu.RUnlock()
	sort.Strings(names)
	return names
}

func normalizeSQLAggregateCombinatorName(name string) string {
	return strings.ToUpper(strings.TrimSpace(name))
}

func isNilSQLAggregateState(state SQLAggregateState) bool {
	if state == nil {
		return true
	}
	value := reflect.ValueOf(state)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
