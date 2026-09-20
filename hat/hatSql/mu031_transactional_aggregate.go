package hatSql

import (
	"errors"
	"fmt"
)

var (
	ErrSQLAggregateStateNil                 = errors.New("hatSql: transactional aggregate state is nil")
	ErrSQLAggregateStateNotSerializable     = errors.New("hatSql: aggregate state is not serializable")
	ErrSQLAggregateStateCallbackRequired    = errors.New("hatSql: aggregate transaction callback is required")
	ErrSQLAggregateStatePanic               = errors.New("hatSql: aggregate transaction callback panicked")
	ErrSQLAggregateStateRollback             = errors.New("hatSql: aggregate transaction rollback failed")
)

// SQLTransactionalAggregateState adds opt-in snapshot-backed transactions to
// a serializable aggregate state. Existing aggregate execution does not use
// this wrapper and therefore pays no snapshot cost.
type SQLTransactionalAggregateState struct {
	state        SQLAggregateState
	serializable SQLSerializableAggregateState
}

// NewSQLTransactionalAggregateState wraps a serializable state. A binary
// snapshot is required because arbitrary aggregate implementations cannot be
// cloned safely through the base interface.
func NewSQLTransactionalAggregateState(state SQLAggregateState) (*SQLTransactionalAggregateState, error) {
	if isNilSQLAggregateState(state) {
		return nil, ErrSQLAggregateStateNil
	}
	serializable, ok := state.(SQLSerializableAggregateState)
	if !ok {
		return nil, ErrSQLAggregateStateNotSerializable
	}
	return &SQLTransactionalAggregateState{state: state, serializable: serializable}, nil
}

// Run executes one aggregate mutation transaction. A returned error or a
// panic restores the snapshot captured before callback. A successful callback
// publishes its changes. The callback receives the underlying state so it can
// use aggregate-specific interfaces without making the base wrapper claim
// optional capabilities it does not have.
func (state *SQLTransactionalAggregateState) Run(callback func(SQLAggregateState) error) (err error) {
	if state == nil || isNilSQLAggregateState(state.state) {
		return ErrSQLAggregateStateNil
	}
	if callback == nil {
		return ErrSQLAggregateStateCallbackRequired
	}
	snapshot, err := state.snapshot()
	if err != nil {
		return err
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("%w: %v", ErrSQLAggregateStatePanic, recovered)
			if rollbackErr := state.restore(snapshot); rollbackErr != nil {
				err = fmt.Errorf("%w; %w", err, rollbackErr)
			}
			return
		}
		if err == nil {
			return
		}
		if rollbackErr := state.restore(snapshot); rollbackErr != nil {
			err = fmt.Errorf("%w; %w", err, rollbackErr)
		}
	}()
	err = callback(state.state)
	return err
}

// Add applies one value transactionally.
func (state *SQLTransactionalAggregateState) Add(value interface{}) error {
	return state.Run(func(inner SQLAggregateState) error { return inner.Add(value) })
}

// Merge applies one merge transactionally. Transactional wrappers are
// unwrapped before calling an implementation that expects its concrete type.
func (state *SQLTransactionalAggregateState) Merge(other SQLAggregateState) error {
	return state.Run(func(inner SQLAggregateState) error {
		return inner.Merge(unwrapSQLTransactionalAggregateState(other))
	})
}

// Finalize returns the underlying result and converts an implementation panic
// into ErrSQLAggregateStatePanic.
func (state *SQLTransactionalAggregateState) Finalize() (result interface{}, err error) {
	if state == nil || isNilSQLAggregateState(state.state) {
		return nil, ErrSQLAggregateStateNil
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			result = nil
			err = fmt.Errorf("%w: %v", ErrSQLAggregateStatePanic, recovered)
		}
	}()
	return state.state.Finalize()
}

// MarshalBinary returns an isolated snapshot of the underlying state.
func (state *SQLTransactionalAggregateState) MarshalBinary() (snapshot []byte, err error) {
	if state == nil || isNilSQLAggregateState(state.state) {
		return nil, ErrSQLAggregateStateNil
	}
	return state.snapshot()
}

// UnmarshalBinary applies a snapshot transactionally.
func (state *SQLTransactionalAggregateState) UnmarshalBinary(snapshot []byte) error {
	return state.Run(func(_ SQLAggregateState) error {
		return state.serializable.UnmarshalBinary(snapshot)
	})
}

func (state *SQLTransactionalAggregateState) snapshot() (snapshot []byte, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			snapshot = nil
			err = fmt.Errorf("%w: %v", ErrSQLAggregateStatePanic, recovered)
		}
	}()
	encoded, err := state.serializable.MarshalBinary()
	if err != nil {
		return nil, err
	}
	return append([]byte(nil), encoded...), nil
}

func (state *SQLTransactionalAggregateState) restore(snapshot []byte) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("%w: %w", ErrSQLAggregateStateRollback, fmt.Errorf("%w: %v", ErrSQLAggregateStatePanic, recovered))
		}
	}()
	if err := state.serializable.UnmarshalBinary(snapshot); err != nil {
		return fmt.Errorf("%w: %v", ErrSQLAggregateStateRollback, err)
	}
	return nil
}

// SQLTransactionalRetractableAggregateState is the capability-preserving
// transactional wrapper for states that support both retraction and binary
// snapshots.
type SQLTransactionalRetractableAggregateState struct {
	base        *SQLTransactionalAggregateState
	retractable SQLRetractableAggregateState
}

// NewSQLTransactionalRetractableAggregateState wraps a retractable,
// serializable aggregate state without making non-retractable wrappers appear
// to support the optional capability.
func NewSQLTransactionalRetractableAggregateState(state SQLRetractableAggregateState) (*SQLTransactionalRetractableAggregateState, error) {
	base, err := NewSQLTransactionalAggregateState(state)
	if err != nil {
		return nil, err
	}
	return &SQLTransactionalRetractableAggregateState{base: base, retractable: state}, nil
}

// Run executes a callback transactionally against the underlying state.
func (state *SQLTransactionalRetractableAggregateState) Run(callback func(SQLAggregateState) error) error {
	if state == nil {
		return ErrSQLAggregateStateNil
	}
	return state.base.Run(callback)
}

// Add applies one value transactionally.
func (state *SQLTransactionalRetractableAggregateState) Add(value interface{}) error {
	if state == nil {
		return ErrSQLAggregateStateNil
	}
	return state.base.Add(value)
}

// Retract removes one value transactionally.
func (state *SQLTransactionalRetractableAggregateState) Retract(value interface{}) error {
	if state == nil {
		return ErrSQLAggregateStateNil
	}
	return state.base.Run(func(_ SQLAggregateState) error { return state.retractable.Retract(value) })
}

// Merge applies one merge transactionally.
func (state *SQLTransactionalRetractableAggregateState) Merge(other SQLAggregateState) error {
	if state == nil {
		return ErrSQLAggregateStateNil
	}
	return state.base.Merge(other)
}

// Finalize returns the underlying result.
func (state *SQLTransactionalRetractableAggregateState) Finalize() (interface{}, error) {
	if state == nil {
		return nil, ErrSQLAggregateStateNil
	}
	return state.base.Finalize()
}

// MarshalBinary returns an isolated snapshot.
func (state *SQLTransactionalRetractableAggregateState) MarshalBinary() ([]byte, error) {
	if state == nil {
		return nil, ErrSQLAggregateStateNil
	}
	return state.base.MarshalBinary()
}

// UnmarshalBinary applies a snapshot transactionally.
func (state *SQLTransactionalRetractableAggregateState) UnmarshalBinary(snapshot []byte) error {
	if state == nil {
		return ErrSQLAggregateStateNil
	}
	return state.base.UnmarshalBinary(snapshot)
}

func unwrapSQLTransactionalAggregateState(state SQLAggregateState) SQLAggregateState {
	switch wrapped := state.(type) {
	case *SQLTransactionalAggregateState:
		if wrapped == nil {
			return nil
		}
		return wrapped.state
	case *SQLTransactionalRetractableAggregateState:
		if wrapped == nil || wrapped.base == nil {
			return nil
		}
		return wrapped.base.state
	default:
		return state
	}
}
