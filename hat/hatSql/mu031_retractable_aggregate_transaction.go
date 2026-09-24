package hatSql

import (
	"errors"
	"fmt"
)

var (
	// ErrSQLAggregateTransactionNotSerializable reports that a state cannot be
	// snapshotted for atomic rollback.
	ErrSQLAggregateTransactionNotSerializable = errors.New("hatSql: aggregate transaction requires serializable state")
	// ErrSQLAggregateTransactionClosed reports use after Commit or Rollback.
	ErrSQLAggregateTransactionClosed = errors.New("hatSql: aggregate transaction is closed")
	// ErrSQLAggregateCallbackPanic reports a panic converted into an error from
	// an aggregate callback or snapshot callback.
	ErrSQLAggregateCallbackPanic = errors.New("hatSql: aggregate callback panicked")
)

// SQLAggregateTransaction applies aggregate callbacks against one initial
// binary snapshot. Any callback error or panic restores that snapshot, while
// Commit keeps the accumulated state and closes the transaction. The wrapper
// is single-owner and opt-in; direct SQLAggregateState calls are unchanged.
type SQLAggregateTransaction struct {
	state       SQLSerializableAggregateState
	retractable SQLRetractableAggregateState
	snapshot    []byte
	closed      bool
}

// NewSQLAggregateTransaction creates an atomic aggregate mutation boundary.
// The state must implement SQLSerializableAggregateState so rollback can be
// exact rather than a best-effort inverse operation.
func NewSQLAggregateTransaction(state SQLAggregateState) (*SQLAggregateTransaction, error) {
	if isNilSQLAggregateState(state) {
		return nil, ErrSQLAggregateTransactionNotSerializable
	}
	serializable, ok := state.(SQLSerializableAggregateState)
	if !ok {
		return nil, ErrSQLAggregateTransactionNotSerializable
	}
	snapshot, err := marshalSQLAggregateTransactionState(serializable)
	if err != nil {
		return nil, err
	}
	transaction := &SQLAggregateTransaction{
		state:       serializable,
		snapshot:    snapshot,
		retractable: nil,
	}
	if retractable, ok := state.(SQLRetractableAggregateState); ok {
		transaction.retractable = retractable
	}
	return transaction, nil
}

// Add atomically adds one value. A callback error or panic restores the
// transaction's initial state and leaves the transaction open for reuse.
func (transaction *SQLAggregateTransaction) Add(value interface{}) error {
	return transaction.apply("add", func() error {
		return transaction.state.Add(value)
	})
}

// Retract atomically removes one value when the wrapped state advertises the
// retractable capability.
func (transaction *SQLAggregateTransaction) Retract(value interface{}) error {
	if err := transaction.ensureOpen(); err != nil {
		return err
	}
	if transaction.retractable == nil {
		return ErrSQLAggregateCombinatorNotRetractable
	}
	return transaction.apply("retract", func() error {
		return transaction.retractable.Retract(value)
	})
}

// Merge atomically merges one partial aggregate state.
func (transaction *SQLAggregateTransaction) Merge(other SQLAggregateState) error {
	return transaction.apply("merge", func() error {
		if isNilSQLAggregateState(other) {
			return ErrSQLAggregateCombinatorInvalid
		}
		return transaction.state.Merge(other)
	})
}

// Finalize reads the current transactional state without closing it. Panics
// from user code are converted to ErrSQLAggregateCallbackPanic.
func (transaction *SQLAggregateTransaction) Finalize() (result interface{}, err error) {
	if err := transaction.ensureOpen(); err != nil {
		return nil, err
	}
	defer func() {
		if recover() != nil {
			result = nil
			err = ErrSQLAggregateCallbackPanic
		}
	}()
	return transaction.state.Finalize()
}

// Commit publishes the accumulated state and closes the mutation boundary.
func (transaction *SQLAggregateTransaction) Commit() error {
	if err := transaction.ensureOpen(); err != nil {
		return err
	}
	transaction.closed = true
	transaction.snapshot = nil
	return nil
}

// Rollback restores the initial snapshot and closes the mutation boundary.
func (transaction *SQLAggregateTransaction) Rollback() error {
	if err := transaction.ensureOpen(); err != nil {
		return err
	}
	err := transaction.restoreSnapshot()
	transaction.closed = true
	transaction.snapshot = nil
	return err
}

func (transaction *SQLAggregateTransaction) ensureOpen() error {
	if transaction == nil || transaction.closed || transaction.state == nil {
		return ErrSQLAggregateTransactionClosed
	}
	return nil
}

func (transaction *SQLAggregateTransaction) apply(operation string, callback func() error) (err error) {
	if err := transaction.ensureOpen(); err != nil {
		return err
	}
	defer func() {
		if recover() != nil {
			err = fmt.Errorf("%w: %s", ErrSQLAggregateCallbackPanic, operation)
		}
		if err == nil {
			return
		}
		if restoreErr := transaction.restoreSnapshot(); restoreErr != nil {
			err = fmt.Errorf("%w: %s; restore failed: %v", err, operation, restoreErr)
		}
	}()
	return callback()
}

func marshalSQLAggregateTransactionState(state SQLSerializableAggregateState) (snapshot []byte, err error) {
	defer func() {
		if recover() != nil {
			snapshot = nil
			err = ErrSQLAggregateCallbackPanic
		}
	}()
	encoded, err := state.MarshalBinary()
	if err != nil {
		return nil, err
	}
	return append([]byte(nil), encoded...), nil
}

func (transaction *SQLAggregateTransaction) restoreSnapshot() (err error) {
	defer func() {
		if recover() != nil {
			err = ErrSQLAggregateCallbackPanic
		}
	}()
	return transaction.state.UnmarshalBinary(append([]byte(nil), transaction.snapshot...))
}
