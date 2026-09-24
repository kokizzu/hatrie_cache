package hatSql

import (
	"errors"
	"testing"
)

func TestMU031AggregateTransactionRollbackAndCommit(t *testing.T) {
	state := &mu031TransactionalSum{}
	tx, err := NewSQLAggregateTransaction(state)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Add(int64(3)); err != nil {
		t.Fatal(err)
	}
	if err := tx.Add("wrong type"); !errors.Is(err, errMU031TransactionalSumType) {
		t.Fatalf("failed Add() error = %v, want %v", err, errMU031TransactionalSumType)
	}
	if state.total != 0 {
		t.Fatalf("state after failed transaction operation = %d, want 0", state.total)
	}
	if err := tx.Add(int64(7)); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
	if state.total != 7 {
		t.Fatalf("committed state = %d, want 7", state.total)
	}
	if err := tx.Add(int64(1)); !errors.Is(err, ErrSQLAggregateTransactionClosed) {
		t.Fatalf("Add() after Commit() error = %v, want closed", err)
	}
}

func TestMU031AggregateTransactionPanicIsolationAndRetract(t *testing.T) {
	state := &mu031TransactionalSum{panicOnAdd: true}
	tx, err := NewSQLAggregateTransaction(state)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Add(int64(2)); !errors.Is(err, ErrSQLAggregateCallbackPanic) {
		t.Fatalf("panic Add() error = %v, want callback panic", err)
	}
	if state.total != 0 {
		t.Fatalf("state after panic = %d, want 0", state.total)
	}
	state.panicOnAdd = false
	if err := tx.Add(int64(9)); err != nil {
		t.Fatal(err)
	}
	if err := tx.Retract(int64(4)); err != nil {
		t.Fatalf("Retract() error = %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback() error = %v", err)
	}
	if state.total != 0 {
		t.Fatalf("state after explicit rollback = %d, want 0", state.total)
	}
	if err := tx.Rollback(); !errors.Is(err, ErrSQLAggregateTransactionClosed) {
		t.Fatalf("second Rollback() error = %v, want closed", err)
	}
}

func TestMU031AggregateTransactionRequiresSerializableState(t *testing.T) {
	if _, err := NewSQLAggregateTransaction(&mu031LegacyAggregate{}); !errors.Is(err, ErrSQLAggregateTransactionNotSerializable) {
		t.Fatalf("non-serializable state error = %v, want not serializable", err)
	}
}

func TestMU031AggregateTransactionClosedPrecedesCapabilityError(t *testing.T) {
	state := &mu031SerializableNonRetractable{}
	tx, err := NewSQLAggregateTransaction(state)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := tx.Retract(int64(1)); !errors.Is(err, ErrSQLAggregateTransactionClosed) {
		t.Fatalf("Retract() after Commit() error = %v, want closed", err)
	}
}

func TestMU031AggregateTransactionMergeRollback(t *testing.T) {
	state := &mu031TransactionalSum{}
	tx, err := NewSQLAggregateTransaction(state)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Add(int64(4)); err != nil {
		t.Fatal(err)
	}
	if err := tx.Merge(&mu031TransactionalSum{total: 6}); err != nil {
		t.Fatalf("Merge() error = %v", err)
	}
	if state.total != 10 {
		t.Fatalf("merged state = %d, want 10", state.total)
	}
	if err := tx.Merge(&mu031LegacyAggregate{}); err == nil {
		t.Fatal("Merge() with an incompatible state succeeded")
	}
	if state.total != 0 {
		t.Fatalf("state after failed merge = %d, want 0", state.total)
	}
}

var errMU031TransactionalSumType = errors.New("mu031 transactional sum expects int64")

type mu031TransactionalSum struct {
	total      int64
	panicOnAdd bool
}

func (state *mu031TransactionalSum) Add(value interface{}) error {
	if state.panicOnAdd {
		panic("test panic")
	}
	number, ok := value.(int64)
	if !ok {
		return errMU031TransactionalSumType
	}
	state.total += number
	return nil
}

func (state *mu031TransactionalSum) Retract(value interface{}) error {
	number, ok := value.(int64)
	if !ok {
		return errMU031TransactionalSumType
	}
	state.total -= number
	return nil
}

func (state *mu031TransactionalSum) Merge(other SQLAggregateState) error {
	source, ok := other.(*mu031TransactionalSum)
	if !ok {
		return errors.New("mu031 transactional sum type mismatch")
	}
	state.total += source.total
	return nil
}

func (state *mu031TransactionalSum) Finalize() (interface{}, error) {
	return state.total, nil
}

func (state *mu031TransactionalSum) MarshalBinary() ([]byte, error) {
	return []byte{byte(state.total), byte(state.total >> 8), byte(state.total >> 16), byte(state.total >> 24), byte(state.total >> 32), byte(state.total >> 40), byte(state.total >> 48), byte(state.total >> 56)}, nil
}

func (state *mu031TransactionalSum) UnmarshalBinary(data []byte) error {
	if len(data) != 8 {
		return errors.New("mu031 transactional sum snapshot length")
	}
	state.total = int64(uint64(data[0]) | uint64(data[1])<<8 | uint64(data[2])<<16 | uint64(data[3])<<24 | uint64(data[4])<<32 | uint64(data[5])<<40 | uint64(data[6])<<48 | uint64(data[7])<<56)
	return nil
}

type mu031LegacyAggregate struct{}

func (*mu031LegacyAggregate) Add(interface{}) error          { return nil }
func (*mu031LegacyAggregate) Merge(SQLAggregateState) error  { return nil }
func (*mu031LegacyAggregate) Finalize() (interface{}, error) { return nil, nil }

type mu031SerializableNonRetractable struct{}

func (*mu031SerializableNonRetractable) Add(interface{}) error          { return nil }
func (*mu031SerializableNonRetractable) Merge(SQLAggregateState) error  { return nil }
func (*mu031SerializableNonRetractable) Finalize() (interface{}, error) { return nil, nil }
func (*mu031SerializableNonRetractable) MarshalBinary() ([]byte, error) {
	return []byte{0}, nil
}
func (*mu031SerializableNonRetractable) UnmarshalBinary([]byte) error { return nil }
