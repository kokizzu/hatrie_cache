package hatSql_test

import (
	"errors"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestMU031TransactionalAggregateRollsBackReturnedErrors(t *testing.T) {
	transaction, err := hatSql.NewSQLTransactionalAggregateState(&mu031SerializableSum{})
	if err != nil {
		t.Fatalf("NewSQLTransactionalAggregateState() error = %v", err)
	}
	if err := transaction.Add(int64(10)); err != nil {
		t.Fatalf("Add() error = %v", err)
	}
	wantErr := errors.New("reject batch")
	if err := transaction.Run(func(state hatSql.SQLAggregateState) error {
		if err := state.Add(int64(5)); err != nil {
			return err
		}
		return wantErr
	}); !errors.Is(err, wantErr) {
		t.Fatalf("Run() error = %v, want %v", err, wantErr)
	}
	assertMU031AggregateTotal(t, transaction, 10)
}

func TestMU031TransactionalAggregateRollsBackPanics(t *testing.T) {
	transaction, err := hatSql.NewSQLTransactionalAggregateState(&mu031SerializableSum{})
	if err != nil {
		t.Fatalf("NewSQLTransactionalAggregateState() error = %v", err)
	}
	if err := transaction.Add(int64(10)); err != nil {
		t.Fatalf("Add() error = %v", err)
	}
	if err := transaction.Run(func(state hatSql.SQLAggregateState) error {
		if err := state.Add(int64(7)); err != nil {
			return err
		}
		panic("callback failure")
	}); !errors.Is(err, hatSql.ErrSQLAggregateStatePanic) {
		t.Fatalf("Run() error = %v, want ErrSQLAggregateStatePanic", err)
	}
	assertMU031AggregateTotal(t, transaction, 10)
}

func TestMU031TransactionalAggregatePreservesCapabilityBoundaries(t *testing.T) {
	base, err := hatSql.NewSQLTransactionalAggregateState(&mu031SerializableSum{})
	if err != nil {
		t.Fatalf("NewSQLTransactionalAggregateState() error = %v", err)
	}
	if _, ok := interface{}(base).(hatSql.SQLRetractableAggregateState); ok {
		t.Fatal("base transactional state must not advertise retraction")
	}
	retractable, err := hatSql.NewSQLTransactionalRetractableAggregateState(&mu031SerializableSum{})
	if err != nil {
		t.Fatalf("NewSQLTransactionalRetractableAggregateState() error = %v", err)
	}
	if err := retractable.Add(int64(11)); err != nil {
		t.Fatalf("retractable Add() error = %v", err)
	}
	if err := retractable.Retract(int64(1)); err != nil {
		t.Fatalf("Retract() error = %v", err)
	}
	assertMU031AggregateTotal(t, retractable, 10)
	if _, err := hatSql.NewSQLTransactionalAggregateState(&mu031LegacySum{}); !errors.Is(err, hatSql.ErrSQLAggregateStateNotSerializable) {
		t.Fatalf("non-serializable constructor error = %v, want ErrSQLAggregateStateNotSerializable", err)
	}
}

func TestMU031TransactionalAggregateRegistryFactories(t *testing.T) {
	registry := hatSql.NewSQLAggregateCombinatorRegistry()
	combinator, err := hatSql.NewSQLAggregateCombinator("serial_sum", func() hatSql.SQLAggregateState {
		return &mu031SerializableSum{}
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(combinator); err != nil {
		t.Fatal(err)
	}
	transaction, err := registry.NewTransactionalState("SERIAL_SUM")
	if err != nil {
		t.Fatalf("NewTransactionalState() error = %v", err)
	}
	if err := transaction.Run(func(state hatSql.SQLAggregateState) error {
		return state.Add(int64(13))
	}); err != nil {
		t.Fatalf("transaction Run() error = %v", err)
	}
	assertMU031AggregateTotal(t, transaction, 13)
	retractable, err := registry.NewTransactionalRetractableState("serial_sum")
	if err != nil {
		t.Fatalf("NewTransactionalRetractableState() error = %v", err)
	}
	if err := retractable.Add(int64(4)); err != nil {
		t.Fatal(err)
	}
	if err := retractable.Retract(int64(4)); err != nil {
		t.Fatal(err)
	}
	assertMU031AggregateTotal(t, retractable, 0)
}

func assertMU031AggregateTotal(t *testing.T, state hatSql.SQLAggregateState, want int64) {
	t.Helper()
	got, err := state.Finalize()
	if err != nil {
		t.Fatalf("Finalize() error = %v", err)
	}
	if got != want {
		t.Fatalf("Finalize() = %v, want %d", got, want)
	}
}
