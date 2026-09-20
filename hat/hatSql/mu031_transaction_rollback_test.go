package hatSql_test

import (
	"errors"
	"testing"

	"hatrie_cache/hat/hatSql"
)

type mu031PanicRestoreState struct {
	mu031SerializableSum
}

func (*mu031PanicRestoreState) UnmarshalBinary([]byte) error {
	panic("restore failure")
}

func TestMU031TransactionalAggregateReportsRollbackPanic(t *testing.T) {
	transaction, err := hatSql.NewSQLTransactionalAggregateState(&mu031PanicRestoreState{})
	if err != nil {
		t.Fatalf("NewSQLTransactionalAggregateState() error = %v", err)
	}
	wantErr := errors.New("reject after partial mutation")
	err = transaction.Run(func(state hatSql.SQLAggregateState) error {
		if err := state.Add(int64(1)); err != nil {
			return err
		}
		return wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("Run() error = %v, want callback error %v", err, wantErr)
	}
	if !errors.Is(err, hatSql.ErrSQLAggregateStateRollback) {
		t.Fatalf("Run() error = %v, want ErrSQLAggregateStateRollback", err)
	}
}
