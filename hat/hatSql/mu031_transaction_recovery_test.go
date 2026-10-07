package hatSql

import (
	"errors"
	"testing"
)

var errMU031RecoveryMutation = errors.New("mutation failed after changing state")
var errMU031RecoveryRestore = errors.New("snapshot restore failed")

type mu031RecoveryFailure struct {
	mu031TransactionalSum
	panicRestore bool
	callbacks    int
}

func (state *mu031RecoveryFailure) Add(interface{}) error {
	state.callbacks++
	state.total = 99
	return errMU031RecoveryMutation
}

func (state *mu031RecoveryFailure) Retract(interface{}) error {
	return state.Add(nil)
}

func (state *mu031RecoveryFailure) Merge(SQLAggregateState) error {
	return state.Add(nil)
}

func (state *mu031RecoveryFailure) UnmarshalBinary([]byte) error {
	state.callbacks++
	state.total = -1 // A restore callback may itself mutate before failing.
	if state.panicRestore {
		panic("private callback panic payload")
	}
	return errMU031RecoveryRestore
}

func TestMU031AggregateTransactionFailedRecoveryClosesBoundary(t *testing.T) {
	for _, operation := range []string{"add", "retract", "merge"} {
		for _, panicRestore := range []bool{false, true} {
			name := operation + "/error"
			if panicRestore {
				name = operation + "/panic"
			}
			t.Run(name, func(t *testing.T) {
				state := &mu031RecoveryFailure{panicRestore: panicRestore}
				tx, err := NewSQLAggregateTransaction(state)
				if err != nil {
					t.Fatal(err)
				}
				switch operation {
				case "add":
					err = tx.Add(int64(1))
				case "retract":
					err = tx.Retract(int64(1))
				case "merge":
					err = tx.Merge(&mu031TransactionalSum{})
				}
				if !errors.Is(err, errMU031RecoveryMutation) {
					t.Fatalf("lost original mutation error: %v", err)
				}
				restoreErr := errMU031RecoveryRestore
				if panicRestore {
					restoreErr = ErrSQLAggregateCallbackPanic
				}
				if !errors.Is(err, restoreErr) {
					t.Errorf("lost recovery error: %v", err)
				}
				callbacks := state.callbacks
				for _, check := range []struct {
					name string
					call func() error
				}{
					{"Commit", tx.Commit},
					{"Add", func() error { return tx.Add(int64(2)) }},
					{"Retract", func() error { return tx.Retract(int64(2)) }},
					{"Merge", func() error { return tx.Merge(&mu031TransactionalSum{}) }},
					{"Finalize", func() error { _, err := tx.Finalize(); return err }},
					{"Rollback", tx.Rollback},
				} {
					if err := check.call(); !errors.Is(err, ErrSQLAggregateTransactionClosed) {
						t.Errorf("%s after failed recovery = %v; want closed", check.name, err)
					}
				}
				if state.callbacks != callbacks {
					t.Error("closed transaction invoked user callbacks")
				}
			})
		}
	}
}
