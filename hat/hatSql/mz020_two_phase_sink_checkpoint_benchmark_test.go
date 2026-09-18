package hatSql_test

import (
	"context"
	"strconv"
	"testing"

	hatSql "hatrie_cache/hat/hatSql"
)

type mz020BenchmarkParticipant struct{}

func (mz020BenchmarkParticipant) Prepare(context.Context, string) error { return nil }
func (mz020BenchmarkParticipant) Commit(context.Context, string) error  { return nil }

func BenchmarkMZ020OnePhaseBaseline(b *testing.B) {
	ledger, err := hatSql.NewSQLSinkExactlyOnceLedger(context.Background(), hatSql.SQLSinkExactlyOnceOptions{
		Capacity: 1024,
	})
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		frontier := uint64(index + 1)
		commit := hatSql.SQLSinkCommit{
			Sink:           "warehouse",
			TransactionID:  "txn-" + strconv.Itoa(index),
			IdempotencyKey: "event-" + strconv.Itoa(index),
			Progress:       []hatSql.SQLSinkProgress{{Sink: "warehouse", Partition: "0", Frontier: frontier}},
		}
		if committed, err := ledger.Commit(commit, func(string) error { return nil }); err != nil || !committed {
			b.Fatalf("Commit() = %t/%v, want true/nil", committed, err)
		}
	}
}

func BenchmarkMZ020TwoPhaseCoordinator(b *testing.B) {
	coordinator, err := hatSql.NewSQLSinkTwoPhaseCoordinator(context.Background(), hatSql.SQLSinkTwoPhaseOptions{
		Capacity: 1024,
	})
	if err != nil {
		b.Fatal(err)
	}
	participant := mz020BenchmarkParticipant{}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		frontier := uint64(index + 1)
		commit := hatSql.SQLSinkCommit{
			Sink:           "warehouse",
			TransactionID:  "txn-" + strconv.Itoa(index),
			IdempotencyKey: "event-" + strconv.Itoa(index),
			Progress:       []hatSql.SQLSinkProgress{{Sink: "warehouse", Partition: "0", Frontier: frontier}},
		}
		if prepared, err := coordinator.Prepare(context.Background(), commit, participant); err != nil || !prepared {
			b.Fatalf("Prepare() = %t/%v, want true/nil", prepared, err)
		}
		if committed, err := coordinator.Commit(context.Background(), commit, participant); err != nil || !committed {
			b.Fatalf("Commit() = %t/%v, want true/nil", committed, err)
		}
	}
}
