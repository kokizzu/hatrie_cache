package hatSql_test

import (
	"strconv"
	"testing"

	hatSql "hatrie_cache/hat/hatSql"
)

func BenchmarkSQLSinkCommitCoordinatorNewCommit(b *testing.B) {
	coordinator := hatSql.NewSQLSinkCommitCoordinator()
	commit := hatSql.SQLSinkCommit{
		Sink:     "warehouse",
		Progress: []hatSql.SQLSinkProgress{{Sink: "warehouse", Partition: "0", Frontier: 10}},
	}
	apply := func() error { return nil }
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		commit.TransactionID = strconv.Itoa(index)
		if committed, err := coordinator.Commit(commit, apply); err != nil || !committed {
			b.Fatalf("Commit() = %t/%v, want true/nil", committed, err)
		}
	}
}

func BenchmarkSQLSinkCommitCoordinatorDuplicateCommit(b *testing.B) {
	coordinator := hatSql.NewSQLSinkCommitCoordinator()
	commit := hatSql.SQLSinkCommit{
		Sink:          "warehouse",
		TransactionID: "txn-1",
		Progress:      []hatSql.SQLSinkProgress{{Sink: "warehouse", Partition: "0", Frontier: 10}},
	}
	if committed, err := coordinator.Commit(commit, func() error { return nil }); err != nil || !committed {
		b.Fatalf("seed Commit() = %t/%v, want true/nil", committed, err)
	}
	apply := func() error { return nil }
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if committed, err := coordinator.Commit(commit, apply); err != nil || committed {
			b.Fatalf("duplicate Commit() = %t/%v, want false/nil", committed, err)
		}
	}
}
