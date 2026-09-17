package hatSql_test

import (
	"strconv"
	"testing"

	hatSql "hatrie_cache/hat/hatSql"
)

func BenchmarkMU018BaselineExistingCommitCoordinator(b *testing.B) {
	coordinator := hatSql.NewSQLSinkCommitCoordinator()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		sequence := uint64(index + 1)
		commit := hatSql.SQLSinkCommit{
			Sink:          "warehouse",
			TransactionID: "txn-" + strconv.Itoa(index),
			Progress:      []hatSql.SQLSinkProgress{{Sink: "warehouse", Partition: "0", Frontier: sequence}},
		}
		if committed, err := coordinator.Commit(commit, func() error { return nil }); err != nil || !committed {
			b.Fatalf("Commit() = %t/%v, want true/nil", committed, err)
		}
	}
}
