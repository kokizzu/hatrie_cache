package hatSql_test

import (
	"strconv"
	"testing"

	hatSql "hatrie_cache/hat/hatSql"
)

func BenchmarkSQLSinkCommitCoordinatorAuditedCommit(b *testing.B) {
	audit := hatSql.NewSQLSinkDeliveryAudit(hatSql.SQLSinkDeliveryAuditOptions{Capacity: 1024})
	coordinator := hatSql.NewSQLSinkCommitCoordinatorWithOptions(hatSql.SQLSinkCommitCoordinatorOptions{Audit: audit})
	commit := hatSql.SQLSinkCommit{
		Sink:     "warehouse",
		Progress: []hatSql.SQLSinkProgress{{Sink: "warehouse", Partition: "0", Frontier: 10}},
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		commit.TransactionID = strconv.Itoa(index)
		if committed, err := coordinator.Commit(commit, func() error { return nil }); err != nil || !committed {
			b.Fatalf("Commit() = %t/%v, want true/nil", committed, err)
		}
	}
}

func BenchmarkSQLSinkDeliveryAuditMarshalBinary(b *testing.B) {
	audit := hatSql.NewSQLSinkDeliveryAudit(hatSql.SQLSinkDeliveryAuditOptions{Capacity: 1024})
	for index := 0; index < 64; index++ {
		if err := audit.Record(hatSql.SQLSinkDeliveryEvent{
			Sink:          "warehouse",
			TransactionID: strconv.Itoa(index),
			Outcome:       hatSql.SQLSinkDeliveryCommitted,
			Progress:      []hatSql.SQLSinkProgress{{Sink: "warehouse", Partition: "0", Frontier: uint64(index)}},
		}); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		payload, err := audit.MarshalBinary()
		if err != nil {
			b.Fatal(err)
		}
		b.SetBytes(int64(len(payload)))
	}
}
