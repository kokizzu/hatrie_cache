package hatSql_test

import (
	"strconv"
	"testing"

	hatSql "hatrie_cache/hat/hatSql"
)

func BenchmarkSQLSourceIngestionCoordinatorNewIngestion(b *testing.B) {
	coordinator := hatSql.NewSQLSourceIngestionCoordinator()
	ingestion := hatSql.SQLSourceIngestion{
		Source: "events",
		Transaction: hatSql.SQLSourceTransaction{
			Offsets: []hatSql.SQLSourceOffset{{Source: "events", Partition: "0", Offset: 10}},
		},
	}
	apply := func() error { return nil }
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		ingestion.Transaction.ID = strconv.Itoa(index)
		if ingested, err := coordinator.Ingest(ingestion, apply); err != nil || !ingested {
			b.Fatalf("Ingest() = %t/%v, want true/nil", ingested, err)
		}
	}
}

func BenchmarkSQLSourceIngestionCoordinatorDuplicateIngestion(b *testing.B) {
	coordinator := hatSql.NewSQLSourceIngestionCoordinator()
	ingestion := hatSql.SQLSourceIngestion{
		Source: "events",
		Transaction: hatSql.SQLSourceTransaction{
			ID:      "txn-1",
			Offsets: []hatSql.SQLSourceOffset{{Source: "events", Partition: "0", Offset: 10}},
		},
	}
	if ingested, err := coordinator.Ingest(ingestion, func() error { return nil }); err != nil || !ingested {
		b.Fatalf("seed Ingest() = %t/%v, want true/nil", ingested, err)
	}
	apply := func() error { return nil }
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if ingested, err := coordinator.Ingest(ingestion, apply); err != nil || ingested {
			b.Fatalf("duplicate Ingest() = %t/%v, want false/nil", ingested, err)
		}
	}
}
