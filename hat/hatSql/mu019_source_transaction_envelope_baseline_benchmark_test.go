package hatSql_test

import (
	"strconv"
	"testing"

	hatSql "hatrie_cache/hat/hatSql"
)

func BenchmarkMU019BeforeSourceIngestion(b *testing.B) {
	const coordinatorWindow = 256
	coordinator := hatSql.NewSQLSourceIngestionCoordinator()
	var ingestion hatSql.SQLSourceIngestion
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if index%coordinatorWindow == 0 {
			coordinator = hatSql.NewSQLSourceIngestionCoordinator()
		}
		ingestion = hatSql.SQLSourceIngestion{
			Source: "events",
			Transaction: hatSql.SQLSourceTransaction{
				ID: strconv.Itoa(index),
				Offsets: []hatSql.SQLSourceOffset{
					{Source: "events", Partition: "0", Offset: uint64(index + 1)},
					{Source: "events", Partition: "1", Offset: uint64(index + 1)},
				},
			},
		}
		if ingested, err := coordinator.Ingest(ingestion, func() error { return nil }); err != nil || !ingested {
			b.Fatalf("Ingest() = %t/%v, want true/nil", ingested, err)
		}
	}
}

func BenchmarkMU019AfterSourceTransactionEnvelope(b *testing.B) {
	const coordinatorWindow = 256
	coordinator := hatSql.NewSQLSourceIngestionCoordinator()
	var envelope hatSql.SQLSourceTransactionEnvelope
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if index%coordinatorWindow == 0 {
			coordinator = hatSql.NewSQLSourceIngestionCoordinator()
		}
		envelope = hatSql.SQLSourceTransactionEnvelope{
			Source: "events",
			Transaction: hatSql.SQLSourceTransaction{
				ID: strconv.Itoa(index),
				Offsets: []hatSql.SQLSourceOffset{
					{Source: "events", Partition: "0", Offset: uint64(index + 1)},
					{Source: "events", Partition: "1", Offset: uint64(index + 1)},
				},
			},
			Relations: []string{"users", "orders"},
		}
		if ingested, err := coordinator.IngestEnvelope(envelope, func() error { return nil }); err != nil || !ingested {
			b.Fatalf("IngestEnvelope() = %t/%v, want true/nil", ingested, err)
		}
	}
}
