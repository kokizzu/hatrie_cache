package hatSql_test

import (
	"context"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func BenchmarkSQLExternalSnapshotControl(b *testing.B) {
	rows := mU03BenchmarkRows()
	sources := hatSql.NewVirtualSources()
	if err := sources.Register("orders", hatSql.VirtualSourceFunc(func() ([]hatSql.Row, error) {
		return rows, nil
	})); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := sources.ResolveSQLVirtualSource("orders"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSQLExternalSnapshotResolve(b *testing.B) {
	ingestor := mU03BenchmarkIngestor(b)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := ingestor.ResolveSQLSource("POSTGRES", "orders"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSQLExternalSnapshotIngest(b *testing.B) {
	rows := mU03BenchmarkRows()
	provider := &mU03BenchmarkProvider{
		metadata: hatSql.SQLExternalSnapshotMetadata{
			Source:     "orders-source",
			Key:        "orders",
			Kind:       "POSTGRES",
			SnapshotID: "lsn-1",
		},
		rows: rows,
	}
	store := &mU03BenchmarkStore{}
	ingestor, err := hatSql.NewSQLExternalSnapshotIngestor(hatSql.SQLExternalSnapshotIngestorOptions{Source: "orders-source", Key: "orders", Kind: "POSTGRES"})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := ingestor.IngestSnapshotWithCheckpoint(nil, provider, store, hatSql.SQLExternalSnapshotIngestOptions{RequireSnapshotID: true, AllowReplaceExisting: true}); err != nil {
			b.Fatal(err)
		}
	}
}

func mU03BenchmarkIngestor(b *testing.B) *hatSql.SQLExternalSnapshotIngestor {
	b.Helper()
	ingestor, err := hatSql.NewSQLExternalSnapshotIngestor(hatSql.SQLExternalSnapshotIngestorOptions{Source: "orders-source", Key: "orders", Kind: "POSTGRES"})
	if err != nil {
		b.Fatal(err)
	}
	provider := &mU03BenchmarkProvider{
		metadata: hatSql.SQLExternalSnapshotMetadata{Source: "orders-source", Key: "orders", Kind: "POSTGRES", SnapshotID: "lsn-1"},
		rows:     mU03BenchmarkRows(),
	}
	if _, err := ingestor.IngestSnapshotWithCheckpoint(nil, provider, &mU03BenchmarkStore{}, hatSql.SQLExternalSnapshotIngestOptions{RequireSnapshotID: true}); err != nil {
		b.Fatal(err)
	}
	return ingestor
}

func mU03BenchmarkRows() []hatSql.Row {
	rows := make([]hatSql.Row, 128)
	for index := range rows {
		rows[index] = hatSql.Row{
			"id":     int64(index),
			"name":   "order",
			"total":  float64(index) / 10,
			"active": index%2 == 0,
		}
	}
	return rows
}

type mU03BenchmarkProvider struct {
	metadata hatSql.SQLExternalSnapshotMetadata
	rows     []hatSql.Row
}

func (provider *mU03BenchmarkProvider) Authenticate(context.Context) error { return nil }

func (provider *mU03BenchmarkProvider) Snapshot(_ context.Context, sink hatSql.SQLExternalSnapshotSink) (hatSql.SQLExternalSnapshotMetadata, error) {
	if err := sink.AppendRows(provider.rows); err != nil {
		return hatSql.SQLExternalSnapshotMetadata{}, err
	}
	return provider.metadata, nil
}

type mU03BenchmarkStore struct{}

func (*mU03BenchmarkStore) Load(context.Context, string, string) (hatSql.SQLExternalSnapshot, bool, error) {
	return hatSql.SQLExternalSnapshot{}, false, nil
}

func (*mU03BenchmarkStore) Commit(context.Context, hatSql.SQLExternalSnapshot) error { return nil }
