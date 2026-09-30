package hatSql_test

import (
	"context"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestM227ExternalSnapshotCarriesOffsetsWithFirstLiveFrontier(t *testing.T) {
	provider := &m227SnapshotProvider{
		metadata: hatSql.SQLExternalSnapshotMetadata{
			Source:               "orders-source",
			Key:                  "orders",
			Kind:                 "CDC",
			SnapshotID:           "snapshot-7",
			FirstLiveFrontier:    0,
			FirstLiveFrontierSet: true,
			Offsets:              []hatSql.SQLExternalSnapshotOffset{{Source: "orders-source", Partition: "0", Offset: 7}},
		},
		rows: []hatSql.Row{{"id": int64(1)}},
	}
	store := &m227SnapshotStore{}
	ingestor, err := hatSql.NewSQLExternalSnapshotIngestor(hatSql.SQLExternalSnapshotIngestorOptions{
		Source: "orders-source",
		Key:    "orders",
		Kind:   "CDC",
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := ingestor.IngestSnapshotWithCheckpoint(context.Background(), provider, store, hatSql.SQLExternalSnapshotIngestOptions{RequireSnapshotID: true})
	if err != nil {
		t.Fatal(err)
	}
	if result.FirstLiveFrontier != 0 || !result.FirstLiveFrontierSet || store.snapshot.Metadata.FirstLiveFrontier != 0 || !store.snapshot.Metadata.FirstLiveFrontierSet {
		t.Fatalf("captured first live frontier = %#v/%#v, want set 0", result, store.snapshot.Metadata)
	}
	if len(store.snapshot.Metadata.Offsets) != 1 || store.snapshot.Metadata.Offsets[0].Offset != 7 {
		t.Fatalf("captured offsets = %#v", store.snapshot.Metadata.Offsets)
	}

	restored, err := hatSql.NewSQLExternalSnapshotIngestor(hatSql.SQLExternalSnapshotIngestorOptions{
		Source: "orders-source",
		Key:    "orders",
		Kind:   "CDC",
	})
	if err != nil {
		t.Fatal(err)
	}
	restoreResult, err := restored.IngestSnapshotWithCheckpoint(context.Background(), nil, store, hatSql.SQLExternalSnapshotIngestOptions{RequireSnapshotID: true})
	if err != nil {
		t.Fatal(err)
	}
	if !restoreResult.Restored || restoreResult.FirstLiveFrontier != 0 || !restoreResult.FirstLiveFrontierSet {
		t.Fatalf("restore result = %#v, want restored frontier 0", restoreResult)
	}
}

type m227SnapshotProvider struct {
	metadata hatSql.SQLExternalSnapshotMetadata
	rows     []hatSql.Row
}

func (provider *m227SnapshotProvider) Authenticate(context.Context) error {
	return nil
}

func (provider *m227SnapshotProvider) Snapshot(_ context.Context, sink hatSql.SQLExternalSnapshotSink) (hatSql.SQLExternalSnapshotMetadata, error) {
	if err := sink.AppendRows(provider.rows); err != nil {
		return hatSql.SQLExternalSnapshotMetadata{}, err
	}
	return provider.metadata, nil
}

type m227SnapshotStore struct {
	snapshot hatSql.SQLExternalSnapshot
	found    bool
}

func (store *m227SnapshotStore) Load(context.Context, string, string) (hatSql.SQLExternalSnapshot, bool, error) {
	return store.snapshot, store.found, nil
}

func (store *m227SnapshotStore) Commit(_ context.Context, snapshot hatSql.SQLExternalSnapshot) error {
	store.snapshot = snapshot
	store.found = true
	return nil
}
