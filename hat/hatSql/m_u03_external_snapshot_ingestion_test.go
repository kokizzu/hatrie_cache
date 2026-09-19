package hatSql_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestSQLExternalSnapshotIngestionPagesAndRecoversFromCheckpoint(t *testing.T) {
	provider := &mU03SnapshotProvider{
		metadata: hatSql.SQLExternalSnapshotMetadata{
			Source:     "orders-source",
			Key:        "orders",
			Kind:       "POSTGRES",
			SnapshotID: "lsn-42",
			Offsets:    []hatSql.SQLExternalSnapshotOffset{{Source: "orders-source", Partition: "wal", Offset: 42}},
		},
		pages: [][]hatSql.Row{
			{{"id": int64(1), "total": 10.5}},
			{{"id": int64(2), "total": 20.5}},
		},
	}
	store := &mU03SnapshotStore{}
	ingestor, err := hatSql.NewSQLExternalSnapshotIngestor(hatSql.SQLExternalSnapshotIngestorOptions{
		Source: "orders-source",
		Key:    "orders",
		Kind:   "POSTGRES",
	})
	if err != nil {
		t.Fatal(err)
	}

	result, err := ingestor.IngestSnapshotWithCheckpoint(context.Background(), provider, store, hatSql.SQLExternalSnapshotIngestOptions{RequireSnapshotID: true})
	if err != nil {
		t.Fatal(err)
	}
	if result.SnapshotID != "lsn-42" || result.Rows != 2 || result.Offsets != 1 || result.Pages != 2 || !result.Checkpointed || result.Restored {
		t.Fatalf("ingestion result = %#v", result)
	}
	if provider.authenticateCalls != 1 || provider.snapshotCalls != 1 || store.commitCalls != 1 {
		t.Fatalf("provider/store calls = auth %d snapshot %d commit %d", provider.authenticateCalls, provider.snapshotCalls, store.commitCalls)
	}
	rows, err := ingestor.ResolveSQLSource("POSTGRES", "orders")
	if err != nil {
		t.Fatal(err)
	}
	if want := []hatSql.Row{{"id": int64(1), "total": 10.5}, {"id": int64(2), "total": 20.5}}; !reflect.DeepEqual(rows, want) {
		t.Fatalf("ingested rows = %#v, want %#v", rows, want)
	}

	recovered, err := hatSql.NewSQLExternalSnapshotIngestor(hatSql.SQLExternalSnapshotIngestorOptions{
		Source: "orders-source",
		Key:    "orders",
		Kind:   "POSTGRES",
	})
	if err != nil {
		t.Fatal(err)
	}
	failingProvider := &mU03SnapshotProvider{authenticateErr: errors.New("upstream unavailable")}
	recovery, err := recovered.IngestSnapshotWithCheckpoint(context.Background(), failingProvider, store, hatSql.SQLExternalSnapshotIngestOptions{RequireSnapshotID: true})
	if err != nil {
		t.Fatal(err)
	}
	if !recovery.Restored || recovery.SnapshotID != "lsn-42" || recovery.Rows != 2 || recovery.Offsets != 1 || failingProvider.authenticateCalls != 0 {
		t.Fatalf("recovery result = %#v, auth calls = %d", recovery, failingProvider.authenticateCalls)
	}
	recoveredRows, err := recovered.ResolveSQLSource("POSTGRES", "orders")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(recoveredRows, rows) {
		t.Fatalf("recovered rows = %#v, want %#v", recoveredRows, rows)
	}
}

func TestSQLExternalSnapshotIngestionBoundsAndRollback(t *testing.T) {
	ingestor, err := hatSql.NewSQLExternalSnapshotIngestor(hatSql.SQLExternalSnapshotIngestorOptions{
		Source: "orders-source",
		Key:    "orders",
		Kind:   "CDC",
	})
	if err != nil {
		t.Fatal(err)
	}
	provider := &mU03SnapshotProvider{
		metadata: hatSql.SQLExternalSnapshotMetadata{Source: "orders-source", Key: "orders", Kind: "CDC", SnapshotID: "change-1"},
		pages:    [][]hatSql.Row{{{"id": int64(1)}, {"id": int64(2)}}},
	}
	store := &mU03SnapshotStore{commitErr: errors.New("checkpoint unavailable")}
	if _, err := ingestor.IngestSnapshotWithCheckpoint(context.Background(), provider, store, hatSql.SQLExternalSnapshotIngestOptions{MaxRows: 1}); !errors.Is(err, hatSql.ErrSQLExternalSnapshotRowsLimit) {
		t.Fatalf("row limit error = %v", err)
	}
	if provider.authenticateCalls != 1 || provider.snapshotCalls != 1 || store.commitCalls != 0 {
		t.Fatalf("bounded snapshot calls = auth %d snapshot %d commit %d", provider.authenticateCalls, provider.snapshotCalls, store.commitCalls)
	}

	provider.pages = [][]hatSql.Row{{{"id": int64(1)}}}
	if _, err := ingestor.IngestSnapshotWithCheckpoint(context.Background(), provider, store, hatSql.SQLExternalSnapshotIngestOptions{RequireSnapshotID: true}); !errors.Is(err, hatSql.ErrSQLExternalSnapshotCheckpointCommit) {
		t.Fatalf("checkpoint failure = %v", err)
	}
	rows, err := ingestor.ResolveSQLSource("CDC", "orders")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Fatalf("failed checkpoint mutated rows = %#v", rows)
	}

	store.commitErr = nil
	provider.metadata.SnapshotID = ""
	if _, err := ingestor.IngestSnapshotWithCheckpoint(context.Background(), provider, store, hatSql.SQLExternalSnapshotIngestOptions{RequireSnapshotID: true}); !errors.Is(err, hatSql.ErrSQLExternalSnapshotIDRequired) {
		t.Fatalf("missing snapshot ID error = %v", err)
	}

	provider.metadata.SnapshotID = "change-2"
	provider.pages = [][]hatSql.Row{{{"id": int64(1)}}}
	if _, err := ingestor.IngestSnapshotWithCheckpoint(context.Background(), provider, store, hatSql.SQLExternalSnapshotIngestOptions{MaxPageRows: 0}); err != nil {
		t.Fatalf("default page limit rejected valid page: %v", err)
	}
}

func TestSQLExternalSnapshotIngestionRejectsIdentityAndAuthenticationFailures(t *testing.T) {
	ingestor, err := hatSql.NewSQLExternalSnapshotIngestor(hatSql.SQLExternalSnapshotIngestorOptions{
		Source: "orders-source",
		Key:    "orders",
		Kind:   "KAFKA",
	})
	if err != nil {
		t.Fatal(err)
	}
	store := &mU03SnapshotStore{}
	provider := &mU03SnapshotProvider{
		authenticateErr: errors.New("bad credentials"),
		metadata:        hatSql.SQLExternalSnapshotMetadata{Source: "wrong-source", Key: "orders", Kind: "KAFKA", SnapshotID: "x"},
	}
	if _, err := ingestor.IngestSnapshotWithCheckpoint(context.Background(), provider, store, hatSql.SQLExternalSnapshotIngestOptions{}); !errors.Is(err, hatSql.ErrSQLExternalSnapshotAuthentication) {
		t.Fatalf("authentication error = %v", err)
	}
	if provider.snapshotCalls != 0 {
		t.Fatalf("snapshot called after authentication failure: %d", provider.snapshotCalls)
	}

	provider.authenticateErr = nil
	if _, err := ingestor.IngestSnapshotWithCheckpoint(context.Background(), provider, store, hatSql.SQLExternalSnapshotIngestOptions{}); !errors.Is(err, hatSql.ErrSQLExternalSnapshotIdentity) {
		t.Fatalf("identity error = %v", err)
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	provider.metadata.Source = "orders-source"
	if _, err := ingestor.IngestSnapshotWithCheckpoint(canceled, provider, store, hatSql.SQLExternalSnapshotIngestOptions{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled error = %v", err)
	}
}

func TestSQLExternalSnapshotIngestionCanonicalizesOffsetsAndCopiesRows(t *testing.T) {
	nested := map[string]interface{}{"region": "ap-southeast"}
	input := hatSql.Row{"id": int64(1), "meta": nested, "payload": []byte("snapshot")}
	provider := &mU03SnapshotProvider{
		metadata: hatSql.SQLExternalSnapshotMetadata{
			Source:     "orders-source",
			Key:        "orders",
			Kind:       "CDC",
			SnapshotID: "change-9",
			Offsets: []hatSql.SQLExternalSnapshotOffset{
				{Source: "orders-source", Partition: "z", Offset: 9},
				{Source: "orders-source", Partition: "a", Offset: 8},
			},
		},
		pages: [][]hatSql.Row{{input}},
	}
	ingestor, err := hatSql.NewSQLExternalSnapshotIngestor(hatSql.SQLExternalSnapshotIngestorOptions{Source: "orders-source", Key: "orders", Kind: "CDC"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ingestor.IngestSnapshotWithCheckpoint(context.Background(), provider, &mU03SnapshotStore{}, hatSql.SQLExternalSnapshotIngestOptions{}); err != nil {
		t.Fatal(err)
	}
	input["id"] = int64(99)
	nested["region"] = "changed"
	input["payload"].([]byte)[0] = 'X'
	if offsets := ingestor.Offsets(); len(offsets) != 2 || offsets[0].Partition != "a" || offsets[1].Partition != "z" {
		t.Fatalf("offsets = %#v, want deterministic order", offsets)
	}
	rows, err := ingestor.ResolveSQLSource("CDC", "orders")
	if err != nil {
		t.Fatal(err)
	}
	if rows[0]["id"] != int64(1) || rows[0]["meta"].(map[string]interface{})["region"] != "ap-southeast" || string(rows[0]["payload"].([]byte)) != "snapshot" {
		t.Fatalf("source rows were aliased: %#v", rows)
	}

	duplicateProvider := &mU03SnapshotProvider{
		metadata: hatSql.SQLExternalSnapshotMetadata{
			Source:     "orders-source",
			Key:        "orders",
			Kind:       "CDC",
			SnapshotID: "duplicate",
			Offsets: []hatSql.SQLExternalSnapshotOffset{
				{Source: "orders-source", Partition: "a", Offset: 1},
				{Source: "orders-source", Partition: "a", Offset: 2},
			},
		},
	}
	second, err := hatSql.NewSQLExternalSnapshotIngestor(hatSql.SQLExternalSnapshotIngestorOptions{Source: "orders-source", Key: "orders", Kind: "CDC"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := second.IngestSnapshotWithCheckpoint(context.Background(), duplicateProvider, &mU03SnapshotStore{}, hatSql.SQLExternalSnapshotIngestOptions{}); !errors.Is(err, hatSql.ErrSQLExternalSnapshotOffsetDuplicate) {
		t.Fatalf("duplicate offset error = %v", err)
	}
}

type mU03SnapshotProvider struct {
	metadata          hatSql.SQLExternalSnapshotMetadata
	pages             [][]hatSql.Row
	authenticateErr   error
	authenticateCalls int
	snapshotCalls     int
}

func (provider *mU03SnapshotProvider) Authenticate(context.Context) error {
	provider.authenticateCalls++
	return provider.authenticateErr
}

func (provider *mU03SnapshotProvider) Snapshot(ctx context.Context, sink hatSql.SQLExternalSnapshotSink) (hatSql.SQLExternalSnapshotMetadata, error) {
	provider.snapshotCalls++
	for _, page := range provider.pages {
		if err := sink.AppendRows(page); err != nil {
			return hatSql.SQLExternalSnapshotMetadata{}, err
		}
		if err := ctx.Err(); err != nil {
			return hatSql.SQLExternalSnapshotMetadata{}, err
		}
	}
	return provider.metadata, nil
}

type mU03SnapshotStore struct {
	snapshot    hatSql.SQLExternalSnapshot
	found       bool
	commitErr   error
	commitCalls int
}

func (store *mU03SnapshotStore) Load(context.Context, string, string) (hatSql.SQLExternalSnapshot, bool, error) {
	return store.snapshot, store.found, nil
}

func (store *mU03SnapshotStore) Commit(_ context.Context, snapshot hatSql.SQLExternalSnapshot) error {
	store.commitCalls++
	if store.commitErr != nil {
		return store.commitErr
	}
	store.snapshot = snapshot
	store.found = true
	return nil
}
