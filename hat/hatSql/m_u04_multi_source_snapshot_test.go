package hatSql_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestSQLMultiSourceSnapshotCoordinatorPublishesAtomicViewAndRecovers(t *testing.T) {
	orders := &mU03SnapshotProvider{
		metadata: hatSql.SQLExternalSnapshotMetadata{
			Source:     "orders-source",
			Key:        "orders",
			Kind:       "POSTGRES",
			SnapshotID: "orders-lsn-7",
			Offsets:    []hatSql.SQLExternalSnapshotOffset{{Source: "orders-source", Partition: "wal", Offset: 7}},
		},
		pages: [][]hatSql.Row{{{"id": int64(1), "total": 10.5}}},
	}
	customers := &mU03SnapshotProvider{
		metadata: hatSql.SQLExternalSnapshotMetadata{
			Source:     "customers-source",
			Key:        "customers",
			Kind:       "CDC",
			SnapshotID: "customers-tx-3",
			Offsets:    []hatSql.SQLExternalSnapshotOffset{{Source: "customers-source", Partition: "stream", Offset: 3}},
		},
		pages: [][]hatSql.Row{{{"id": int64(9), "name": "Ada"}}},
	}
	requests := []hatSql.SQLMultiSourceSnapshotRequest{
		{Source: "orders-source", Key: "orders", Kind: "POSTGRES", Provider: orders},
		{Source: "customers-source", Key: "customers", Kind: "CDC", Provider: customers},
	}
	store := &mU04SnapshotStore{}
	coordinator, err := hatSql.NewSQLMultiSourceSnapshotCoordinator(hatSql.SQLMultiSourceSnapshotCoordinatorOptions{MaxSources: 2})
	if err != nil {
		t.Fatal(err)
	}

	result, err := coordinator.CaptureWithCheckpoint(context.Background(), requests, store, hatSql.SQLMultiSourceSnapshotCaptureOptions{RequireSnapshotIDs: true})
	if err != nil {
		t.Fatal(err)
	}
	if result.Restored || !result.Checkpointed || result.Generation != 1 || len(result.Snapshot.Sources) != 2 || result.View == nil {
		t.Fatalf("capture result = %#v", result)
	}
	if store.commitCalls != 1 || orders.authenticateCalls != 1 || customers.authenticateCalls != 1 {
		t.Fatalf("capture calls = store %d orders auth %d customers auth %d", store.commitCalls, orders.authenticateCalls, customers.authenticateCalls)
	}
	if got := result.Snapshot.Sources[0].Metadata.Source; got != "customers-source" {
		t.Fatalf("sources are not deterministic: %#v", result.Snapshot.Sources)
	}
	ordersRows, err := result.View.ResolveSQLSource("POSTGRES", "orders")
	if err != nil {
		t.Fatal(err)
	}
	customersRows, err := result.View.ResolveSQLSource("CDC", "customers")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(ordersRows, []hatSql.Row{{"id": int64(1), "total": 10.5}}) || !reflect.DeepEqual(customersRows, []hatSql.Row{{"id": int64(9), "name": "Ada"}}) {
		t.Fatalf("atomic view rows = orders %#v customers %#v", ordersRows, customersRows)
	}

	recovered, err := hatSql.NewSQLMultiSourceSnapshotCoordinator(hatSql.SQLMultiSourceSnapshotCoordinatorOptions{MaxSources: 2})
	if err != nil {
		t.Fatal(err)
	}
	failingOrders := &mU03SnapshotProvider{authenticateErr: errors.New("upstream unavailable")}
	recoveryRequests := []hatSql.SQLMultiSourceSnapshotRequest{
		{Source: "orders-source", Key: "orders", Kind: "POSTGRES", Provider: failingOrders},
		{Source: "customers-source", Key: "customers", Kind: "CDC", Provider: &mU03SnapshotProvider{authenticateErr: errors.New("upstream unavailable")}},
	}
	recovery, err := recovered.CaptureWithCheckpoint(context.Background(), recoveryRequests, store, hatSql.SQLMultiSourceSnapshotCaptureOptions{RequireSnapshotIDs: true})
	if err != nil {
		t.Fatal(err)
	}
	if !recovery.Restored || recovery.Checkpointed || recovery.Generation != 1 || failingOrders.authenticateCalls != 0 {
		t.Fatalf("recovery result = %#v, auth calls = %d", recovery, failingOrders.authenticateCalls)
	}
	if rows, err := recovery.View.ResolveSQLSource("CDC", "customers"); err != nil || len(rows) != 1 {
		t.Fatalf("recovered customer view = %#v/%v", rows, err)
	}
}

func TestSQLMultiSourceSnapshotCoordinatorFailureIsAtomicAndRejectsDuplicates(t *testing.T) {
	coordinator, err := hatSql.NewSQLMultiSourceSnapshotCoordinator(hatSql.SQLMultiSourceSnapshotCoordinatorOptions{MaxSources: 2})
	if err != nil {
		t.Fatal(err)
	}
	initialStore := &mU04SnapshotStore{}
	initialRequests := []hatSql.SQLMultiSourceSnapshotRequest{
		{Source: "a-source", Key: "a", Kind: "CDC", Provider: mU04Provider("a-source", "a", "a-1")},
		{Source: "b-source", Key: "b", Kind: "CDC", Provider: mU04Provider("b-source", "b", "b-1")},
	}
	if _, err := coordinator.CaptureWithCheckpoint(context.Background(), initialRequests, initialStore, hatSql.SQLMultiSourceSnapshotCaptureOptions{}); err != nil {
		t.Fatal(err)
	}
	before, _ := coordinator.Snapshot()

	failing := []hatSql.SQLMultiSourceSnapshotRequest{
		{Source: "a-source", Key: "a", Kind: "CDC", Provider: mU04Provider("a-source", "a", "a-2")},
		{Source: "b-source", Key: "b", Kind: "CDC", Provider: &mU03SnapshotProvider{authenticateErr: errors.New("denied")}},
	}
	failedStore := &mU04SnapshotStore{}
	if _, err := coordinator.CaptureWithCheckpoint(context.Background(), failing, failedStore, hatSql.SQLMultiSourceSnapshotCaptureOptions{}); !errors.Is(err, hatSql.ErrSQLMultiSourceSnapshotAuthentication) {
		t.Fatalf("failed capture error = %v", err)
	}
	after, _ := coordinator.Snapshot()
	if !reflect.DeepEqual(after, before) || failedStore.commitCalls != 0 {
		t.Fatalf("failed capture changed view/store: before=%#v after=%#v commits=%d", before, after, failedStore.commitCalls)
	}

	duplicate := []hatSql.SQLMultiSourceSnapshotRequest{
		{Source: "same", Key: "one", Kind: "CDC", Provider: mU04Provider("same", "one", "1")},
		{Source: "same", Key: "two", Kind: "CDC", Provider: mU04Provider("same", "two", "2")},
	}
	if _, err := coordinator.CaptureWithCheckpoint(context.Background(), duplicate, &mU04SnapshotStore{}, hatSql.SQLMultiSourceSnapshotCaptureOptions{}); !errors.Is(err, hatSql.ErrSQLMultiSourceSnapshotDuplicateSource) {
		t.Fatalf("duplicate source error = %v", err)
	}
}

func TestSQLMultiSourceSnapshotCoordinatorCommitFailureDoesNotPublish(t *testing.T) {
	coordinator, err := hatSql.NewSQLMultiSourceSnapshotCoordinator(hatSql.SQLMultiSourceSnapshotCoordinatorOptions{MaxSources: 1})
	if err != nil {
		t.Fatal(err)
	}
	store := &mU04SnapshotStore{commitErr: errors.New("disk full")}
	_, err = coordinator.CaptureWithCheckpoint(context.Background(), []hatSql.SQLMultiSourceSnapshotRequest{
		{Source: "a-source", Key: "a", Kind: "CDC", Provider: mU04Provider("a-source", "a", "a-1")},
	}, store, hatSql.SQLMultiSourceSnapshotCaptureOptions{})
	if !errors.Is(err, hatSql.ErrSQLMultiSourceSnapshotCommit) {
		t.Fatalf("commit error = %v", err)
	}
	if snapshot, generation := coordinator.Snapshot(); generation != 0 || len(snapshot.Sources) != 0 {
		t.Fatalf("failed commit published snapshot = %#v generation = %d", snapshot, generation)
	}
}

func mU04Provider(source, key, snapshotID string) *mU03SnapshotProvider {
	return &mU03SnapshotProvider{
		metadata: hatSql.SQLExternalSnapshotMetadata{Source: source, Key: key, Kind: "CDC", SnapshotID: snapshotID},
		pages:    [][]hatSql.Row{{{"id": int64(1)}}},
	}
}

type mU04SnapshotStore struct {
	snapshot    hatSql.SQLMultiSourceSnapshot
	found       bool
	commitCalls int
	commitErr   error
}

func (store *mU04SnapshotStore) Load(context.Context) (hatSql.SQLMultiSourceSnapshot, bool, error) {
	return store.snapshot, store.found, nil
}

func (store *mU04SnapshotStore) Commit(_ context.Context, snapshot hatSql.SQLMultiSourceSnapshot) error {
	store.commitCalls++
	if store.commitErr != nil {
		return store.commitErr
	}
	store.snapshot = snapshot
	store.found = true
	return nil
}
