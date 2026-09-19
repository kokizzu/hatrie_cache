package hatSql_test

import (
	"context"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func BenchmarkSQLMultiSourceSnapshotControlResolve(b *testing.B) {
	control := map[string][]hatSql.Row{
		"POSTGRES\x00orders": {{"id": int64(1), "total": 10.5}},
		"CDC\x00customers":   {{"id": int64(9), "name": "Ada"}},
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		key := "POSTGRES\x00orders"
		if index&1 != 0 {
			key = "CDC\x00customers"
		}
		rows := mU04CloneRows(control[key])
		if len(rows) != 1 {
			b.Fatal("control lookup returned no rows")
		}
	}
}

func BenchmarkSQLMultiSourceSnapshotCapture(b *testing.B) {
	requests := mU04BenchmarkRequests()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		coordinator, err := hatSql.NewSQLMultiSourceSnapshotCoordinator(hatSql.SQLMultiSourceSnapshotCoordinatorOptions{MaxSources: 2, MaxRowsPerSource: 16, MaxOffsetsPerSource: 4, MaxPageRows: 4})
		if err != nil {
			b.Fatal(err)
		}
		store := &mU04SnapshotStore{}
		if _, err := coordinator.CaptureWithCheckpoint(context.Background(), requests, store, hatSql.SQLMultiSourceSnapshotCaptureOptions{RequireSnapshotIDs: true}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSQLMultiSourceSnapshotRecover(b *testing.B) {
	requests := mU04BenchmarkRequests()
	seedCoordinator, err := hatSql.NewSQLMultiSourceSnapshotCoordinator(hatSql.SQLMultiSourceSnapshotCoordinatorOptions{MaxSources: 2, MaxRowsPerSource: 16, MaxOffsetsPerSource: 4, MaxPageRows: 4})
	if err != nil {
		b.Fatal(err)
	}
	store := &mU04SnapshotStore{}
	if _, err := seedCoordinator.CaptureWithCheckpoint(context.Background(), requests, store, hatSql.SQLMultiSourceSnapshotCaptureOptions{RequireSnapshotIDs: true}); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		coordinator, err := hatSql.NewSQLMultiSourceSnapshotCoordinator(hatSql.SQLMultiSourceSnapshotCoordinatorOptions{MaxSources: 2, MaxRowsPerSource: 16, MaxOffsetsPerSource: 4, MaxPageRows: 4})
		if err != nil {
			b.Fatal(err)
		}
		if result, err := coordinator.CaptureWithCheckpoint(context.Background(), requests, store, hatSql.SQLMultiSourceSnapshotCaptureOptions{RequireSnapshotIDs: true}); err != nil || !result.Restored {
			b.Fatalf("recovery result = %#v error = %v", result, err)
		}
	}
}

func BenchmarkSQLMultiSourceSnapshotResolve(b *testing.B) {
	coordinator, err := hatSql.NewSQLMultiSourceSnapshotCoordinator(hatSql.SQLMultiSourceSnapshotCoordinatorOptions{MaxSources: 2, MaxRowsPerSource: 16, MaxOffsetsPerSource: 4, MaxPageRows: 4})
	if err != nil {
		b.Fatal(err)
	}
	if _, err := coordinator.CaptureWithCheckpoint(context.Background(), mU04BenchmarkRequests(), &mU04SnapshotStore{}, hatSql.SQLMultiSourceSnapshotCaptureOptions{RequireSnapshotIDs: true}); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		kind, key := "POSTGRES", "orders"
		if index&1 != 0 {
			kind, key = "CDC", "customers"
		}
		rows, err := coordinator.ResolveSQLSource(kind, key)
		if err != nil || len(rows) != 1 {
			b.Fatalf("resolve rows = %#v error = %v", rows, err)
		}
	}
}

func mU04BenchmarkRequests() []hatSql.SQLMultiSourceSnapshotRequest {
	return []hatSql.SQLMultiSourceSnapshotRequest{
		{
			Source: "orders-source",
			Key:    "orders",
			Kind:   "POSTGRES",
			Provider: &mU03SnapshotProvider{
				metadata: hatSql.SQLExternalSnapshotMetadata{
					Source: "orders-source", Key: "orders", Kind: "POSTGRES", SnapshotID: "orders-7",
					Offsets: []hatSql.SQLExternalSnapshotOffset{{Source: "orders-source", Partition: "wal", Offset: 7}},
				},
				pages: [][]hatSql.Row{{{"id": int64(1), "total": 10.5}}},
			},
		},
		{
			Source: "customers-source",
			Key:    "customers",
			Kind:   "CDC",
			Provider: &mU03SnapshotProvider{
				metadata: hatSql.SQLExternalSnapshotMetadata{
					Source: "customers-source", Key: "customers", Kind: "CDC", SnapshotID: "customers-3",
					Offsets: []hatSql.SQLExternalSnapshotOffset{{Source: "customers-source", Partition: "stream", Offset: 3}},
				},
				pages: [][]hatSql.Row{{{"id": int64(9), "name": "Ada"}}},
			},
		},
	}
}

func mU04CloneRows(rows []hatSql.Row) []hatSql.Row {
	cloned := make([]hatSql.Row, len(rows))
	for index, row := range rows {
		cloned[index] = make(hatSql.Row, len(row))
		for key, value := range row {
			cloned[index][key] = mU04CloneValue(value)
		}
	}
	return cloned
}

func mU04CloneValue(value interface{}) interface{} {
	switch typed := value.(type) {
	case hatSql.Row:
		return mU04CloneRows([]hatSql.Row{typed})[0]
	case map[string]interface{}:
		cloned := make(map[string]interface{}, len(typed))
		for key, nested := range typed {
			cloned[key] = mU04CloneValue(nested)
		}
		return cloned
	case []interface{}:
		cloned := make([]interface{}, len(typed))
		for index, nested := range typed {
			cloned[index] = mU04CloneValue(nested)
		}
		return cloned
	case []byte:
		return append([]byte(nil), typed...)
	default:
		return value
	}
}
