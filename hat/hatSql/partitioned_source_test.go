package hatSql

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

type partitionedSourceTestResolver struct {
	partitions      []SQLSourcePartition
	partitionedCall int
	regularCalls    int
	partitioned     bool
	partitionError  error
	rows            []Row
}

func (resolver *partitionedSourceTestResolver) ResolveSQLSource(name, key string) ([]Row, error) {
	if name != "CACHE" || key != "events" {
		return nil, nil
	}
	resolver.regularCalls++
	return resolver.rows, nil
}

func (resolver *partitionedSourceTestResolver) ResolveSQLSourcePartitions(name, key string) ([]SQLSourcePartition, bool, error) {
	if name != "CACHE" || key != "events" {
		return nil, false, nil
	}
	resolver.partitionedCall++
	if resolver.partitionError != nil {
		return nil, false, resolver.partitionError
	}
	return resolver.partitions, resolver.partitioned, nil
}

func TestPartitionedSourceResolverCombinesOrderedPartitions(t *testing.T) {
	resolver := &partitionedSourceTestResolver{
		partitioned: true,
		partitions: []SQLSourcePartition{
			{Name: "apac", Rows: []Row{{"id": int64(3), "region": "apac"}, {"id": int64(4), "region": "apac"}}},
			{Name: "eu", Rows: []Row{{"id": int64(2), "region": "eu"}}},
		},
	}
	result, err := ExecuteSQLQueryContext(
		context.Background(),
		"FROM CACHE('events') SELECT id, region ORDER BY id",
		resolver,
		SQLQueryOptions{},
	)
	if err != nil {
		t.Fatalf("ExecuteSQLQueryContext() error = %v", err)
	}
	want := []Row{
		{"id": int64(2), "region": "eu"},
		{"id": int64(3), "region": "apac"},
		{"id": int64(4), "region": "apac"},
	}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("result.Rows = %#v, want %#v", result.Rows, want)
	}
	if resolver.partitionedCall != 1 || resolver.regularCalls != 0 {
		t.Fatalf("resolver calls = partitioned %d, regular %d; want 1, 0", resolver.partitionedCall, resolver.regularCalls)
	}
}

func TestPartitionedSourceResolverFallsBackToRegularSource(t *testing.T) {
	resolver := &partitionedSourceTestResolver{
		rows: []Row{{"id": int64(7)}},
	}
	result, err := ExecuteSQLQueryContext(
		context.Background(),
		"FROM CACHE('events') SELECT id",
		resolver,
		SQLQueryOptions{},
	)
	if err != nil {
		t.Fatalf("ExecuteSQLQueryContext() error = %v", err)
	}
	if want := []Row{{"id": int64(7)}}; !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("result.Rows = %#v, want %#v", result.Rows, want)
	}
	if resolver.partitionedCall != 1 || resolver.regularCalls != 1 {
		t.Fatalf("resolver calls = partitioned %d, regular %d; want 1, 1", resolver.partitionedCall, resolver.regularCalls)
	}
}

func TestCatalogResolverForwardsPartitionedSource(t *testing.T) {
	resolver := &partitionedSourceTestResolver{
		partitioned: true,
		partitions:  []SQLSourcePartition{{Rows: []Row{{"id": int64(9)}}}},
	}
	result, err := ExecuteSQLQueryContext(
		context.Background(),
		"FROM CACHE('events') SELECT id",
		CatalogResolver{Source: resolver},
		SQLQueryOptions{},
	)
	if err != nil {
		t.Fatalf("ExecuteSQLQueryContext() error = %v", err)
	}
	if want := []Row{{"id": int64(9)}}; !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("result.Rows = %#v, want %#v", result.Rows, want)
	}
	if resolver.partitionedCall != 1 || resolver.regularCalls != 0 {
		t.Fatalf("resolver calls = partitioned %d, regular %d; want 1, 0", resolver.partitionedCall, resolver.regularCalls)
	}
}

func TestSQLSessionForwardsPartitionsAndPreservesTemporaryTablePrecedence(t *testing.T) {
	resolver := &partitionedSourceTestResolver{
		partitioned: true,
		partitions:  []SQLSourcePartition{{Rows: []Row{{"id": int64(12)}}}},
	}
	session := NewSQLSession(resolver)
	result, err := ExecuteSQLQueryContext(
		context.Background(),
		"FROM CACHE('events') SELECT id",
		session,
		SQLQueryOptions{},
	)
	if err != nil {
		t.Fatalf("partitioned session query error = %v", err)
	}
	if want := []Row{{"id": int64(12)}}; !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("partitioned session rows = %#v, want %#v", result.Rows, want)
	}
	if err := session.CreateTemporaryTable("events", []Row{{"id": int64(13)}}); err != nil {
		t.Fatalf("CreateTemporaryTable() error = %v", err)
	}
	result, err = ExecuteSQLQueryContext(
		context.Background(),
		"FROM CACHE('events') SELECT id",
		session,
		SQLQueryOptions{},
	)
	if err != nil {
		t.Fatalf("temporary-table session query error = %v", err)
	}
	if want := []Row{{"id": int64(13)}}; !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("temporary-table session rows = %#v, want %#v", result.Rows, want)
	}
	if resolver.regularCalls != 0 {
		t.Fatalf("underlying regular source calls = %d, want 0", resolver.regularCalls)
	}
}

func TestPartitionedSourceResolverReturnsPartitionError(t *testing.T) {
	wantErr := errors.New("partition unavailable")
	resolver := &partitionedSourceTestResolver{partitionError: wantErr}
	_, err := ExecuteSQLQueryContext(
		context.Background(),
		"FROM CACHE('events') SELECT id",
		resolver,
		SQLQueryOptions{},
	)
	if !errors.Is(err, wantErr) {
		t.Fatalf("ExecuteSQLQueryContext() error = %v, want %v", err, wantErr)
	}
	if resolver.regularCalls != 0 {
		t.Fatalf("regular source calls = %d, want 0 after partition error", resolver.regularCalls)
	}
}

func TestPartitionedSourceResolverTreatsAvailableEmptyAsAnEmptySource(t *testing.T) {
	resolver := &partitionedSourceTestResolver{
		partitioned: true,
		rows:        []Row{{"id": int64(99)}},
	}
	result, err := ExecuteSQLQueryContext(
		context.Background(),
		"FROM CACHE('events') SELECT COUNT(*) AS total",
		resolver,
		SQLQueryOptions{},
	)
	if err != nil {
		t.Fatalf("ExecuteSQLQueryContext() error = %v", err)
	}
	if len(result.Rows) != 1 || result.Rows[0]["total"] != int64(0) {
		t.Fatalf("empty partition result = %#v, want count 0", result.Rows)
	}
	if resolver.regularCalls != 0 {
		t.Fatalf("regular source calls = %d, want 0", resolver.regularCalls)
	}
}

var partitionedSourceBenchmarkResult SQLQueryResult

func BenchmarkPartitionedSourceResolver(b *testing.B) {
	const (
		partitionCount = 8
		rowsPerPart    = 256
	)
	rows := make([]Row, 0, partitionCount*rowsPerPart)
	partitions := make([]SQLSourcePartition, partitionCount)
	for partition := range partitions {
		partitions[partition].Name = "partition-" + string(rune('a'+partition))
		partitions[partition].Rows = make([]Row, 0, rowsPerPart)
		for row := 0; row < rowsPerPart; row++ {
			item := Row{"id": int64(partition*rowsPerPart + row), "partition": int64(partition)}
			partitions[partition].Rows = append(partitions[partition].Rows, item)
			rows = append(rows, item)
		}
	}
	query := "FROM CACHE('events') SELECT COUNT(*) AS total"
	for _, test := range []struct {
		name        string
		partitioned bool
		partitions  []SQLSourcePartition
		rows        []Row
	}{
		{name: "regular", rows: rows},
		{name: "partitioned", partitioned: true, partitions: partitions},
	} {
		b.Run(test.name, func(b *testing.B) {
			resolver := &partitionedSourceTestResolver{
				partitioned: test.partitioned,
				partitions:  test.partitions,
				rows:        test.rows,
			}
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				result, err := ExecuteSQLQueryContext(context.Background(), query, resolver, SQLQueryOptions{})
				if err != nil {
					b.Fatal(err)
				}
				if len(result.Rows) != 1 || result.Rows[0]["total"] != int64(partitionCount*rowsPerPart) {
					b.Fatalf("result = %#v", result.Rows)
				}
				partitionedSourceBenchmarkResult = result
			}
		})
	}
}
