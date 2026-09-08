package hatSql

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

type partitionedSourceTestResolver struct {
	partitions       []SQLSourcePartition
	prunedPartitions []SQLSourcePartition
	partitionedCall  int
	predicateCalls   int
	regularCalls     int
	partitioned      bool
	pruneAvailable   bool
	partitionError   error
	rows             []Row
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

func (resolver *partitionedSourceTestResolver) ResolveSQLSourcePartitionsForPredicate(name, key string, predicate SQLPartitionPredicate) ([]SQLSourcePartition, bool, error) {
	if name != "CACHE" || key != "events" || !resolver.pruneAvailable {
		return nil, false, nil
	}
	resolver.predicateCalls++
	if predicate.Field != "region" {
		return nil, false, nil
	}
	if predicate.Operator == "=" && len(predicate.Values) == 1 && predicate.Values[0] == "apac" {
		return resolver.prunedPartitions, true, nil
	}
	if predicate.Operator == "IN" && reflect.DeepEqual(predicate.Values, []interface{}{"apac", "eu"}) {
		return resolver.partitions, true, nil
	}
	return nil, false, nil
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

func TestCatalogResolverForwardsPartitionPruning(t *testing.T) {
	resolver := &partitionedSourceTestResolver{
		partitioned:      true,
		pruneAvailable:   true,
		partitions:       []SQLSourcePartition{{Rows: []Row{{"id": int64(1), "region": "apac"}}}, {Rows: []Row{{"id": int64(2), "region": "eu"}}}},
		prunedPartitions: []SQLSourcePartition{{Rows: []Row{{"id": int64(1), "region": "apac"}}}},
	}
	result, err := ExecuteSQLQueryContext(
		context.Background(),
		"FROM CACHE('events') SELECT id WHERE region = 'apac'",
		CatalogResolver{Source: resolver},
		SQLQueryOptions{},
	)
	if err != nil {
		t.Fatalf("ExecuteSQLQueryContext() error = %v", err)
	}
	if len(result.Rows) != 1 || result.Rows[0]["id"] != int64(1) {
		t.Fatalf("result.Rows = %#v, want one APAC row", result.Rows)
	}
	if resolver.predicateCalls != 1 || resolver.partitionedCall != 0 {
		t.Fatalf("resolver calls = predicate %d, partitions %d; want 1, 0", resolver.predicateCalls, resolver.partitionedCall)
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

func TestSQLSessionForwardsPartitionPruning(t *testing.T) {
	resolver := &partitionedSourceTestResolver{
		partitioned:      true,
		pruneAvailable:   true,
		partitions:       []SQLSourcePartition{{Rows: []Row{{"id": int64(1), "region": "apac"}}}, {Rows: []Row{{"id": int64(2), "region": "eu"}}}},
		prunedPartitions: []SQLSourcePartition{{Rows: []Row{{"id": int64(1), "region": "apac"}}}},
	}
	result, err := ExecuteSQLQueryContext(
		context.Background(),
		"FROM CACHE('events') SELECT id WHERE region = 'apac'",
		NewSQLSession(resolver),
		SQLQueryOptions{},
	)
	if err != nil {
		t.Fatalf("ExecuteSQLQueryContext() error = %v", err)
	}
	if len(result.Rows) != 1 || result.Rows[0]["id"] != int64(1) {
		t.Fatalf("result.Rows = %#v, want one APAC row", result.Rows)
	}
	if resolver.predicateCalls != 1 || resolver.partitionedCall != 0 {
		t.Fatalf("resolver calls = predicate %d, partitions %d; want 1, 0", resolver.predicateCalls, resolver.partitionedCall)
	}
}

func TestPartitionPruningResolverAppliesToExecuteSQLQueryRows(t *testing.T) {
	resolver := &partitionedSourceTestResolver{
		partitioned:      true,
		pruneAvailable:   true,
		partitions:       []SQLSourcePartition{{Rows: []Row{{"id": int64(1), "region": "apac"}}}, {Rows: []Row{{"id": int64(2), "region": "eu"}}}},
		prunedPartitions: []SQLSourcePartition{{Rows: []Row{{"id": int64(1), "region": "apac"}}}},
	}
	var rows []Row
	err := ExecuteSQLQueryRows(
		context.Background(),
		"FROM CACHE('events') SELECT id WHERE region = 'apac'",
		resolver,
		nil,
		SQLQueryOptions{},
		func(_ []string, row Row) error {
			rows = append(rows, row)
			return nil
		},
	)
	if err != nil {
		t.Fatalf("ExecuteSQLQueryRows() error = %v", err)
	}
	if want := []Row{{"id": int64(1)}}; !reflect.DeepEqual(rows, want) {
		t.Fatalf("rows = %#v, want %#v", rows, want)
	}
	if resolver.predicateCalls != 1 || resolver.partitionedCall != 0 || resolver.regularCalls != 0 {
		t.Fatalf("resolver calls = predicate %d, partitions %d, regular %d; want 1, 0, 0", resolver.predicateCalls, resolver.partitionedCall, resolver.regularCalls)
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

func TestPartitionPruningResolverUsesLiteralEqualityConjunct(t *testing.T) {
	resolver := &partitionedSourceTestResolver{
		partitioned:      true,
		pruneAvailable:   true,
		partitions:       []SQLSourcePartition{{Name: "apac", Rows: []Row{{"id": int64(1), "region": "apac"}}}, {Name: "eu", Rows: []Row{{"id": int64(2), "region": "eu"}}}},
		prunedPartitions: []SQLSourcePartition{{Name: "apac", Rows: []Row{{"id": int64(1), "region": "apac"}}}},
	}
	result, err := ExecuteSQLQueryContext(
		context.Background(),
		"FROM CACHE('events') SELECT id, region WHERE region = 'apac'",
		resolver,
		SQLQueryOptions{},
	)
	if err != nil {
		t.Fatalf("ExecuteSQLQueryContext() error = %v", err)
	}
	want := []Row{{"id": int64(1), "region": "apac"}}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("result.Rows = %#v, want %#v", result.Rows, want)
	}
	if resolver.predicateCalls != 1 || resolver.partitionedCall != 0 || resolver.regularCalls != 0 {
		t.Fatalf("resolver calls = predicate %d, partitions %d, regular %d; want 1, 0, 0", resolver.predicateCalls, resolver.partitionedCall, resolver.regularCalls)
	}
}

func TestPartitionPruningResolverUsesLiteralINPredicate(t *testing.T) {
	resolver := &partitionedSourceTestResolver{
		partitioned:    true,
		pruneAvailable: true,
		partitions:     []SQLSourcePartition{{Name: "apac", Rows: []Row{{"id": int64(1), "region": "apac"}}}, {Name: "eu", Rows: []Row{{"id": int64(2), "region": "eu"}}}, {Name: "us", Rows: []Row{{"id": int64(3), "region": "us"}}}},
	}
	result, err := ExecuteSQLQueryContext(
		context.Background(),
		"FROM CACHE('events') SELECT id, region WHERE region IN ('apac', 'eu') ORDER BY id",
		resolver,
		SQLQueryOptions{},
	)
	if err != nil {
		t.Fatalf("ExecuteSQLQueryContext() error = %v", err)
	}
	want := []Row{{"id": int64(1), "region": "apac"}, {"id": int64(2), "region": "eu"}}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("result.Rows = %#v, want %#v", result.Rows, want)
	}
	if resolver.predicateCalls != 1 || resolver.partitionedCall != 0 || resolver.regularCalls != 0 {
		t.Fatalf("resolver calls = predicate %d, partitions %d, regular %d; want 1, 0, 0", resolver.predicateCalls, resolver.partitionedCall, resolver.regularCalls)
	}
}

func TestPartitionPruningResolverDoesNotPruneORPredicate(t *testing.T) {
	resolver := &partitionedSourceTestResolver{
		partitioned:    true,
		pruneAvailable: true,
		partitions:     []SQLSourcePartition{{Name: "apac", Rows: []Row{{"id": int64(1), "region": "apac"}}}, {Name: "eu", Rows: []Row{{"id": int64(2), "region": "eu"}}}},
	}
	result, err := ExecuteSQLQueryContext(
		context.Background(),
		"FROM CACHE('events') SELECT id, region WHERE region = 'apac' OR region = 'eu' ORDER BY id",
		resolver,
		SQLQueryOptions{},
	)
	if err != nil {
		t.Fatalf("ExecuteSQLQueryContext() error = %v", err)
	}
	if len(result.Rows) != 2 {
		t.Fatalf("result.Rows = %#v, want both partitions", result.Rows)
	}
	if resolver.predicateCalls != 0 || resolver.partitionedCall != 1 {
		t.Fatalf("resolver calls = predicate %d, partitions %d; want 0, 1", resolver.predicateCalls, resolver.partitionedCall)
	}
}

func TestSQLQueryPartitionPredicatesSkipTABLESAMPLE(t *testing.T) {
	query := &sqlQuery{
		from: &sqlSource{kind: "CACHE", alias: "events"},
		where: sqlExpr{
			kind:  "binary",
			op:    "=",
			left:  &sqlExpr{kind: "field", name: "region"},
			right: &sqlExpr{kind: "literal", value: "apac"},
		},
		sample: &sqlTableSample{},
	}
	if predicates := sqlQueryPartitionPredicates(query); len(predicates) != 0 {
		t.Fatalf("TABLESAMPLE predicates = %#v, want none", predicates)
	}
}

func TestPartitionPruningResolverSkipsTABLESAMPLE(t *testing.T) {
	resolver := &partitionedSourceTestResolver{
		partitioned:    true,
		pruneAvailable: true,
		partitions: []SQLSourcePartition{
			{Name: "apac", Rows: []Row{{"id": int64(1), "region": "apac"}}},
			{Name: "eu", Rows: []Row{{"id": int64(2), "region": "eu"}}},
		},
		prunedPartitions: []SQLSourcePartition{{Name: "apac", Rows: []Row{{"id": int64(1), "region": "apac"}}}},
	}
	result, err := ExecuteSQLQueryContext(
		context.Background(),
		"SELECT id, region FROM CACHE('events') TABLESAMPLE BERNOULLI (100) REPEATABLE (7) WHERE region = 'apac'",
		resolver,
		SQLQueryOptions{},
	)
	if err != nil {
		t.Fatalf("ExecuteSQLQueryContext() error = %v", err)
	}
	want := []Row{{"id": int64(1), "region": "apac"}}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("result.Rows = %#v, want %#v", result.Rows, want)
	}
	if resolver.predicateCalls != 0 || resolver.partitionedCall != 1 {
		t.Fatalf("resolver calls = predicate %d, partitions %d; want 0, 1", resolver.predicateCalls, resolver.partitionedCall)
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

func BenchmarkPartitionPruningSourceResolver(b *testing.B) {
	const (
		partitionCount = 8
		rowsPerPart    = 256
	)
	partitions := make([]SQLSourcePartition, partitionCount)
	for partition := range partitions {
		region := "eu"
		if partition == 0 {
			region = "apac"
		}
		partitions[partition].Name = "partition-" + string(rune('a'+partition))
		partitions[partition].Rows = make([]Row, 0, rowsPerPart)
		for row := 0; row < rowsPerPart; row++ {
			partitions[partition].Rows = append(partitions[partition].Rows, Row{
				"id":     int64(partition*rowsPerPart + row),
				"region": region,
			})
		}
	}
	query := "FROM CACHE('events') SELECT id WHERE region = 'apac'"
	for _, test := range []struct {
		name           string
		pruneAvailable bool
		pruned         []SQLSourcePartition
	}{
		{name: "without_pruning"},
		{name: "with_pruning", pruneAvailable: true, pruned: partitions[:1]},
	} {
		b.Run(test.name, func(b *testing.B) {
			resolver := &partitionedSourceTestResolver{
				partitioned:      true,
				pruneAvailable:   test.pruneAvailable,
				partitions:       partitions,
				prunedPartitions: test.pruned,
			}
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				result, err := ExecuteSQLQueryContext(context.Background(), query, resolver, SQLQueryOptions{})
				if err != nil {
					b.Fatal(err)
				}
				if len(result.Rows) != rowsPerPart {
					b.Fatalf("result rows = %d, want %d", len(result.Rows), rowsPerPart)
				}
				partitionedSourceBenchmarkResult = result
			}
		})
	}
}
