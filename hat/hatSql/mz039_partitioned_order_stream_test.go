package hatSql

import (
	"context"
	"reflect"
	"testing"
)

type mz39PartitionedOrderResolver struct {
	rows         []Row
	partitions   []SQLSourcePartition
	resolveCalls int
	orderedCalls int
}

func (resolver *mz39PartitionedOrderResolver) ResolveSQLSource(string, string) ([]Row, error) {
	resolver.resolveCalls++
	return resolver.rows, nil
}

func (resolver *mz39PartitionedOrderResolver) ResolveSQLOrderedSourcePartitions(_, _ string, _ string, desc, _, _ bool) ([]SQLSourcePartition, bool, error) {
	resolver.orderedCalls++
	if !desc {
		return resolver.partitions, true, nil
	}
	partitions := make([]SQLSourcePartition, len(resolver.partitions))
	for index, partition := range resolver.partitions {
		partitions[index] = partition
		partitions[index].Rows = append([]SQLRow(nil), partition.Rows...)
		for left, right := 0, len(partitions[index].Rows)-1; left < right; left, right = left+1, right-1 {
			partitions[index].Rows[left], partitions[index].Rows[right] = partitions[index].Rows[right], partitions[index].Rows[left]
		}
	}
	return partitions, true, nil
}

func TestMZ39PartitionedOrderStreamUsesOrderedPartitions(t *testing.T) {
	resolver := mz39PartitionedOrderFixture(4, 32)
	var got []Row
	err := ExecuteSQLQueryRows(
		context.Background(),
		`FROM CACHE('events') SELECT id, score ORDER BY score LIMIT 7`,
		&resolver,
		nil,
		SQLQueryOptions{},
		func(_ []string, row SQLRow) error {
			got = append(got, row)
			return nil
		},
	)
	if err != nil {
		t.Fatalf("ExecuteSQLQueryRows() error = %v", err)
	}
	want := []Row{
		{"id": 0, "score": 0},
		{"id": 32, "score": 1},
		{"id": 64, "score": 2},
		{"id": 96, "score": 3},
		{"id": 1, "score": 4},
		{"id": 33, "score": 5},
		{"id": 65, "score": 6},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("rows = %#v, want %#v", got, want)
	}
	if resolver.orderedCalls != 1 {
		t.Fatalf("ordered partition calls = %d, want 1", resolver.orderedCalls)
	}
	if resolver.resolveCalls != 0 {
		t.Fatalf("full-source calls = %d, want 0", resolver.resolveCalls)
	}
}

func TestMZ39PartitionedOrderStreamPreservesFallback(t *testing.T) {
	rows := []Row{{"id": 2, "score": 2}, {"id": 1, "score": 1}}
	result, err := ExecuteSQLQuery(
		`FROM CACHE('events') SELECT id, score ORDER BY score`,
		SourceResolverFunc(func(string, string) ([]Row, error) { return rows, nil }),
	)
	if err != nil {
		t.Fatalf("fallback ExecuteSQLQuery() error = %v", err)
	}
	want := []Row{{"id": 1, "score": 1}, {"id": 2, "score": 2}}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("fallback rows = %#v, want %#v", result.Rows, want)
	}
}

func TestMZ39PartitionedOrderStreamRejectsNonBinaryCollation(t *testing.T) {
	query, err := parseSQLQuery(`FROM CACHE('events') SELECT id, score ORDER BY score`)
	if err != nil {
		t.Fatal(err)
	}
	query.orderBy[0].collation = SQLCollation("NOCASE")
	if sqlPartitionedOrderStreamShape(query) {
		t.Fatal("non-binary collation must use the existing fallback")
	}
}

func TestMZ39PartitionedOrderStreamPreservesFilterOffsetAndDescendingOrder(t *testing.T) {
	resolver := mz39PartitionedOrderFixture(4, 8)
	got := collectMZ39Rows(t, `FROM CACHE('events') SELECT id, score WHERE score >= 10 ORDER BY score DESC LIMIT 4 OFFSET 2`, &resolver)
	want := []Row{
		{"id": 15, "score": 29},
		{"id": 7, "score": 28},
		{"id": 30, "score": 27},
		{"id": 22, "score": 26},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("rows = %#v, want %#v", got, want)
	}
	if resolver.orderedCalls != 1 || resolver.resolveCalls != 0 {
		t.Fatalf("resolver calls = ordered %d, full %d; want ordered 1, full 0", resolver.orderedCalls, resolver.resolveCalls)
	}
}

func collectMZ39Rows(t *testing.T, query string, resolver SQLSourceResolver) []Row {
	t.Helper()
	var rows []Row
	err := ExecuteSQLQueryRows(context.Background(), query, resolver, nil, SQLQueryOptions{}, func(_ []string, row SQLRow) error {
		rows = append(rows, row)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

func BenchmarkMZ39PartitionedOrderBaseline(b *testing.B) {
	fixture := mz39PartitionedOrderFixture(16, 4096)
	resolver := SourceResolverFunc(func(string, string) ([]Row, error) { return fixture.rows, nil })
	benchmarkMZ39PartitionedOrder(b, resolver)
}

func BenchmarkMZ39PartitionedOrderStream(b *testing.B) {
	resolver := mz39PartitionedOrderFixture(16, 4096)
	benchmarkMZ39PartitionedOrder(b, &resolver)
}

func benchmarkMZ39PartitionedOrder(b *testing.B, resolver SQLSourceResolver) {
	b.Helper()
	query := `FROM CACHE('events') SELECT id, score ORDER BY score LIMIT 64`
	for b.Loop() {
		rows := 0
		if err := ExecuteSQLQueryRows(context.Background(), query, resolver, nil, SQLQueryOptions{}, func(_ []string, _ SQLRow) error {
			rows++
			return nil
		}); err != nil {
			b.Fatal(err)
		}
		if rows != 64 {
			b.Fatalf("rows = %d, want 64", rows)
		}
	}
}

func mz39PartitionedOrderFixture(partitionCount, rowsPerPartition int) mz39PartitionedOrderResolver {
	partitions := make([]SQLSourcePartition, partitionCount)
	rows := make([]Row, 0, partitionCount*rowsPerPartition)
	id := 0
	for partition := range partitions {
		partitions[partition].Name = "partition-" + string(rune('a'+partition))
		partitions[partition].Rows = make([]Row, rowsPerPartition)
		for offset := range partitions[partition].Rows {
			score := offset*partitionCount + partition
			row := Row{"id": id, "score": score}
			partitions[partition].Rows[offset] = row
			rows = append(rows, row)
			id++
		}
	}
	return mz39PartitionedOrderResolver{rows: rows, partitions: partitions}
}
