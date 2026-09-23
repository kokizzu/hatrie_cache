package hatSql

import (
	"context"
	"reflect"
	"testing"
)

func TestCH005RuntimeJoinPartitionFilterPreservesResultsAndPrunes(t *testing.T) {
	resolver := ch005RuntimeJoinPartitionResolver{
		left: []SQLRow{
			{"k": int64(2), "id": "left-2"},
			{"k": int64(3), "id": "left-3"},
		},
		partitions: []SQLSourcePartition{
			{Name: "cold", Rows: []SQLRow{{"k": int64(100), "id": "right-cold"}}},
			{Name: "hot", Rows: []SQLRow{{"k": int64(2), "id": "right-2"}, {"k": int64(3), "id": "right-3"}}},
		},
		partitionBounds: map[string][2]int64{"cold": {100, 100}, "hot": {2, 3}},
	}
	query := "FROM CACHE('left') AS l JOIN CACHE('right') AS r ON l.k = r.k SELECT l.id AS left_id, r.id AS right_id"
	baseline, err := ExecuteSQLQueryContext(context.Background(), query, &resolver, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("baseline join: %v", err)
	}
	resolver.boundsCalls = 0
	resolver.regularCalls = 0
	filtered, err := ExecuteSQLQueryContext(context.Background(), query, &resolver, SQLQueryOptions{RuntimeJoinPartitionFilter: true})
	if err != nil {
		t.Fatalf("runtime-filter join: %v", err)
	}
	if !reflect.DeepEqual(filtered, baseline) {
		t.Fatalf("filtered result = %#v, want %#v", filtered, baseline)
	}
	if resolver.boundsCalls != 1 || resolver.regularCalls != 1 {
		t.Fatalf("resolver calls = bounds %d regular %d, want one bounds call and one left-source call", resolver.boundsCalls, resolver.regularCalls)
	}
	if !reflect.DeepEqual(resolver.lastBounds, SQLRuntimeJoinBounds{Field: "k", Min: int64(2), Max: int64(3)}) {
		t.Fatalf("runtime bounds = %#v, want k=[2,3]", resolver.lastBounds)
	}
	if !reflect.DeepEqual(resolver.lastPartitions, []string{"hot"}) {
		t.Fatalf("selected partitions = %#v, want [hot]", resolver.lastPartitions)
	}
}

func TestCH005RuntimeJoinPartitionFilterIsOffByDefault(t *testing.T) {
	resolver := ch005RuntimeJoinPartitionResolver{
		left:            []SQLRow{{"k": int64(2), "id": "left"}},
		partitions:      []SQLSourcePartition{{Name: "hot", Rows: []SQLRow{{"k": int64(2), "id": "right"}}}},
		partitionBounds: map[string][2]int64{"hot": {2, 2}},
	}
	_, err := ExecuteSQLQueryContext(context.Background(), "FROM CACHE('left') AS l JOIN CACHE('right') AS r ON l.k = r.k SELECT l.id, r.id", &resolver, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("default join: %v", err)
	}
	if resolver.boundsCalls != 0 || resolver.regularCalls != 2 {
		t.Fatalf("resolver calls = bounds %d regular %d, want no bounds and two regular calls", resolver.boundsCalls, resolver.regularCalls)
	}
}

func TestCH005RuntimeJoinPartitionFilterFallsBackForMixedKeys(t *testing.T) {
	resolver := ch005RuntimeJoinPartitionResolver{
		left: []SQLRow{
			{"k": int64(2), "id": "left-number"},
			{"k": "3", "id": "left-text"},
		},
		partitions: []SQLSourcePartition{
			{Name: "keys", Rows: []SQLRow{{"k": int64(2), "id": "right-number"}, {"k": "3", "id": "right-text"}}},
		},
		partitionBounds: map[string][2]int64{"keys": {2, 3}},
	}
	query := "FROM CACHE('left') AS l JOIN CACHE('right') AS r ON l.k = r.k SELECT l.id AS left_id, r.id AS right_id"
	baseline, err := ExecuteSQLQueryContext(context.Background(), query, &resolver, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("baseline join: %v", err)
	}
	resolver.boundsCalls = 0
	resolver.regularCalls = 0
	filtered, err := ExecuteSQLQueryContext(context.Background(), query, &resolver, SQLQueryOptions{RuntimeJoinPartitionFilter: true})
	if err != nil {
		t.Fatalf("mixed-key join: %v", err)
	}
	if !reflect.DeepEqual(filtered, baseline) {
		t.Fatalf("mixed-key result = %#v, want %#v", filtered, baseline)
	}
	if resolver.boundsCalls != 0 || resolver.regularCalls != 2 {
		t.Fatalf("mixed-key resolver calls = bounds %d regular %d, want regular fallback", resolver.boundsCalls, resolver.regularCalls)
	}
}

func TestCH005RuntimeJoinPartitionFilterKeepsOuterJoinPath(t *testing.T) {
	resolver := ch005RuntimeJoinPartitionResolver{
		left:            []SQLRow{{"k": int64(2), "id": "left"}},
		partitions:      []SQLSourcePartition{{Name: "hot", Rows: []SQLRow{{"k": int64(2), "id": "right"}}}},
		partitionBounds: map[string][2]int64{"hot": {2, 2}},
	}
	_, err := ExecuteSQLQueryContext(context.Background(), "FROM CACHE('left') AS l LEFT JOIN CACHE('right') AS r ON l.k = r.k SELECT l.id, r.id", &resolver, SQLQueryOptions{RuntimeJoinPartitionFilter: true})
	if err != nil {
		t.Fatalf("left join: %v", err)
	}
	if resolver.boundsCalls != 0 || resolver.regularCalls != 2 {
		t.Fatalf("left-join resolver calls = bounds %d regular %d, want regular path", resolver.boundsCalls, resolver.regularCalls)
	}
}

func BenchmarkCH005RuntimeJoinPartitionFilterBaseline(b *testing.B) {
	benchmarkCH005RuntimeJoinPartitionFilter(b, SQLQueryOptions{})
}

func BenchmarkCH005RuntimeJoinPartitionFilter(b *testing.B) {
	benchmarkCH005RuntimeJoinPartitionFilter(b, SQLQueryOptions{RuntimeJoinPartitionFilter: true})
}

func benchmarkCH005RuntimeJoinPartitionFilter(b *testing.B, options SQLQueryOptions) {
	resolver := newCH005RuntimeJoinBenchmarkResolver()
	query := "FROM CACHE('left') AS l JOIN CACHE('right') AS r ON l.k = r.k SELECT l.id AS left_id, r.id AS right_id"
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := ExecuteSQLQueryContext(context.Background(), query, resolver, options); err != nil {
			b.Fatal(err)
		}
	}
}

func newCH005RuntimeJoinBenchmarkResolver() *ch005RuntimeJoinPartitionResolver {
	const (
		partitionCount = 128
		rowsPerPart    = 256
		selectedPart   = 97
	)
	resolver := &ch005RuntimeJoinPartitionResolver{
		left:            make([]SQLRow, 128),
		partitions:      make([]SQLSourcePartition, 0, partitionCount),
		partitionBounds: make(map[string][2]int64, partitionCount),
	}
	for index := range resolver.left {
		key := int64(selectedPart*rowsPerPart + 32 + index%32)
		resolver.left[index] = SQLRow{"k": key, "id": key}
	}
	for partitionIndex := 0; partitionIndex < partitionCount; partitionIndex++ {
		name := "partition-" + string(rune('a'+partitionIndex/26)) + string(rune('a'+partitionIndex%26))
		rows := make([]SQLRow, rowsPerPart)
		minimum := int64(partitionIndex * rowsPerPart)
		for rowIndex := range rows {
			key := minimum + int64(rowIndex)
			rows[rowIndex] = SQLRow{"k": key, "id": key}
		}
		resolver.partitions = append(resolver.partitions, SQLSourcePartition{Name: name, Rows: rows})
		resolver.partitionBounds[name] = [2]int64{minimum, minimum + rowsPerPart - 1}
	}
	return resolver
}

type ch005RuntimeJoinPartitionResolver struct {
	left            []SQLRow
	partitions      []SQLSourcePartition
	partitionBounds map[string][2]int64
	lastBounds      SQLRuntimeJoinBounds
	lastPartitions  []string
	boundsCalls     int
	regularCalls    int
}

func (resolver *ch005RuntimeJoinPartitionResolver) ResolveSQLSource(_ string, key string) ([]SQLRow, error) {
	resolver.regularCalls++
	if key == "left" {
		return resolver.left, nil
	}
	return flattenSQLSourcePartitions(resolver.partitions), nil
}

func (resolver *ch005RuntimeJoinPartitionResolver) ResolveSQLSourcePartitionsForJoinBounds(_ string, _ string, bounds SQLRuntimeJoinBounds) ([]SQLSourcePartition, bool, error) {
	resolver.boundsCalls++
	resolver.lastBounds = bounds
	resolver.lastPartitions = nil
	selected := make([]SQLSourcePartition, 0, len(resolver.partitions))
	minimum, maximum := bounds.Min.(int64), bounds.Max.(int64)
	for _, partition := range resolver.partitions {
		partitionBounds := resolver.partitionBounds[partition.Name]
		if partitionBounds[1] < minimum || partitionBounds[0] > maximum {
			continue
		}
		resolver.lastPartitions = append(resolver.lastPartitions, partition.Name)
		selected = append(selected, partition)
	}
	return selected, true, nil
}
