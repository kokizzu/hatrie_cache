package hatSql_test

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestSQLPartitionedKeysetPaginationMergesOrderedPartitions(t *testing.T) {
	resolver := &partitionedKeysetResolver{
		partitions: []hatSql.SQLSourcePartition{
			{Name: "apac", Rows: []hatSql.Row{
				{"id": "a-1", "kind": "keep", "score": int64(1)},
				{"id": "a-2", "kind": "skip", "score": int64(4)},
				{"id": "a-3", "kind": "keep", "score": int64(7)},
			}},
			{Name: "eu", Rows: []hatSql.Row{
				{"id": "e-1", "kind": "keep", "score": int64(2)},
				{"id": "e-2", "kind": "keep", "score": int64(3)},
				{"id": "e-3", "kind": "keep", "score": int64(8)},
			}},
		},
	}
	query := "SELECT e.id, e.score FROM CACHE('events') AS e WHERE e.kind = 'keep' ORDER BY e.score LIMIT 5"

	first, err := hatSql.ExecuteSQLQueryKeysetPage(context.Background(), query, resolver, nil, hatSql.SQLQueryOptions{}, 2, "")
	if err != nil {
		t.Fatal(err)
	}
	second, err := hatSql.ExecuteSQLQueryKeysetPage(context.Background(), query, resolver, nil, hatSql.SQLQueryOptions{}, 2, first.NextCursor)
	if err != nil {
		t.Fatal(err)
	}
	third, err := hatSql.ExecuteSQLQueryKeysetPage(context.Background(), query, resolver, nil, hatSql.SQLQueryOptions{}, 2, second.NextCursor)
	if err != nil {
		t.Fatal(err)
	}

	wantFirst := []hatSql.Row{{"id": "a-1", "score": int64(1)}, {"id": "e-1", "score": int64(2)}}
	wantSecond := []hatSql.Row{{"id": "e-2", "score": int64(3)}, {"id": "a-3", "score": int64(7)}}
	wantThird := []hatSql.Row{{"id": "e-3", "score": int64(8)}}
	if !reflect.DeepEqual(first.Rows, wantFirst) || !reflect.DeepEqual(second.Rows, wantSecond) || !reflect.DeepEqual(third.Rows, wantThird) {
		t.Fatalf("partitioned keyset pages = %#v / %#v / %#v, want %#v / %#v / %#v", first.Rows, second.Rows, third.Rows, wantFirst, wantSecond, wantThird)
	}
	if !first.HasMore || !second.HasMore || third.HasMore || third.NextCursor != "" {
		t.Fatalf("partitioned keyset page state = (%t,%q), (%t,%q), (%t,%q)", first.HasMore, first.NextCursor, second.HasMore, second.NextCursor, third.HasMore, third.NextCursor)
	}
	if resolver.orderedCalls != 3 || resolver.regularCalls != 0 {
		t.Fatalf("resolver calls = ordered %d, regular %d; want 3, 0", resolver.orderedCalls, resolver.regularCalls)
	}
}

type partitionedKeysetResolver struct {
	partitions   []hatSql.SQLSourcePartition
	orderedCalls int
	regularCalls int
}

func (resolver *partitionedKeysetResolver) ResolveSQLSource(name, key string) ([]hatSql.Row, error) {
	if name != "CACHE" || key != "events" {
		return nil, nil
	}
	resolver.regularCalls++
	return nil, fmt.Errorf("regular source path should not be used")
}

func (resolver *partitionedKeysetResolver) ResolveSQLOrderedSourcePartitions(name, key, field string, desc, nullsFirst, nullsLast bool) ([]hatSql.SQLSourcePartition, bool, error) {
	if name != "CACHE" || key != "events" || field != "score" {
		return nil, false, fmt.Errorf("unexpected ordered partition request")
	}
	resolver.orderedCalls++
	return resolver.partitions, true, nil
}

func BenchmarkSQLPartitionedKeysetPagination(b *testing.B) {
	partitions := benchmarkOrderedPartitions(16, 2048)
	query := "SELECT e.id, e.score FROM CACHE('events') AS e ORDER BY e.score"
	b.Run("FlattenedOffsetPage", func(b *testing.B) {
		resolver := &partitionedBaselineResolver{partitions: partitions}
		b.ReportAllocs()
		for index := 0; index < b.N; index++ {
			if _, err := hatSql.ExecuteSQLQueryPage(context.Background(), query, resolver, nil, hatSql.SQLQueryOptions{}, 100, ""); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("PartitionedKeysetMerge", func(b *testing.B) {
		resolver := &partitionedKeysetResolver{partitions: partitions}
		b.ReportAllocs()
		for index := 0; index < b.N; index++ {
			if _, err := hatSql.ExecuteSQLQueryKeysetPage(context.Background(), query, resolver, nil, hatSql.SQLQueryOptions{}, 100, ""); err != nil {
				b.Fatal(err)
			}
		}
	})
}

type partitionedBaselineResolver struct {
	partitions []hatSql.SQLSourcePartition
}

func (resolver *partitionedBaselineResolver) ResolveSQLSource(name, key string) ([]hatSql.Row, error) {
	return nil, fmt.Errorf("unexpected regular source resolution for %s/%s", name, key)
}

func (resolver *partitionedBaselineResolver) ResolveSQLSourcePartitions(name, key string) ([]hatSql.SQLSourcePartition, bool, error) {
	if name != "CACHE" || key != "events" {
		return nil, false, nil
	}
	return resolver.partitions, true, nil
}

func benchmarkOrderedPartitions(partitionCount, rowsPerPartition int) []hatSql.SQLSourcePartition {
	partitions := make([]hatSql.SQLSourcePartition, partitionCount)
	for partitionIndex := range partitions {
		rows := make([]hatSql.Row, rowsPerPartition)
		for rowIndex := range rows {
			score := int64(rowIndex*partitionCount + partitionIndex)
			rows[rowIndex] = hatSql.Row{"id": fmt.Sprintf("%d-%d", partitionIndex, rowIndex), "score": score}
		}
		partitions[partitionIndex] = hatSql.SQLSourcePartition{Name: fmt.Sprintf("partition-%d", partitionIndex), Rows: rows}
	}
	return partitions
}

func TestSQLPartitionedKeysetPaginationPreservesDescendingTieOrder(t *testing.T) {
	resolver := &partitionedKeysetResolver{
		partitions: []hatSql.SQLSourcePartition{
			{Name: "apac", Rows: []hatSql.Row{
				{"id": "a-7", "score": int64(7)},
				{"id": "a-5", "score": int64(5)},
				{"id": "a-null", "score": nil},
			}},
			{Name: "eu", Rows: []hatSql.Row{
				{"id": "e-7", "score": int64(7)},
				{"id": "e-6", "score": int64(6)},
				{"id": "e-null", "score": nil},
			}},
		},
	}
	query := "SELECT e.id, e.score FROM CACHE('events') AS e ORDER BY e.score DESC NULLS LAST"
	first, err := hatSql.ExecuteSQLQueryKeysetPage(context.Background(), query, resolver, nil, hatSql.SQLQueryOptions{}, 3, "")
	if err != nil {
		t.Fatal(err)
	}
	second, err := hatSql.ExecuteSQLQueryKeysetPage(context.Background(), query, resolver, nil, hatSql.SQLQueryOptions{}, 3, first.NextCursor)
	if err != nil {
		t.Fatal(err)
	}
	wantFirst := []hatSql.Row{{"id": "a-7", "score": int64(7)}, {"id": "e-7", "score": int64(7)}, {"id": "e-6", "score": int64(6)}}
	wantSecond := []hatSql.Row{{"id": "a-5", "score": int64(5)}, {"id": "a-null", "score": nil}, {"id": "e-null", "score": nil}}
	if !reflect.DeepEqual(first.Rows, wantFirst) || !reflect.DeepEqual(second.Rows, wantSecond) {
		t.Fatalf("descending pages = %#v / %#v, want %#v / %#v", first.Rows, second.Rows, wantFirst, wantSecond)
	}
	if !first.HasMore || second.HasMore || second.NextCursor != "" {
		t.Fatalf("descending page state = (%t,%q), (%t,%q)", first.HasMore, first.NextCursor, second.HasMore, second.NextCursor)
	}
}

func TestSQLPartitionedKeysetPaginationRejectsUnorderedPartition(t *testing.T) {
	resolver := &partitionedKeysetResolver{
		partitions: []hatSql.SQLSourcePartition{{Name: "apac", Rows: []hatSql.Row{{"id": "a-2", "score": int64(2)}, {"id": "a-1", "score": int64(1)}}}},
	}
	_, err := hatSql.ExecuteSQLQueryKeysetPage(context.Background(), "SELECT e.id FROM CACHE('events') AS e ORDER BY e.score", resolver, nil, hatSql.SQLQueryOptions{}, 2, "")
	if err == nil {
		t.Fatal("unordered partition was accepted")
	}
}

func TestSQLPartitionedKeysetPaginationRejectsChangedPartitionLayout(t *testing.T) {
	resolver := &partitionedKeysetResolver{
		partitions: []hatSql.SQLSourcePartition{
			{Name: "apac", Rows: []hatSql.Row{{"id": "a-1", "score": int64(1)}}},
			{Name: "eu", Rows: []hatSql.Row{{"id": "e-1", "score": int64(2)}}},
		},
	}
	query := "SELECT e.id FROM CACHE('events') AS e ORDER BY e.score"
	first, err := hatSql.ExecuteSQLQueryKeysetPage(context.Background(), query, resolver, nil, hatSql.SQLQueryOptions{}, 1, "")
	if err != nil {
		t.Fatal(err)
	}
	resolver.partitions[0].Name = "us"
	_, err = hatSql.ExecuteSQLQueryKeysetPage(context.Background(), query, resolver, nil, hatSql.SQLQueryOptions{}, 1, first.NextCursor)
	if err == nil {
		t.Fatal("changed partition layout was accepted")
	}
}

func TestSQLPartitionedKeysetPaginationForwardsThroughCatalogAndSession(t *testing.T) {
	base := &partitionedKeysetResolver{
		partitions: []hatSql.SQLSourcePartition{{Name: "apac", Rows: []hatSql.Row{{"id": "a-1", "score": int64(1)}}}},
	}
	query := "SELECT e.id FROM CACHE('events') AS e ORDER BY e.score"
	for name, resolver := range map[string]hatSql.SQLSourceResolver{
		"catalog": hatSql.CatalogResolver{Source: base},
		"session": hatSql.NewSQLSession(base),
	} {
		result, err := hatSql.ExecuteSQLQueryKeysetPage(context.Background(), query, resolver, nil, hatSql.SQLQueryOptions{}, 1, "")
		if err != nil {
			t.Fatalf("%s resolver error = %v", name, err)
		}
		if want := []hatSql.Row{{"id": "a-1"}}; !reflect.DeepEqual(result.Rows, want) {
			t.Fatalf("%s rows = %#v, want %#v", name, result.Rows, want)
		}
	}
}

func TestSQLPartitionedKeysetPaginationFallsBackToDirectStream(t *testing.T) {
	resolver := &partitionedKeysetFallbackResolver{rows: []hatSql.Row{{"id": "a-1", "score": int64(1)}, {"id": "a-2", "score": int64(2)}}}
	result, err := hatSql.ExecuteSQLQueryKeysetPage(context.Background(), "SELECT e.id FROM CACHE('events') AS e ORDER BY e.score", resolver, nil, hatSql.SQLQueryOptions{}, 1, "")
	if err != nil {
		t.Fatal(err)
	}
	if want := []hatSql.Row{{"id": "a-1"}}; !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("fallback rows = %#v, want %#v", result.Rows, want)
	}
	if result.NextCursor == "" || resolver.orderedCalls != 1 || resolver.streamCalls != 1 {
		t.Fatalf("fallback state = cursor %q, ordered calls %d, stream calls %d", result.NextCursor, resolver.orderedCalls, resolver.streamCalls)
	}
}

type partitionedKeysetFallbackResolver struct {
	rows         []hatSql.Row
	orderedCalls int
	streamCalls  int
}

func (resolver *partitionedKeysetFallbackResolver) ResolveSQLSource(name, key string) ([]hatSql.Row, error) {
	return resolver.rows, nil
}

func (resolver *partitionedKeysetFallbackResolver) ResolveSQLOrderedSourcePartitions(name, key, field string, desc, nullsFirst, nullsLast bool) ([]hatSql.SQLSourcePartition, bool, error) {
	resolver.orderedCalls++
	return nil, false, nil
}

func (resolver *partitionedKeysetFallbackResolver) StreamSQLOrderedSourceAfter(ctx context.Context, name, key, field string, desc, nullsFirst, nullsLast bool, after hatSql.SQLKeysetPosition, visit func(hatSql.Row, hatSql.SQLKeysetPosition) error) (bool, error) {
	resolver.streamCalls++
	start := 0
	if after.Valid {
		start = int(after.Tie) + 1
	}
	for index := start; index < len(resolver.rows); index++ {
		if err := ctx.Err(); err != nil {
			return true, err
		}
		if err := visit(resolver.rows[index], hatSql.SQLKeysetPosition{Value: resolver.rows[index][field], Tie: uint64(index), Valid: true}); err != nil {
			return true, err
		}
	}
	return true, nil
}
