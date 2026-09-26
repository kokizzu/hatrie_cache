package hatSql

import (
	"testing"
	"time"
)

const mz009ValidityPartitionQuery = `
SELECT id
FROM CACHE('events')
WHERE VALID_AT(TIMESTAMP '2026-01-01T12:00:00Z', valid_from, valid_to)
ORDER BY id`

type mz009ValidityPartitionResolver struct {
	rows        []Row
	partitions  []SQLSourcePartition
	pruneCalls  int
	sourceCalls int
}

func (resolver *mz009ValidityPartitionResolver) ResolveSQLSource(string, string) ([]Row, error) {
	resolver.sourceCalls++
	return resolver.rows, nil
}

func (resolver *mz009ValidityPartitionResolver) ResolveSQLSourcePartitionsForPredicate(_, _ string, predicate SQLPartitionPredicate) ([]SQLSourcePartition, bool, error) {
	resolver.pruneCalls++
	if predicate.Operator != "VALID_AT" || predicate.ValidFromField != "valid_from" || predicate.ValidToField != "valid_to" || len(predicate.Values) != 1 {
		return nil, false, nil
	}
	return resolver.partitions[:1], true, nil
}

func TestMZ009ValidityPartitionPruning(t *testing.T) {
	resolver := &mz009ValidityPartitionResolver{}
	resolver.partitions = mz009ValidityPartitions()
	for _, partition := range resolver.partitions {
		resolver.rows = append(resolver.rows, partition.Rows...)
	}

	result, err := ExecuteSQLQuery(mz009ValidityPartitionQuery, resolver)
	if err != nil {
		t.Fatalf("ExecuteSQLQuery() error = %v", err)
	}
	if got, want := len(result.Rows), 4; got != want {
		t.Fatalf("rows = %d, want %d: %#v", got, want, result.Rows)
	}
	if got, want := resolver.pruneCalls, 1; got != want {
		t.Fatalf("partition pruning calls = %d, want %d", got, want)
	}
	if got := resolver.sourceCalls; got != 0 {
		t.Fatalf("full source calls = %d, want 0", got)
	}
	for index, row := range result.Rows {
		if got, want := row["id"], int64(index+1); got != want {
			t.Fatalf("row %d id = %#v, want %d", index, got, want)
		}
	}
}

func mz009ValidityPartitions() []SQLSourcePartition {
	target := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	matching := make([]Row, 4)
	for index := range matching {
		matching[index] = Row{
			"id":         int64(index + 1),
			"valid_from": target.Add(-time.Duration(index+1) * time.Hour),
			"valid_to":   target.Add(time.Duration(index+1) * time.Hour),
		}
	}
	matching = append(matching, Row{
		"id":         int64(99),
		"valid_from": target.Add(2 * time.Hour),
		"valid_to":   target.Add(3 * time.Hour),
	})
	partitions := []SQLSourcePartition{{Name: "matching", Rows: matching}}
	for partitionIndex := 1; partitionIndex < 64; partitionIndex++ {
		rows := make([]Row, 256)
		from := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC).Add(time.Duration(partitionIndex) * 24 * time.Hour)
		to := from.Add(12 * time.Hour)
		for rowIndex := range rows {
			rows[rowIndex] = Row{
				"id":         int64(partitionIndex*256 + rowIndex + 5),
				"valid_from": from,
				"valid_to":   to,
			}
		}
		partitions = append(partitions, SQLSourcePartition{Name: "expired", Rows: rows})
	}
	return partitions
}

type mz009ValidityBaselineResolver struct {
	rows []Row
}

func (resolver mz009ValidityBaselineResolver) ResolveSQLSource(string, string) ([]Row, error) {
	return resolver.rows, nil
}

func BenchmarkMZ009ValidityPartitionPruningBaseline(b *testing.B) {
	rows := make([]Row, 0, 64*256)
	for _, partition := range mz009ValidityPartitions() {
		rows = append(rows, partition.Rows...)
	}
	benchmarkMZ009ValidityPartitionPruning(b, mz009ValidityBaselineResolver{rows: rows})
}

func BenchmarkMZ009ValidityPartitionPruningFastPath(b *testing.B) {
	partitions := mz009ValidityPartitions()
	rows := make([]Row, 0, 64*256)
	for _, partition := range partitions {
		rows = append(rows, partition.Rows...)
	}
	benchmarkMZ009ValidityPartitionPruning(b, &mz009ValidityPartitionResolver{rows: rows, partitions: partitions})
}

func benchmarkMZ009ValidityPartitionPruning(b *testing.B, resolver SQLSourceResolver) {
	b.Helper()
	for index := 0; index < b.N; index++ {
		result, err := ExecuteSQLQuery(mz009ValidityPartitionQuery, resolver)
		if err != nil {
			b.Fatal(err)
		}
		if len(result.Rows) != 4 {
			b.Fatalf("rows = %d, want 4", len(result.Rows))
		}
	}
}
