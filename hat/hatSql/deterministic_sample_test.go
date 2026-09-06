package hatSql

import (
	"errors"
	"math"
	"reflect"
	"testing"
)

func TestSampleSQLRowsIsDeterministicAcrossOrderAndPartitions(t *testing.T) {
	rows := make([]SQLRow, 256)
	for index := range rows {
		rows[index] = SQLRow{"id": index, "value": index * 2}
	}
	key := func(row SQLRow) string { return string(rune(row["id"].(int))) }

	first, err := SampleSQLRows(rows, key, 0.25, 42)
	if err != nil {
		t.Fatalf("SampleSQLRows() error = %v", err)
	}
	reordered := append(append([]SQLRow(nil), rows[128:]...), rows[:128]...)
	second, err := SampleSQLRows(reordered, key, 0.25, 42)
	if err != nil {
		t.Fatalf("SampleSQLRows(reordered) error = %v", err)
	}
	firstKeys := sampledSQLRowIDs(first)
	secondKeys := sampledSQLRowIDs(second)
	if !reflect.DeepEqual(firstKeys, sortedInts(secondKeys)) {
		t.Fatalf("sample keys changed with input order: first=%v second=%v", firstKeys, secondKeys)
	}

	left, err := SampleSQLRows(rows[:128], key, 0.25, 42)
	if err != nil {
		t.Fatalf("SampleSQLRows(left partition) error = %v", err)
	}
	right, err := SampleSQLRows(rows[128:], key, 0.25, 42)
	if err != nil {
		t.Fatalf("SampleSQLRows(right partition) error = %v", err)
	}
	partitionKeys := append(sampledSQLRowIDs(left), sampledSQLRowIDs(right)...)
	if !reflect.DeepEqual(firstKeys, sortedInts(partitionKeys)) {
		t.Fatalf("sample keys changed across partitions: all=%v partitions=%v", firstKeys, partitionKeys)
	}
}

func TestSampleSQLRowsHandlesBoundaryFractionsAndPreservesRows(t *testing.T) {
	rows := []SQLRow{{"id": 1}, {"id": 2}}
	key := func(row SQLRow) string { return string(rune(row["id"].(int))) }

	zero, err := SampleSQLRows(rows, key, 0, 1)
	if err != nil {
		t.Fatalf("zero fraction error = %v", err)
	}
	if zero != nil {
		t.Fatalf("zero fraction result = %#v, want nil", zero)
	}
	one, err := SampleSQLRows(rows, key, 1, 1)
	if err != nil {
		t.Fatalf("one fraction error = %v", err)
	}
	if !reflect.DeepEqual(one, rows) {
		t.Fatalf("one fraction result = %#v, want %#v", one, rows)
	}
	for _, fraction := range []float64{-0.1, 1.1, math.NaN(), math.Inf(1)} {
		if _, err := SampleSQLRows(rows, key, fraction, 1); !errors.Is(err, ErrSQLSamplingInvalidFraction) {
			t.Errorf("fraction %v error = %v, want ErrSQLSamplingInvalidFraction", fraction, err)
		}
	}
}

func TestSampleSQLRowsRejectsMissingKey(t *testing.T) {
	if _, err := SampleSQLRows([]SQLRow{{"id": 1}}, nil, 0.5, 1); !errors.Is(err, ErrSQLSamplingKeyRequired) {
		t.Fatalf("nil key error = %v, want ErrSQLSamplingKeyRequired", err)
	}
}

func BenchmarkSampleSQLRows(b *testing.B) {
	rows := make([]SQLRow, 1024)
	for index := range rows {
		rows[index] = SQLRow{"id": index, "key": string(rune(index)), "value": index * 2}
	}
	key := func(row SQLRow) string { return row["key"].(string) }
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := SampleSQLRows(rows, key, 0.25, 42); err != nil {
			b.Fatal(err)
		}
	}
}

func sampledSQLRowIDs(rows []SQLRow) []int {
	ids := make([]int, len(rows))
	for index, row := range rows {
		ids[index] = row["id"].(int)
	}
	return ids
}

func sortedInts(values []int) []int {
	sorted := append([]int(nil), values...)
	for index := 1; index < len(sorted); index++ {
		for position := index; position > 0 && sorted[position] < sorted[position-1]; position-- {
			sorted[position], sorted[position-1] = sorted[position-1], sorted[position]
		}
	}
	return sorted
}
