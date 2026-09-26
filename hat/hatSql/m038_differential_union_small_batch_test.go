package hatSql

import (
	"reflect"
	"testing"
)

func TestMZ038DifferentialUnionSmallBatchesPreserveMultiplicity(t *testing.T) {
	row := Row{"id": int64(1), "value": "first"}
	secondRow := Row{"id": int64(1), "value": "second"}

	tests := []struct {
		name    string
		batches [][]DifferentialRow
		want    []DifferentialRow
	}{
		{
			name:    "single update",
			batches: [][]DifferentialRow{{{Key: "k", Time: 1, Diff: 1, Row: row}}},
			want:    []DifferentialRow{{Key: "k", Time: 1, Diff: 1, Row: row}},
		},
		{
			name: "weighted duplicate",
			batches: [][]DifferentialRow{
				{{Key: "k", Time: 2, Diff: 3, Row: row}},
				{{Key: "k", Time: 2, Diff: 1, Row: secondRow}},
			},
			want: []DifferentialRow{{Key: "k", Time: 2, Diff: 4, Row: row}},
		},
		{
			name: "exact cancellation",
			batches: [][]DifferentialRow{
				{{Key: "k", Time: 3, Diff: 1, Row: row}},
				{{Key: "k", Time: 3, Diff: -1, Row: secondRow}},
			},
		},
		{
			name:    "zero update",
			batches: [][]DifferentialRow{{{Key: "k", Time: 4, Diff: 0, Row: row}}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := UnionDifferentialRows(test.batches...)
			if err != nil {
				t.Fatalf("UnionDifferentialRows() error = %v", err)
			}
			if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("UnionDifferentialRows() = %#v, want %#v", got, test.want)
			}
		})
	}
}

func BenchmarkM038DifferentialUnionSmallBatch(b *testing.B) {
	row := Row{"id": int64(1)}
	benchmarks := []struct {
		name    string
		batches [][]DifferentialRow
	}{
		{
			name:    "single-update",
			batches: [][]DifferentialRow{{{Key: "k", Time: 1, Diff: 1, Row: row}}},
		},
		{
			name: "exact-cancellation",
			batches: [][]DifferentialRow{
				{{Key: "k", Time: 1, Diff: 1, Row: row}},
				{{Key: "k", Time: 1, Diff: -1, Row: row}},
			},
		},
	}

	for _, benchmark := range benchmarks {
		b.Run(benchmark.name, func(b *testing.B) {
			for index := 0; index < b.N; index++ {
				result, err := UnionDifferentialRows(benchmark.batches...)
				if err != nil {
					b.Fatal(err)
				}
				differentialDifferenceBenchmarkSink += len(result)
			}
		})
	}
}
