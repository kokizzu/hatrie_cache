package hatSql

import (
	"reflect"
	"testing"
)

func TestMZ037DifferentialDifferenceSmallBatchesPreserveWeights(t *testing.T) {
	row := Row{"id": int64(1), "value": "left"}
	rightRow := Row{"id": int64(1), "value": "right"}

	tests := []struct {
		name  string
		left  []DifferentialRow
		right []DifferentialRow
		want  []DifferentialRow
	}{
		{
			name: "single left insertion",
			left: []DifferentialRow{{Key: "k", Time: 1, Diff: 1, Row: row}},
			want: []DifferentialRow{{Key: "k", Time: 1, Diff: 1, Row: row}},
		},
		{
			name:  "single right retraction",
			right: []DifferentialRow{{Key: "k", Time: 2, Diff: 1, Row: rightRow}},
			want:  []DifferentialRow{{Key: "k", Time: 2, Diff: -1, Row: rightRow}},
		},
		{
			name: "exact cancellation",
			left: []DifferentialRow{{Key: "k", Time: 3, Diff: 1, Row: row}},
			right: []DifferentialRow{{Key: "k", Time: 3, Diff: 1, Row: rightRow}},
		},
		{
			name:  "weighted transition",
			left: []DifferentialRow{{Key: "k", Time: 4, Diff: 3, Row: row}},
			right: []DifferentialRow{{Key: "k", Time: 4, Diff: 1, Row: rightRow}},
			want:  []DifferentialRow{{Key: "k", Time: 4, Diff: 2, Row: row}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := ExceptDifferentialRows(test.left, test.right)
			if err != nil {
				t.Fatalf("ExceptDifferentialRows() error = %v", err)
			}
			if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("ExceptDifferentialRows() = %#v, want %#v", got, test.want)
			}
		})
	}
}

func BenchmarkM037DifferentialDifferenceSmallBatch(b *testing.B) {
	row := Row{"id": int64(1)}
	benchmarks := []struct {
		name  string
		left  []DifferentialRow
		right []DifferentialRow
	}{
		{
			name: "single-left",
			left: []DifferentialRow{{Key: "k", Time: 1, Diff: 1, Row: row}},
		},
		{
			name: "exact-cancellation",
			left: []DifferentialRow{{Key: "k", Time: 1, Diff: 1, Row: row}},
			right: []DifferentialRow{{Key: "k", Time: 1, Diff: 1, Row: row}},
		},
	}

	for _, benchmark := range benchmarks {
		b.Run(benchmark.name, func(b *testing.B) {
			for index := 0; index < b.N; index++ {
				result, err := ExceptDifferentialRows(benchmark.left, benchmark.right)
				if err != nil {
					b.Fatal(err)
				}
				differentialDifferenceBenchmarkSink += len(result)
			}
		})
	}
}
