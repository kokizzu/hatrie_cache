package hatSql

import (
	"fmt"
	"strconv"
	"testing"
)

var differentialAverageBenchmarkSink int

func BenchmarkGroupAverageInt64DifferentialRows(b *testing.B) {
	rows := make([]DifferentialRow, 1024)
	for index := range rows {
		rows[index] = DifferentialRow{
			Key:  fmt.Sprintf("row-%d", index),
			Time: uint64(index),
			Diff: 1,
			Row:  Row{"group": strconv.Itoa(index % 256), "value": int64(index)},
		}
	}
	for _, test := range []struct {
		name string
		fn   func() error
	}{
		{
			name: "before_separate_count_sum",
			fn: func() error {
				countRows, err := GroupCountDifferentialRows(rows, differentialTestGroupKey)
				if err != nil {
					return err
				}
				sumRows, err := GroupSumInt64DifferentialRows(rows, differentialTestGroupKey, differentialTestInt64Value)
				if err != nil {
					return err
				}
				differentialAverageBenchmarkSink += len(countRows) + len(sumRows)
				return nil
			},
		},
		{
			name: "before_combined_count_sum",
			fn: func() error {
				result, err := GroupCountSumInt64DifferentialRows(rows, differentialTestGroupKey, differentialTestInt64Value)
				differentialAverageBenchmarkSink += len(result)
				return err
			},
		},
		{
			name: "after_average",
			fn: func() error {
				result, err := GroupAverageInt64DifferentialRows(rows, differentialTestGroupKey, differentialTestInt64Value)
				differentialAverageBenchmarkSink += len(result)
				return err
			},
		},
	} {
		b.Run(test.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if err := test.fn(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
