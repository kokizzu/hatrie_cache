package hatSql

import (
	"strconv"
	"testing"
)

func BenchmarkDifferentialOperators(b *testing.B) {
	rows := make([]DifferentialRow, 256)
	for index := range rows {
		rows[index] = DifferentialRow{
			Key:  "row-" + strconv.Itoa(index),
			Time: uint64(index % 8),
			Diff: 1,
			Row:  Row{"id": int64(index), "keep": index%2 == 0, "group": "all"},
		}
	}
	left := make([]DifferentialRow, 32)
	right := make([]DifferentialRow, 32)
	for index := range left {
		left[index] = DifferentialRow{Key: "left-" + strconv.Itoa(index), Diff: 1, Row: Row{"id": int64(index)}}
		right[index] = DifferentialRow{Key: "right-" + strconv.Itoa(index), Diff: -1, Row: Row{"id": int64(index)}}
	}

	b.Run("filter", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if _, err := FilterDifferentialRows(rows, func(row Row) (bool, error) { return row["keep"].(bool), nil }); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("map", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if _, err := MapDifferentialRows(rows, func(row Row) (string, Row, error) {
				return "group", Row{"group": row["group"]}, nil
			}); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("flat_map", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if _, err := FlatMapDifferentialRows(rows, func(row Row) ([]DifferentialFlatMapResult, error) {
				return []DifferentialFlatMapResult{{Key: "group", Row: Row{"group": row["group"]}}}, nil
			}); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("union", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if _, err := UnionDifferentialRows(rows, rows); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("join", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if _, err := JoinDifferentialRows(left, right,
				func(left, right Row) (bool, error) { return left["id"] == right["id"], nil },
				func(left, right Row) (string, Row, error) {
					return "joined", Row{"id": left["id"], "right": right["id"]}, nil
				},
			); err != nil {
				b.Fatal(err)
			}
		}
	})
}
