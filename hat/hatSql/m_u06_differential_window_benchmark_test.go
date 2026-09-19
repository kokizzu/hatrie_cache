package hatSql_test

import (
	"fmt"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func BenchmarkDifferentialWindowApply(b *testing.B) {
	for _, testCase := range []struct {
		name  string
		mode  hatSql.DifferentialWindowFrameMode
		start int64
		end   int64
	}{
		{name: "rows", mode: hatSql.DifferentialWindowFrameRows, start: -8, end: 0},
		{name: "range", mode: hatSql.DifferentialWindowFrameRange, start: -8, end: 0},
	} {
		b.Run(testCase.name, func(b *testing.B) {
			updates := mU06BenchmarkWindowRows(256)
			b.ReportAllocs()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				window, err := hatSql.NewDifferentialWindow(hatSql.DifferentialWindowOptions{
					Mode:    testCase.mode,
					Start:   testCase.start,
					End:     testCase.end,
					Value:   mU06BenchmarkWindowValue,
					MaxRows: 512,
				})
				if err != nil {
					b.Fatal(err)
				}
				if _, err := window.Apply(updates); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkDifferentialWindowLateCorrection(b *testing.B) {
	for _, mode := range []hatSql.DifferentialWindowFrameMode{
		hatSql.DifferentialWindowFrameRows,
		hatSql.DifferentialWindowFrameRange,
	} {
		b.Run(fmt.Sprintf("mode-%d", mode), func(b *testing.B) {
			window, err := hatSql.NewDifferentialWindow(hatSql.DifferentialWindowOptions{
				Mode:    mode,
				Start:   -8,
				End:     0,
				Value:   mU06BenchmarkWindowValue,
				MaxRows: 512,
			})
			if err != nil {
				b.Fatal(err)
			}
			if _, err := window.Apply(mU06BenchmarkWindowRows(256)); err != nil {
				b.Fatal(err)
			}
			late := hatSql.DifferentialRow{Key: "late", Time: 128, Diff: 1, Row: hatSql.Row{"value": int64(128)}}
			retract := late
			retract.Diff = -1
			retract.Row = nil
			b.ReportAllocs()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				update := late
				if index%2 == 1 {
					update = retract
				}
				if _, err := window.Apply([]hatSql.DifferentialRow{update}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func mU06BenchmarkWindowRows(count int) []hatSql.DifferentialRow {
	rows := make([]hatSql.DifferentialRow, count)
	for index := range rows {
		rows[index] = hatSql.DifferentialRow{
			Key:  fmt.Sprintf("row-%04d", index),
			Time: uint64(index),
			Diff: 1,
			Row:  hatSql.Row{"value": int64(index)},
		}
	}
	return rows
}

func mU06BenchmarkWindowValue(row hatSql.SQLRow) (float64, bool, error) {
	value, ok := row["value"].(int64)
	return float64(value), ok, nil
}
