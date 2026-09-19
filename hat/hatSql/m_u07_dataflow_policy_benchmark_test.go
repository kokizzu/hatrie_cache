package hatSql_test

import (
	"strconv"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func BenchmarkDifferentialDataflowApply(b *testing.B) {
	rows := mU07BenchmarkRows(1024)
	for _, testCase := range []struct {
		name     string
		frontier hatSql.DifferentialDataflowFrontier
	}{
		{name: "manual", frontier: hatSql.DifferentialDataflowManual},
		{name: "batch_max", frontier: hatSql.DifferentialDataflowBatchMax},
	} {
		b.Run(testCase.name, func(b *testing.B) {
			flow, err := hatSql.NewDifferentialDataflow(hatSql.DifferentialDataflowOptions{
				Policy: hatSql.DifferentialDataflowPolicy{
					AllowedLateness: 0,
					Correction:      hatSql.DifferentialDataflowDrop,
					Frontier:        testCase.frontier,
				},
				InitialFrontier: 256,
				Sink: func([]hatSql.DifferentialRow) error {
					return nil
				},
			})
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if err := flow.Apply(rows); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func mU07BenchmarkRows(count int) []hatSql.DifferentialRow {
	rows := make([]hatSql.DifferentialRow, count)
	for index := range rows {
		rows[index] = hatSql.DifferentialRow{
			Key:  strconv.Itoa(index),
			Time: uint64(index % 512),
			Diff: 1,
			Row:  hatSql.Row{"value": index},
		}
	}
	return rows
}
