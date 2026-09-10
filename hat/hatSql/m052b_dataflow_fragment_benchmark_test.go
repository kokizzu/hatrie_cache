package hatSql

import (
	"context"
	"strconv"
	"testing"
)

const benchmarkSQLDataflowFragmentStages = 5

var benchmarkSQLDataflowFragmentSink int

func BenchmarkSQLDataflowFragmentExecution(b *testing.B) {
	input := benchmarkSQLDataflowFragmentRows()
	b.Run("direct", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		var sink int
		for index := 0; index < b.N; index++ {
			rows := input
			for stage := 0; stage < benchmarkSQLDataflowFragmentStages; stage++ {
				rows = benchmarkSQLDataflowFragmentStage(rows, stage)
			}
			sink += len(rows)
		}
		benchmarkSQLDataflowFragmentSink = sink
	})
	b.Run("executor", func(b *testing.B) {
		plan := SQLDataflowPlan{
			Format: sqlDataflowPlanFormat,
			Root:   benchmarkSQLDataflowFragmentStages - 1,
			Fragments: func() []SQLDataflowFragment {
				fragments := make([]SQLDataflowFragment, benchmarkSQLDataflowFragmentStages)
				for stage := range fragments {
					fragments[stage] = SQLDataflowFragment{
						ID:   stage,
						Kind: "STAGE",
					}
					if stage > 0 {
						fragments[stage].Inputs = []int{stage - 1}
					}
				}
				return fragments
			}(),
		}
		executor, err := CompileSQLDataflow(plan, func(_ context.Context, fragment SQLDataflowFragment, inputs SQLDataflowFragmentInputs) ([]SQLRow, error) {
			rows := inputs.Initial()
			if inputs.Len() > 0 {
				rows = inputs.Rows(0)
			}
			return benchmarkSQLDataflowFragmentStage(rows, fragment.ID), nil
		})
		if err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		var sink int
		for index := 0; index < b.N; index++ {
			rows, err := executor.Execute(context.Background(), input)
			if err != nil {
				b.Fatal(err)
			}
			sink += len(rows)
		}
		benchmarkSQLDataflowFragmentSink = sink
	})
}

func benchmarkSQLDataflowFragmentRows() []Row {
	rows := make([]Row, 128)
	for index := range rows {
		rows[index] = Row{
			"id":    int64(index),
			"value": int64(index % 17),
		}
	}
	return rows
}

func benchmarkSQLDataflowFragmentStage(rows []Row, stage int) []Row {
	output := make([]Row, len(rows))
	column := "stage_" + strconv.Itoa(stage)
	for index, row := range rows {
		clone := make(Row, len(row)+1)
		for key, value := range row {
			clone[key] = value
		}
		clone[column] = int64(stage)
		output[index] = clone
	}
	return output
}
