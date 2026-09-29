package hatSql

import (
	"context"
	"fmt"
	"testing"
)

func BenchmarkM219BackgroundIndexBuild(b *testing.B) {
	batches := m219BenchmarkBatches(4096, 128)
	b.SetBytes(int64(len(batches) * len(batches[0].Updates)))
	b.Run("synchronous_apply", func(b *testing.B) {
		b.ReportAllocs()
		for iteration := 0; iteration < b.N; iteration++ {
			index, err := NewIncrementalPointLookup(IncrementalPointLookupDefinition{
				IndexKey: func(row Row) (string, error) { return row["region"].(string), nil },
			})
			if err != nil {
				b.Fatal(err)
			}
			for _, batch := range batches {
				if err := index.Apply(batch.Updates); err != nil {
					b.Fatal(err)
				}
			}
		}
	})
	for _, test := range []struct {
		name    string
		options SQLBackgroundIndexBuildOptions
	}{
		{name: "background_builder", options: SQLBackgroundIndexBuildOptions{}},
		{name: "background_builder_clone_inputs", options: SQLBackgroundIndexBuildOptions{CloneInputs: true}},
	} {
		test := test
		b.Run(test.name, func(b *testing.B) {
			b.ReportAllocs()
			for iteration := 0; iteration < b.N; iteration++ {
				index, err := NewIncrementalPointLookup(IncrementalPointLookupDefinition{
					IndexKey: func(row Row) (string, error) { return row["region"].(string), nil },
				})
				if err != nil {
					b.Fatal(err)
				}
				builder, err := NewSQLBackgroundIndexBuilder(SQLBackgroundIndexBuildDefinition{
					Name:    "benchmark",
					Batches: batches,
					Apply:   index.Apply,
				}, test.options)
				if err != nil {
					b.Fatal(err)
				}
				if err := builder.Start(context.Background()); err != nil {
					b.Fatal(err)
				}
				if err := builder.Wait(context.Background()); err != nil {
					b.Fatal(err)
				}
				if builder.Status().State != SQLBackgroundIndexBuildReady {
					b.Fatalf("builder state = %s", builder.Status().State)
				}
			}
		})
	}
}

func BenchmarkM219BackgroundIndexStatusSnapshot(b *testing.B) {
	builder, err := NewSQLBackgroundIndexBuilder(SQLBackgroundIndexBuildDefinition{
		Name:    "status",
		Batches: []SQLBackgroundIndexBuildBatch{{Frontier: 1, Updates: []DifferentialRow{{Key: "a", Diff: 1}}}},
		Apply:   func([]DifferentialRow) error { return nil },
	}, SQLBackgroundIndexBuildOptions{})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		_ = builder.Status()
	}
}

func m219BenchmarkBatches(totalRows, batchSize int) []SQLBackgroundIndexBuildBatch {
	batches := make([]SQLBackgroundIndexBuildBatch, 0, (totalRows+batchSize-1)/batchSize)
	for start := 0; start < totalRows; start += batchSize {
		end := start + batchSize
		if end > totalRows {
			end = totalRows
		}
		updates := make([]DifferentialRow, 0, end-start)
		for rowIndex := start; rowIndex < end; rowIndex++ {
			updates = append(updates, DifferentialRow{
				Key:  fmt.Sprintf("row-%06d", rowIndex),
				Diff: 1,
				Row:  Row{"region": fmt.Sprintf("region-%02d", rowIndex%32), "value": int64(rowIndex)},
			})
		}
		batches = append(batches, SQLBackgroundIndexBuildBatch{Frontier: uint64(end), Updates: updates})
	}
	return batches
}
