package hatSql_test

import (
	"testing"

	"hatrie_cache/hat/hatSql"
)

var typedTableArrangementProgressBenchmarkSink uint64

func BenchmarkTypedTableArrangementHydrationProgress(b *testing.B) {
	for _, benchmark := range []struct {
		name         string
		withProgress bool
	}{
		{name: "baseline_hydrate", withProgress: false},
		{name: "tracked_hydrate", withProgress: true},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			table, _, definition := newTypedTableArrangementBenchmarkFixture(b)
			arrangements, err := hatSql.NewTypedTableAggregateArrangements(table)
			if err != nil {
				b.Fatal(err)
			}
			arrangement, err := arrangements.Acquire(definition)
			if err != nil {
				b.Fatal(err)
			}
			defer arrangement.Release()
			for {
				report, err := arrangement.Hydrate(1024)
				if err != nil {
					b.Fatal(err)
				}
				if report.Complete {
					break
				}
			}
			if err := table.CompactChangesThrough(uint64(typedTableArrangementHydrationBenchmarkRows)); err != nil {
				b.Fatal(err)
			}
			var progress *hatSql.TypedTableArrangementHydrationProgress
			if benchmark.withProgress {
				progress = hatSql.NewTypedTableArrangementHydrationProgress(nil)
			}

			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				b.StopTimer()
				change, err := table.Upsert("tail", typedTableArrangementBenchmarkValues(index))
				if err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
				var report hatSql.TypedTableAggregateArrangementHydration
				if benchmark.withProgress {
					report, err = arrangement.HydrateWithProgress(1, progress)
				} else {
					report, err = arrangement.Hydrate(1)
				}
				if err != nil {
					b.Fatal(err)
				}
				typedTableArrangementProgressBenchmarkSink = report.After
				b.StopTimer()
				if report.Applied != 1 || report.After != change.Sequence {
					b.Fatalf("Hydrate() report = %#v for change %#v", report, change)
				}
				if err := table.CompactChangesThrough(change.Sequence); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
			}
		})
	}
}
