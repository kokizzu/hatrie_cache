package hatPipeline

import "testing"

var mz046SchemaMigrationBenchmarkSink any

func BenchmarkMZ046DirectDependencySet(b *testing.B) {
	dependencies := []string{"reader", "sink", "index", "writer"}
	known := make(map[string]struct{}, len(dependencies))
	acknowledged := make(map[string]struct{}, len(dependencies))
	for _, dependency := range dependencies {
		known[dependency] = struct{}{}
	}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		dependency := dependencies[index%len(dependencies)]
		if _, ok := known[dependency]; !ok {
			b.Fatal("unknown dependency")
		}
		acknowledged[dependency] = struct{}{}
		mz046SchemaMigrationBenchmarkSink = len(acknowledged)
	}
}

func BenchmarkMZ046BarrierAcknowledge(b *testing.B) {
	barrier, err := NewSchemaMigrationBarrier(SchemaMigrationBarrierOptions{MaxBarriers: 1, MaxDependencies: 4})
	if err != nil {
		b.Fatal(err)
	}
	dependencies := []string{"reader", "sink", "index", "writer"}
	if _, err := barrier.Prepare(SchemaMigrationBarrierSpec{ID: "migration", Version: 1, Dependencies: dependencies}); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		err := barrier.Acknowledge("migration", dependencies[index%len(dependencies)], 1)
		if err != nil {
			b.Fatal(err)
		}
		mz046SchemaMigrationBenchmarkSink = index
	}
}
