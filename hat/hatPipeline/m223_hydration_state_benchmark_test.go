package hatPipeline_test

import (
	"testing"

	"hatrie_cache/hat/hatPipeline"
)

var m223HydrationSnapshotSink hatPipeline.HydrationSnapshot

func BenchmarkM223HydrationSnapshot(b *testing.B) {
	machine := hatPipeline.NewHydrationStateMachine()
	if err := machine.Begin(1); err != nil {
		b.Fatal(err)
	}
	if err := machine.Advance(1); err != nil {
		b.Fatal(err)
	}
	if err := machine.Complete(); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		m223HydrationSnapshotSink = machine.Snapshot()
	}
}

func BenchmarkM223HydrationAdvance(b *testing.B) {
	machine := hatPipeline.NewHydrationStateMachine()
	if err := machine.Begin(uint64(b.N)); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := machine.Advance(1); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	if err := machine.Complete(); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkM223HydrationLifecycle(b *testing.B) {
	machine := hatPipeline.NewHydrationStateMachine()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := machine.Begin(1); err != nil {
			b.Fatal(err)
		}
		if err := machine.Advance(1); err != nil {
			b.Fatal(err)
		}
		if err := machine.Complete(); err != nil {
			b.Fatal(err)
		}
	}
}
