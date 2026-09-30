package hatPipeline_test

import (
	"testing"

	"hatrie_cache/hat/hatPipeline"
)

var m224HydrationEstimateSink hatPipeline.HydrationEstimate

func BenchmarkM224HydrationEstimate(b *testing.B) {
	machine := hatPipeline.NewHydrationStateMachine()
	if err := machine.Begin(1000000); err != nil {
		b.Fatal(err)
	}
	if err := machine.Advance(250000); err != nil {
		b.Fatal(err)
	}
	if err := machine.SetRate(5000); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		m224HydrationEstimateSink = machine.Estimate()
	}
}

func BenchmarkM224HydrationSetRate(b *testing.B) {
	machine := hatPipeline.NewHydrationStateMachine()
	if err := machine.Begin(1000000); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := machine.SetRate(float64(index + 1)); err != nil {
			b.Fatal(err)
		}
	}
}
