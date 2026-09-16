package hatPipeline

import (
	"context"
	"testing"
)

var mz007FrontierBackpressureBenchmarkSink uint64

// Keep the no-lock control path from being folded into the benchmark loop.
//
//go:noinline
func mz007FrontierBackpressureBaselineAdmit(consumed, maxLag, frontier uint64) bool {
	return frontier <= frontierBackpressureLimit(consumed, maxLag)
}

func BenchmarkMZ007FrontierBackpressureBaseline(b *testing.B) {
	const maxLag = uint64(1024)
	var consumed uint64
	var admitted uint64
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		frontier := uint64(i) & (maxLag - 1)
		if mz007FrontierBackpressureBaselineAdmit(consumed, maxLag, frontier) {
			admitted++
		}
	}
	b.StopTimer()
	mz007FrontierBackpressureBenchmarkSink = admitted
}

func BenchmarkMZ007FrontierBackpressureTryAdmit(b *testing.B) {
	gate, err := NewFrontierBackpressure(FrontierBackpressureOptions{MaxLag: 1024})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		admitted, err := gate.TryAdmit(uint64(i) & (1024 - 1))
		if err != nil || !admitted {
			b.Fatalf("TryAdmit() = admitted %v, err %v", admitted, err)
		}
	}
	b.StopTimer()
	mz007FrontierBackpressureBenchmarkSink = gate.Stats().Admitted
}

func BenchmarkMZ007FrontierBackpressureWaitAdmitted(b *testing.B) {
	gate, err := NewFrontierBackpressure(FrontierBackpressureOptions{MaxLag: 1024})
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := gate.Wait(ctx, uint64(i)&(1024-1)); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	mz007FrontierBackpressureBenchmarkSink = gate.Stats().Admitted
}

func BenchmarkMZ007FrontierBackpressureBlockedTryAdmit(b *testing.B) {
	gate, err := NewFrontierBackpressure(FrontierBackpressureOptions{MaxLag: 1})
	if err != nil {
		b.Fatal(err)
	}
	if admitted, err := gate.TryAdmit(0); err != nil || !admitted {
		b.Fatalf("initial TryAdmit() = admitted %v, err %v", admitted, err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		admitted, err := gate.TryAdmit(2)
		if err != nil || admitted {
			b.Fatalf("blocked TryAdmit() = admitted %v, err %v", admitted, err)
		}
	}
	b.StopTimer()
	mz007FrontierBackpressureBenchmarkSink = gate.Stats().BlockedAttempts
}
