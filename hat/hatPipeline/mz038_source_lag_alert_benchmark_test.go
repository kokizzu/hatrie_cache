package hatPipeline

import "testing"

func BenchmarkMZ038SourceLagAlertObserve(b *testing.B) {
	registry, err := NewSourceLagAlertRegistry(SourceLagAlertRegistryOptions{})
	if err != nil {
		b.Fatal(err)
	}
	if _, err := registry.Observe("orders", 0); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := registry.Observe("orders", uint64(index%2001)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMZ038SourceLagAlertThresholdControl(b *testing.B) {
	state := SourceLagAlertHealthy
	const (
		warning  = uint64(100)
		critical = uint64(1000)
		recovery = uint64(50)
	)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		lag := uint64(index % 2001)
		state = nextSourceLagAlertState(state, lag, SourceLagAlertPolicy{WarningLag: warning, CriticalLag: critical, RecoveryLag: recovery})
	}
	if state == SourceLagAlertState("") {
		b.Fatal("threshold control ended with empty state")
	}
}
