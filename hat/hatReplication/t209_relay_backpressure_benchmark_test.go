package hatReplication

import "testing"

func BenchmarkT209RelayBackpressureLegacyLagCheck(b *testing.B) {
	const highWatermark = uint64(10_000)
	lag := uint64(1)
	allowed := 0
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if lag < highWatermark {
			allowed++
		}
	}
	b.StopTimer()
	if allowed == 0 {
		b.Fatal("legacy lag check admitted no work")
	}
}

func BenchmarkT209RelayBackpressureAdmit(b *testing.B) {
	backpressure := NewRelayBackpressure(RelayBackpressureOptions{
		Enabled:       true,
		HighWatermark: 10_000,
	})
	allowed := 0
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if backpressure.Admit(1).Allowed {
			allowed++
		}
	}
	b.StopTimer()
	if allowed == 0 {
		b.Fatal("relay backpressure admitted no work")
	}
}
