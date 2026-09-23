package hatReplication

import "testing"

func BenchmarkTTG42SingleRelayBaseline(b *testing.B) {
	relay := NewRelayBackpressure(RelayBackpressureOptions{
		Enabled:         true,
		HighWatermark:   10_000,
		ResumeWatermark: 5_000,
	})
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		relay.Admit(uint64(index & 255))
	}
}

func BenchmarkTTG42PeerRelayBackpressureObserve(b *testing.B) {
	controller, err := NewPeerRelayBackpressure(PeerRelayBackpressureOptions{
		Enabled:         true,
		HighWatermark:   10_000,
		ResumeWatermark: 5_000,
		MaxPeers:        16,
	})
	if err != nil {
		b.Fatal(err)
	}
	peers := make([]string, 16)
	for index := range peers {
		peers[index] = "peer-" + string(rune('a'+index))
		if _, err := controller.Observe(peers[index], 1); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := controller.Observe(peers[index&15], uint64(index&255)); err != nil {
			b.Fatal(err)
		}
	}
}
