package hatJournal

import (
	"testing"
	"time"
)

var spaceSyncAppenderBenchmarkSink int

type spaceSyncAppenderBenchmarkWriter struct{}

func (spaceSyncAppenderBenchmarkWriter) Write(payload []byte) (int, error) {
	spaceSyncAppenderBenchmarkSink += len(payload)
	return len(payload), nil
}

func BenchmarkSpaceSyncAppenderBoundary(b *testing.B) {
	payload := []byte("event-payload")
	for _, test := range []struct {
		name       string
		policy     SpaceSyncPolicy
		maxBytes   int
		wantSynces bool
	}{
		{name: "direct-writer-control"},
		{name: "disabled", policy: SpaceSyncPolicyDisabled},
		{name: "periodic-pending", policy: SpaceSyncPolicyPeriodic, maxBytes: 1 << 30},
		{name: "immediate", policy: SpaceSyncPolicyImmediate, wantSynces: true},
	} {
		b.Run(test.name, func(b *testing.B) {
			writer := spaceSyncAppenderBenchmarkWriter{}
			syncs := 0
			if test.name == "direct-writer-control" {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					written, err := writer.Write(payload)
					if err != nil || written != len(payload) {
						b.Fatalf("Write() = %d/%v", written, err)
					}
				}
				return
			}
			registry := mustBenchmarkSpaceSyncPolicyRegistry(b, test.policy)
			maxBytes := test.maxBytes
			if maxBytes == 0 {
				maxBytes = DefaultSpaceSyncAppenderPeriodicMaxBytes
			}
			appender, err := NewSpaceSyncAppender(&writer, func() error {
				syncs++
				return nil
			}, SpaceSyncAppenderOptions{
				Registry:         registry,
				PeriodicInterval: time.Hour,
				PeriodicMaxBytes: maxBytes,
			})
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				result, err := appender.Append("bench", payload)
				if err != nil || result.BytesWritten != len(payload) {
					b.Fatalf("Append() = %#v/%v", result, err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(syncs), "syncs/op")
		})
	}
}

func mustBenchmarkSpaceSyncPolicyRegistry(b *testing.B, policy SpaceSyncPolicy) *SpaceSyncPolicyRegistry {
	b.Helper()
	registry, err := NewSpaceSyncPolicyRegistry(SpaceSyncPolicyOptions{
		DefaultPolicy: policy,
	})
	if err != nil {
		b.Fatal(err)
	}
	return registry
}
