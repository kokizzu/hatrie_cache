package hatTopology

import (
	"context"
	"testing"
)

func newConfigWatchReadBenchmarkLog(b *testing.B) *ConfigWatchLog {
	b.Helper()
	log, err := NewConfigWatchLog(ConfigWatchOptions{
		HistoryLimit: 1024,
		Authorizer: func(context.Context, ConfigWatchAuthorization) error {
			return nil
		},
	})
	if err != nil {
		b.Fatalf("NewConfigWatchLog() error = %v", err)
	}
	for version := uint64(1); version <= 1024; version++ {
		if err := log.Publish(context.Background(), "bench", ConfigWatchEvent{
			Version: version,
			Source:  "node-a",
			Key:     "feature/a",
			Value:   []byte("on"),
		}); err != nil {
			b.Fatalf("Publish(%d) error = %v", version, err)
		}
	}
	return log
}

func BenchmarkConfigWatchReadResumeNearTail(b *testing.B) {
	log := newConfigWatchReadBenchmarkLog(b)
	request := ConfigWatchRequest{Principal: "bench", AfterVersion: 1023, Limit: 1}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := log.Read(ctx, request); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkConfigWatchReadResumeMiddle(b *testing.B) {
	log := newConfigWatchReadBenchmarkLog(b)
	request := ConfigWatchRequest{Principal: "bench", AfterVersion: 511, Limit: 1}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := log.Read(ctx, request); err != nil {
			b.Fatal(err)
		}
	}
}
