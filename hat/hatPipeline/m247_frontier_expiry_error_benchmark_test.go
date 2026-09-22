package hatPipeline

import "testing"

var m247ExpiredErrorSink error
var m247ExpiredMessageSink string
var m247ValidTimestampErrorSink error

func benchmarkM247ExpiredRetention(b *testing.B) *FrontierRetentionRegistry {
	b.Helper()
	frontiers, err := NewFrontierRegistry(FrontierRegistryOptions{})
	if err != nil {
		b.Fatalf("NewFrontierRegistry() error = %v", err)
	}
	if err := frontiers.Register("orders"); err != nil {
		b.Fatalf("Register() error = %v", err)
	}
	if err := frontiers.Advance("orders", 100, 120); err != nil {
		b.Fatalf("Advance() error = %v", err)
	}
	retention, err := NewFrontierRetentionRegistry(frontiers, FrontierRetentionOptions{})
	if err != nil {
		b.Fatalf("NewFrontierRetentionRegistry() error = %v", err)
	}
	b.Cleanup(func() { _ = retention.Close() })
	return retention
}

func BenchmarkM247AcquireExpired(b *testing.B) {
	retention := benchmarkM247ExpiredRetention(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, m247ExpiredErrorSink = retention.Acquire("orders", 99)
	}
}

func BenchmarkM247CheckTimestampValid(b *testing.B) {
	retention := benchmarkM247ExpiredRetention(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m247ValidTimestampErrorSink = retention.checkTimestamp("orders", 100)
	}
}

func BenchmarkM247AcquireExpiredMessage(b *testing.B) {
	retention := benchmarkM247ExpiredRetention(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := retention.Acquire("orders", 99)
		m247ExpiredMessageSink = err.Error()
	}
}
