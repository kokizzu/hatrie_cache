//go:build !tu37baseline

package hatReplication

import (
	"testing"
	"time"
)

func BenchmarkTU37UnthrottledApplyAdmission(b *testing.B) {
	const batchSize = 128
	var applied uint64
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		applied += batchSize
	}
	if applied == 0 {
		b.Fatal("benchmark did not apply a batch")
	}
}

func BenchmarkTU37ThrottleReserve(b *testing.B) {
	now := time.Unix(300, 0)
	throttle, err := NewApplierThrottle(ApplierThrottleOptions{
		EntriesPerSecond: 1_000_000_000,
		Burst:            1024,
		Now:              func() time.Time { return now },
	})
	if err != nil {
		b.Fatalf("NewApplierThrottle() error = %v", err)
	}
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		if _, err := throttle.Reserve(1); err != nil {
			b.Fatal(err)
		}
	}
}
