package hatSql

import (
	"context"
	"testing"
)

// BenchmarkCHU39GateAcquireReleaseBaseline measures the pre-feature FIFO gate
// path. It remains as the control for the default zero-priority path.
func BenchmarkCHU39GateAcquireReleaseBaseline(b *testing.B) {
	gate := newNamespaceQueryGate(1)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := gate.acquire(ctx); err != nil {
			b.Fatal(err)
		}
		gate.release()
	}
}
