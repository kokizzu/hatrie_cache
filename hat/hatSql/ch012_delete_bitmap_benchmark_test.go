package hatSql

import (
	"fmt"
	"runtime"
	"testing"
)

var (
	ch012DeleteBoolMaskSink   []bool
	ch012DeleteBitmapMaskSink typedTableDeleteBitmap
)

func BenchmarkCH012DeleteMaskBacking(b *testing.B) {
	for _, rows := range []int{1024, 10000, 100000} {
		b.Run(fmt.Sprintf("bool-slice/rows=%d", rows), func(b *testing.B) {
			b.ReportMetric(float64(rows), "mask-bytes")
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				mask := make([]bool, rows)
				mask[rows-1] = true
				ch012DeleteBoolMaskSink = mask
			}
			runtime.KeepAlive(ch012DeleteBoolMaskSink)
		})
		b.Run(fmt.Sprintf("packed-bitmap/rows=%d", rows), func(b *testing.B) {
			b.ReportMetric(float64((rows+63)/64*8), "mask-bytes")
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				mask := newTypedTableDeleteBitmap(rows)
				mask.set(rows - 1)
				ch012DeleteBitmapMaskSink = mask
			}
			runtime.KeepAlive(ch012DeleteBitmapMaskSink)
		})
	}
}
