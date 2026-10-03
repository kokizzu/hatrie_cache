package hatSql

import "testing"

func BenchmarkCHU06PersistentDeleteBitmap(b *testing.B) {
	source := ch005BenchmarkPatchTable(b)
	encoded, err := source.MarshalDeleteBitmap()
	if err != nil {
		b.Fatal(err)
	}
	full, err := source.MarshalPatchState()
	if err != nil {
		b.Fatal(err)
	}
	restored := ch005BenchmarkPatchTable(b)
	b.Run("Marshal", func(b *testing.B) {
		b.ReportAllocs()
		b.ReportMetric(float64(len(encoded)), "snapshot_bytes")
		b.ReportMetric(float64(len(full)), "full_snapshot_bytes")
		var sink []byte
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			sink, err = source.MarshalDeleteBitmap()
			if err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		b.ReportMetric(float64(len(encoded)), "snapshot_bytes")
		b.ReportMetric(float64(len(full)), "full_snapshot_bytes")
		_ = sink
	})
	b.Run("Restore", func(b *testing.B) {
		b.ReportAllocs()
		b.ReportMetric(float64(len(encoded)), "snapshot_bytes")
		b.ReportMetric(float64(len(full)), "full_snapshot_bytes")
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			if err := restored.RestoreDeleteBitmap(encoded); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		b.ReportMetric(float64(len(encoded)), "snapshot_bytes")
		b.ReportMetric(float64(len(full)), "full_snapshot_bytes")
	})
	b.Run("MarshalCold", func(b *testing.B) {
		b.ReportAllocs()
		b.ReportMetric(float64(len(encoded)), "snapshot_bytes")
		b.ReportMetric(float64(len(full)), "full_snapshot_bytes")
		var sink []byte
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			source.mu.Lock()
			source.invalidateTypedTableDeleteBitmapFingerprintLocked()
			source.mu.Unlock()
			sink, err = source.MarshalDeleteBitmap()
			if err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		b.ReportMetric(float64(len(encoded)), "snapshot_bytes")
		b.ReportMetric(float64(len(full)), "full_snapshot_bytes")
		_ = sink
	})
}
