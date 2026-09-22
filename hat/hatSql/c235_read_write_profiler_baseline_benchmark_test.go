package hatSql

import "testing"

var c235ReadWriteProfilerSink uint64

func BenchmarkC235ReadWriteProfilerNoOp(b *testing.B) {
	sample := SQLReadWriteTaskSample{Rows: 128, Bytes: 8192}
	var total uint64
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		total += sample.Rows + sample.Bytes + uint64(index&1)
	}
	c235ReadWriteProfilerSink = total
}

func BenchmarkC235ReadWriteProfilerRecord(b *testing.B) {
	profiler, err := NewSQLReadWriteProfiler(SQLReadWriteProfilerOptions{})
	if err != nil {
		b.Fatal(err)
	}
	sample := SQLReadWriteTaskSample{Rows: 128, Bytes: 8192}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		captured, err := profiler.RecordRead("events", "part-0", "value", sample)
		if err != nil || !captured {
			b.Fatalf("RecordRead() = %t, %v", captured, err)
		}
	}
}
