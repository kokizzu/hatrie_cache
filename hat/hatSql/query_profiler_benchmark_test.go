package hatSql

import (
	"testing"
	"time"
)

func BenchmarkCH032BaselineProfileNoop(b *testing.B) {
	sample := struct {
		Operator    string
		CPUTime     time.Duration
		BlockedTime time.Duration
		Rows        uint64
		Bytes       uint64
	}{
		Operator:    "scan",
		CPUTime:     time.Microsecond,
		BlockedTime: time.Nanosecond,
		Rows:        128,
		Bytes:       4096,
	}
	var total uint64
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		total += sample.Rows + sample.Bytes + uint64(sample.CPUTime) + uint64(sample.BlockedTime)
	}
	b.StopTimer()
	if total == 0 {
		b.Fatal("baseline was optimized away")
	}
}

func BenchmarkCH032QueryProfilerRecord(b *testing.B) {
	profiler, err := NewSQLQueryProfiler(SQLQueryProfilerOptions{MaxSamplesPerQuery: 64})
	if err != nil {
		b.Fatal(err)
	}
	sample := SQLQueryProfileSample{
		Operator:  "scan",
		CPUTime:   time.Microsecond,
		Timestamp: time.Unix(1, 0),
	}
	for index := 0; index < 64; index++ {
		if _, err := profiler.Record("q1", sample); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	var captured bool
	for index := 0; index < b.N; index++ {
		captured, err = profiler.Record("q1", sample)
	}
	b.StopTimer()
	if err != nil || !captured {
		b.Fatalf("Record() captured %v, error %v", captured, err)
	}
}

func BenchmarkCH032QueryProfilerRecordSampled(b *testing.B) {
	profiler, err := NewSQLQueryProfiler(SQLQueryProfilerOptions{SampleEvery: 16})
	if err != nil {
		b.Fatal(err)
	}
	sample := SQLQueryProfileSample{Operator: "scan", Timestamp: time.Unix(1, 0)}
	b.ResetTimer()
	var capturedCount int
	var captured bool
	for index := 0; index < b.N; index++ {
		captured, err = profiler.Record("q1", sample)
		if captured {
			capturedCount++
		}
	}
	b.StopTimer()
	if err != nil || capturedCount == 0 {
		b.Fatalf("Record() captured %d samples, error %v", capturedCount, err)
	}
}

func BenchmarkCH032QueryProfilerProfile(b *testing.B) {
	profiler, err := NewSQLQueryProfiler(SQLQueryProfilerOptions{})
	if err != nil {
		b.Fatal(err)
	}
	for index := 0; index < 64; index++ {
		if _, err := profiler.Record("q1", SQLQueryProfileSample{Operator: "scan", Timestamp: time.Unix(1, 0)}); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	var profile SQLQueryProfile
	var ok bool
	for index := 0; index < b.N; index++ {
		profile, ok = profiler.Profile("q1")
	}
	b.StopTimer()
	if !ok || len(profile.Samples) != 64 {
		b.Fatalf("Profile() = %#v, found %v", profile, ok)
	}
}
