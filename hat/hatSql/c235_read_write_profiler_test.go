package hatSql

import (
	"errors"
	"math"
	"testing"
	"time"
)

func TestC235ReadWriteProfilerAggregatesByPartAndColumn(t *testing.T) {
	profiler, err := NewSQLReadWriteProfiler(SQLReadWriteProfilerOptions{MaxEntries: 8})
	if err != nil {
		t.Fatalf("NewSQLReadWriteProfiler() error = %v", err)
	}
	read := SQLReadWriteTaskSample{Rows: 10, Bytes: 100, Duration: 2 * time.Millisecond, Timestamp: time.Unix(10, 0)}
	if captured, err := profiler.RecordRead(" events ", "part-1", "score", read); err != nil || !captured {
		t.Fatalf("RecordRead() = %t, %v; want captured", captured, err)
	}
	read.Rows = 3
	read.Bytes = 30
	read.Duration = time.Millisecond
	if captured, err := profiler.RecordRead("events", "part-1", "score", read); err != nil || !captured {
		t.Fatalf("second RecordRead() = %t, %v; want captured", captured, err)
	}
	if captured, err := profiler.RecordWrite("events", "part-1", "score", SQLReadWriteTaskSample{
		Rows: 4, Bytes: 40, Duration: 3 * time.Millisecond, Timestamp: time.Unix(11, 0),
	}); err != nil || !captured {
		t.Fatalf("RecordWrite() = %t, %v; want captured", captured, err)
	}

	snapshot := profiler.Snapshot()
	if len(snapshot) != 2 {
		t.Fatalf("Snapshot() length = %d, want 2: %#v", len(snapshot), snapshot)
	}
	if snapshot[0].Operation != SQLReadWriteOperationRead || snapshot[0].Table != "events" || snapshot[0].Part != "part-1" || snapshot[0].Column != "score" || snapshot[0].Tasks != 2 || snapshot[0].Rows != 13 || snapshot[0].Bytes != 130 || snapshot[0].Duration != 3*time.Millisecond {
		t.Fatalf("read aggregate = %#v", snapshot[0])
	}
	if snapshot[1].Operation != SQLReadWriteOperationWrite || snapshot[1].Tasks != 1 || snapshot[1].Rows != 4 || snapshot[1].Bytes != 40 || snapshot[1].Duration != 3*time.Millisecond {
		t.Fatalf("write aggregate = %#v", snapshot[1])
	}
	stats := profiler.Stats()
	if stats.EntryCount != 2 || stats.ReadTaskCount != 2 || stats.WriteTaskCount != 1 || stats.ReadBytes != 130 || stats.WriteBytes != 40 {
		t.Fatalf("Stats() = %#v", stats)
	}
}

func TestC235ReadWriteProfilerBoundsAndValidation(t *testing.T) {
	profiler, err := NewSQLReadWriteProfiler(SQLReadWriteProfilerOptions{MaxEntries: 1})
	if err != nil {
		t.Fatalf("NewSQLReadWriteProfiler() error = %v", err)
	}
	if _, err := profiler.RecordRead("", "part", "column", SQLReadWriteTaskSample{}); !errors.Is(err, ErrSQLReadWriteProfilerTableRequired) {
		t.Fatalf("empty table error = %v", err)
	}
	if _, err := profiler.RecordRead("table", "part", "column", SQLReadWriteTaskSample{Duration: -time.Nanosecond}); !errors.Is(err, ErrSQLReadWriteProfilerDurationInvalid) {
		t.Fatalf("negative duration error = %v", err)
	}
	if _, err := profiler.RecordRead("table", "part", "column", SQLReadWriteTaskSample{Rows: math.MaxUint64, Bytes: math.MaxUint64}); err != nil {
		t.Fatalf("first bounded record error = %v", err)
	}
	if _, err := profiler.RecordWrite("other", "part", "column", SQLReadWriteTaskSample{}); err != nil {
		t.Fatalf("second bounded record error = %v", err)
	}
	stats := profiler.Stats()
	if stats.EntryCount != 1 || stats.EvictedEntryCount != 1 || stats.ReadTaskCount != 1 || stats.WriteTaskCount != 1 {
		t.Fatalf("bounded Stats() = %#v", stats)
	}
	if captured, err := profiler.RecordRead("other", "part", "column", SQLReadWriteTaskSample{Rows: 1, Bytes: 1}); err != nil || !captured {
		t.Fatalf("reused RecordRead() = %t, %v; want captured", captured, err)
	}
	profiler.Close()
	if _, err := profiler.RecordWrite("other", "part", "column", SQLReadWriteTaskSample{}); !errors.Is(err, ErrSQLReadWriteProfilerClosed) {
		t.Fatalf("closed error = %v", err)
	}
}
