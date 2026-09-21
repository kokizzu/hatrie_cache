package hatSql

import (
	"errors"
	"testing"
	"time"
)

func TestCH235SQLQueryProfilerAggregatesBoundedPartColumnObservations(t *testing.T) {
	profiler, err := NewSQLQueryProfiler(SQLQueryProfilerOptions{
		MaxQueries:             2,
		MaxPartColumnsPerQuery: 2,
	})
	if err != nil {
		t.Fatalf("NewSQLQueryProfiler() error = %v", err)
	}

	read := SQLQueryPartColumnSample{
		Part:      "part-1",
		Column:    "status",
		Operation: "READ",
		CPUTime:   time.Millisecond,
		Rows:      10,
		Bytes:     100,
	}
	if captured, err := profiler.RecordPartColumn("q1", read); err != nil || !captured {
		t.Fatalf("first RecordPartColumn() = %v/%v, want captured", captured, err)
	}
	read.CPUTime = 2 * time.Millisecond
	read.Rows = 5
	read.Bytes = 50
	if captured, err := profiler.RecordPartColumn("q1", read); err != nil || !captured {
		t.Fatalf("second RecordPartColumn() = %v/%v, want captured", captured, err)
	}

	write := SQLQueryPartColumnSample{
		Part:      "part-1",
		Column:    "status",
		Operation: "write",
		CPUTime:   3 * time.Millisecond,
		Rows:      2,
		Bytes:     20,
	}
	if captured, err := profiler.RecordPartColumn("q1", write); err != nil || !captured {
		t.Fatalf("write RecordPartColumn() = %v/%v, want captured", captured, err)
	}

	if captured, err := profiler.RecordPartColumn("q1", SQLQueryPartColumnSample{
		Part:      "part-2",
		Column:    "amount",
		Operation: "read",
	}); err != nil || captured {
		t.Fatalf("bounded RecordPartColumn() = %v/%v, want dropped without error", captured, err)
	}

	profile, ok := profiler.PartColumnProfile("q1")
	if !ok || len(profile.Entries) != 2 || profile.DroppedObservations != 1 {
		t.Fatalf("PartColumnProfile() = %#v/%v, want two entries and one dropped observation", profile, ok)
	}
	if profile.Entries[0].Operation != "read" || profile.Entries[1].Operation != "write" {
		t.Fatalf("PartColumnProfile() entries = %#v, want deterministic operation order", profile.Entries)
	}
	readProfile := profile.Entries[0]
	if readProfile.Part != "part-1" || readProfile.Column != "status" || readProfile.Observations != 2 || readProfile.CPUTime != 3*time.Millisecond || readProfile.Rows != 15 || readProfile.Bytes != 150 {
		t.Fatalf("read part-column profile = %#v", readProfile)
	}

	profile.Entries[0].Column = "mutated"
	again, ok := profiler.PartColumnProfile("q1")
	if !ok || again.Entries[0].Column != "status" {
		t.Fatalf("PartColumnProfile() was not isolated: %#v/%v", again, ok)
	}

	stats := profiler.Stats()
	if stats.PartColumnObservationCount != 3 || stats.DroppedPartColumnObservationCount != 1 {
		t.Fatalf("profiler part-column stats = %#v", stats)
	}
}

func TestCH235SQLQueryProfilerValidatesPartColumnObservations(t *testing.T) {
	profiler, err := NewSQLQueryProfiler(SQLQueryProfilerOptions{})
	if err != nil {
		t.Fatalf("NewSQLQueryProfiler() error = %v", err)
	}
	for name, sample := range map[string]SQLQueryPartColumnSample{
		"part":      {Column: "status", Operation: "read"},
		"column":    {Part: "part-1", Operation: "read"},
		"operation": {Part: "part-1", Column: "status"},
		"cpu":       {Part: "part-1", Column: "status", Operation: "read", CPUTime: -1},
	} {
		if _, err := profiler.RecordPartColumn("q1", sample); !errors.Is(err, map[string]error{
			"part":      ErrSQLQueryProfilerPartRequired,
			"column":    ErrSQLQueryProfilerColumnRequired,
			"operation": ErrSQLQueryProfilerOperationInvalid,
			"cpu":       ErrSQLQueryProfilerDurationInvalid,
		}[name]) {
			t.Errorf("invalid %s error = %v", name, err)
		}
	}
}

func TestCH235SQLQueryProfilerPartColumnCountersSaturate(t *testing.T) {
	profiler, err := NewSQLQueryProfiler(SQLQueryProfilerOptions{MaxPartColumnsPerQuery: 1})
	if err != nil {
		t.Fatalf("NewSQLQueryProfiler() error = %v", err)
	}
	if _, err := profiler.RecordPartColumn("q1", SQLQueryPartColumnSample{
		Part:      "part-1",
		Column:    "value",
		Operation: "read",
		CPUTime:   time.Duration(1<<63 - 1),
		Rows:      ^uint64(0),
		Bytes:     ^uint64(0),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := profiler.RecordPartColumn("q1", SQLQueryPartColumnSample{
		Part:      "part-1",
		Column:    "value",
		Operation: "read",
		CPUTime:   time.Nanosecond,
		Rows:      1,
		Bytes:     1,
	}); err != nil {
		t.Fatal(err)
	}
	profile, ok := profiler.PartColumnProfile("q1")
	if !ok || len(profile.Entries) != 1 {
		t.Fatalf("PartColumnProfile() = %#v/%v", profile, ok)
	}
	entry := profile.Entries[0]
	if entry.CPUTime != time.Duration(1<<63-1) || entry.Rows != ^uint64(0) || entry.Bytes != ^uint64(0) {
		t.Fatalf("saturated part-column entry = %#v", entry)
	}
}
