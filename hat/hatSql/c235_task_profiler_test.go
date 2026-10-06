package hatSql

import (
	"errors"
	"testing"
	"time"
)

func TestC235TaskProfilerAggregatesByOperationPartAndColumn(t *testing.T) {
	profiler, err := NewSQLTaskProfiler(SQLTaskProfilerOptions{MaxEntries: 4})
	if err != nil {
		t.Fatalf("NewSQLTaskProfiler() error = %v", err)
	}
	read := SQLTaskProfileSample{
		Operation:        SQLTaskRead,
		Table:            "orders",
		Part:             "part-01",
		Column:           "customer_id",
		Rows:             10,
		Bytes:            1200,
		ElapsedNanos:     40,
		AllocatedBytes:   80,
		AllocatedObjects: 3,
	}
	if captured, err := profiler.Record(read); err != nil || !captured {
		t.Fatalf("first Record() = %v/%v, want captured", captured, err)
	}
	read.Rows = 5
	read.Bytes = 600
	read.ElapsedNanos = 20
	read.AllocatedBytes = 40
	read.AllocatedObjects = 2
	read.Failed = true
	if captured, err := profiler.Record(read); err != nil || !captured {
		t.Fatalf("second Record() = %v/%v, want captured", captured, err)
	}
	if captured, err := profiler.Record(SQLTaskProfileSample{
		Operation:    SQLTaskWrite,
		Table:        "orders",
		Part:         "part-01",
		Column:       "customer_id",
		Rows:         1,
		Bytes:        100,
		ElapsedNanos: 30,
	}); err != nil || !captured {
		t.Fatalf("write Record() = %v/%v, want captured", captured, err)
	}

	profiles := profiler.Profiles()
	if len(profiles) != 2 {
		t.Fatalf("Profiles() length = %d, want 2: %#v", len(profiles), profiles)
	}
	if profiles[0].Operation != SQLTaskRead || profiles[0].Operations != 2 || profiles[0].Rows != 15 || profiles[0].Bytes != 1800 || profiles[0].ElapsedNanos != 60 || profiles[0].AllocatedBytes != 120 || profiles[0].AllocatedObjects != 5 || profiles[0].Failures != 1 {
		t.Fatalf("read profile = %#v", profiles[0])
	}
	if profiles[1].Operation != SQLTaskWrite || profiles[1].Operations != 1 {
		t.Fatalf("write profile = %#v", profiles[1])
	}
	stats := profiler.Stats()
	if stats.EntryCount != 2 || stats.RecordCount != 3 || stats.DroppedRecordCount != 0 {
		t.Fatalf("Stats() = %#v", stats)
	}
}

func TestC235TaskProfilerBoundsAndValidatesInput(t *testing.T) {
	if _, err := NewSQLTaskProfiler(SQLTaskProfilerOptions{MaxEntries: -1}); !errors.Is(err, ErrSQLTaskProfilerLimitInvalid) {
		t.Fatalf("negative MaxEntries error = %v", err)
	}
	profiler, err := NewSQLTaskProfiler(SQLTaskProfilerOptions{MaxEntries: 1})
	if err != nil {
		t.Fatal(err)
	}
	invalid := []SQLTaskProfileSample{
		{Operation: "merge", Table: "orders", Part: "part-01"},
		{Operation: SQLTaskRead, Part: "part-01"},
		{Operation: SQLTaskRead, Table: "orders", Part: "part-01", ElapsedNanos: -1},
	}
	for index, sample := range invalid {
		if _, err := profiler.Record(sample); !errors.Is(err, ErrSQLTaskProfilerInputInvalid) {
			t.Errorf("invalid sample %d error = %v", index, err)
		}
	}
	if captured, err := profiler.Record(SQLTaskProfileSample{Operation: SQLTaskRead, Table: "orders", Part: "part-01", Column: "id"}); err != nil || !captured {
		t.Fatalf("valid Record() = %v/%v", captured, err)
	}
	if captured, err := profiler.Record(SQLTaskProfileSample{Operation: SQLTaskRead, Table: "orders", Part: "part-02", Column: "id"}); err != nil || captured {
		t.Fatalf("bounded Record() = %v/%v, want dropped without error", captured, err)
	}
	if profiler.Stats().DroppedRecordCount != 1 {
		t.Fatalf("dropped count = %#v", profiler.Stats())
	}

	profiler.Close()
	if _, err := profiler.Record(SQLTaskProfileSample{Operation: SQLTaskRead, Table: "orders", Part: "part-01"}); !errors.Is(err, ErrSQLTaskProfilerClosed) {
		t.Fatalf("closed Record() error = %v", err)
	}
}

func TestC235TaskProfilerCopiesAndSaturatesTotals(t *testing.T) {
	profiler, err := NewSQLTaskProfiler(SQLTaskProfilerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	sample := SQLTaskProfileSample{Operation: SQLTaskRead, Table: "orders", Part: "part-01", Column: "id", Rows: ^uint64(0), Bytes: ^uint64(0), ElapsedNanos: time.Duration(^uint64(0) >> 1), AllocatedBytes: ^uint64(0), AllocatedObjects: ^uint64(0)}
	if _, err := profiler.Record(sample); err != nil {
		t.Fatal(err)
	}
	if _, err := profiler.Record(sample); err != nil {
		t.Fatal(err)
	}
	profile := profiler.Profiles()[0]
	if profile.Rows != ^uint64(0) || profile.Bytes != ^uint64(0) || profile.ElapsedNanos != time.Duration(^uint64(0)>>1) || profile.AllocatedBytes != ^uint64(0) || profile.AllocatedObjects != ^uint64(0) {
		t.Fatalf("saturated profile = %#v", profile)
	}
}
