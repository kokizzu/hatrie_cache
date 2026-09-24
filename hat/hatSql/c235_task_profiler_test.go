package hatSql

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func TestSQLTaskProfilerAggregatesReadWriteByTablePartAndColumn(t *testing.T) {
	profiler, err := NewSQLTaskProfiler(SQLTaskProfilerOptions{MaxEntries: 8})
	if err != nil {
		t.Fatalf("new task profiler: %v", err)
	}

	accepted, err := profiler.Record(SQLTaskProfileRecord{
		Table:     "events",
		Part:      "part-0001",
		Column:    "user_id",
		Operation: SQLTaskRead,
		Rows:      10,
		Bytes:     100,
		Duration:  2 * time.Millisecond,
	})
	if err != nil || !accepted {
		t.Fatalf("first read record = accepted %v, err %v", accepted, err)
	}
	accepted, err = profiler.Record(SQLTaskProfileRecord{
		Table:     "events",
		Part:      "part-0001",
		Column:    "user_id",
		Operation: SQLTaskRead,
		Rows:      5,
		Bytes:     40,
		Duration:  time.Millisecond,
	})
	if err != nil || !accepted {
		t.Fatalf("second read record = accepted %v, err %v", accepted, err)
	}
	accepted, err = profiler.Record(SQLTaskProfileRecord{
		Table:     "events",
		Part:      "part-0001",
		Column:    "user_id",
		Operation: SQLTaskWrite,
		Rows:      3,
		Bytes:     24,
		Duration:  3 * time.Millisecond,
	})
	if err != nil || !accepted {
		t.Fatalf("write record = accepted %v, err %v", accepted, err)
	}

	profiles := profiler.Profiles()
	if len(profiles) != 2 {
		t.Fatalf("profile count = %d, want 2", len(profiles))
	}
	read := profiles[0]
	if read.Operation != SQLTaskRead || read.ReadTasks != 2 || read.Rows != 15 || read.Bytes != 140 || read.Duration != 3*time.Millisecond {
		t.Fatalf("read profile = %#v", read)
	}
	write := profiles[1]
	if write.Operation != SQLTaskWrite || write.WriteTasks != 1 || write.Rows != 3 || write.Bytes != 24 || write.Duration != 3*time.Millisecond {
		t.Fatalf("write profile = %#v", write)
	}
}

func TestSQLTaskProfilerBoundsEntriesAndValidatesRecords(t *testing.T) {
	profiler, err := NewSQLTaskProfiler(SQLTaskProfilerOptions{MaxEntries: 1})
	if err != nil {
		t.Fatalf("new task profiler: %v", err)
	}
	if accepted, err := profiler.Record(SQLTaskProfileRecord{Table: "events", Part: "p", Column: "id", Operation: SQLTaskRead}); err != nil || !accepted {
		t.Fatalf("first empty-cost record = accepted %v, err %v", accepted, err)
	}
	if accepted, err := profiler.Record(SQLTaskProfileRecord{Table: "events", Part: "p", Column: "name", Operation: SQLTaskRead}); err != nil || !accepted {
		t.Fatalf("bounded replacement record = accepted %v, err %v", accepted, err)
	}
	if got := profiler.Stats().EvictedEntryCount; got != 1 {
		t.Fatalf("evicted entries = %d, want 1", got)
	}
	if _, err := profiler.Record(SQLTaskProfileRecord{Table: "", Part: "p", Column: "id", Operation: SQLTaskRead}); !errors.Is(err, ErrSQLTaskProfilerFieldRequired) {
		t.Fatalf("missing table error = %v", err)
	}
	if _, err := profiler.Record(SQLTaskProfileRecord{Table: "events", Part: "p", Column: "id", Operation: "scan"}); !errors.Is(err, ErrSQLTaskProfilerOperationInvalid) {
		t.Fatalf("invalid operation error = %v", err)
	}
	if _, err := profiler.Record(SQLTaskProfileRecord{Table: "events", Part: "p", Column: "id", Operation: SQLTaskRead, Duration: -time.Nanosecond}); !errors.Is(err, ErrSQLTaskProfilerDurationInvalid) {
		t.Fatalf("negative duration error = %v", err)
	}
}

func TestSQLTaskProfilerCloseStopsRecording(t *testing.T) {
	profiler, err := NewSQLTaskProfiler(SQLTaskProfilerOptions{MaxEntries: 1})
	if err != nil {
		t.Fatalf("new task profiler: %v", err)
	}
	profiler.Close()
	if _, err := profiler.Record(SQLTaskProfileRecord{Table: "events", Part: "p", Column: "id", Operation: SQLTaskRead}); !errors.Is(err, ErrSQLTaskProfilerClosed) {
		t.Fatalf("closed profiler error = %v", err)
	}
}

func TestSQLTaskProfilerConcurrentRecordKeepsCountersConsistent(t *testing.T) {
	profiler, err := NewSQLTaskProfiler(SQLTaskProfilerOptions{MaxEntries: 2})
	if err != nil {
		t.Fatalf("new task profiler: %v", err)
	}
	record := SQLTaskProfileRecord{Table: "events", Part: "p", Column: "id", Operation: SQLTaskRead, Rows: 1, Bytes: 8}
	const workers = 8
	const recordsPerWorker = 100
	var group sync.WaitGroup
	group.Add(workers)
	for worker := 0; worker < workers; worker++ {
		go func() {
			defer group.Done()
			for index := 0; index < recordsPerWorker; index++ {
				if accepted, err := profiler.Record(record); err != nil || !accepted {
					t.Errorf("record = accepted %v, err %v", accepted, err)
					return
				}
			}
		}()
	}
	group.Wait()
	stats := profiler.Stats()
	if stats.RecordCount != workers*recordsPerWorker || stats.ReadTaskCount != workers*recordsPerWorker {
		t.Fatalf("stats = %#v, want %d records", stats, workers*recordsPerWorker)
	}
	profiles := profiler.Profiles()
	if len(profiles) != 1 || profiles[0].Tasks != workers*recordsPerWorker || profiles[0].Rows != workers*recordsPerWorker {
		t.Fatalf("profiles = %#v", profiles)
	}
}

func TestSQLTaskProfilerRejectsInvalidOptionsAndSaturatesCounters(t *testing.T) {
	if _, err := NewSQLTaskProfiler(SQLTaskProfilerOptions{MaxEntries: -1}); !errors.Is(err, ErrSQLTaskProfilerOptionsInvalid) {
		t.Fatalf("negative max entries error = %v", err)
	}
	if _, err := NewSQLTaskProfiler(SQLTaskProfilerOptions{MaxEntries: maxSQLTaskProfilerMaxEntries + 1}); !errors.Is(err, ErrSQLTaskProfilerOptionsInvalid) {
		t.Fatalf("oversized max entries error = %v", err)
	}
	profiler, err := NewSQLTaskProfiler(SQLTaskProfilerOptions{MaxEntries: 1})
	if err != nil {
		t.Fatalf("new task profiler: %v", err)
	}
	record := SQLTaskProfileRecord{
		Table:     "events",
		Part:      "p",
		Column:    "id",
		Operation: SQLTaskRead,
		Rows:      ^uint64(0),
		Bytes:     ^uint64(0),
		Duration:  maxSQLTaskProfilerDuration,
	}
	if _, err := profiler.Record(record); err != nil {
		t.Fatalf("first saturation record: %v", err)
	}
	if _, err := profiler.Record(record); err != nil {
		t.Fatalf("second saturation record: %v", err)
	}
	profile := profiler.Profiles()[0]
	if profile.Rows != ^uint64(0) || profile.Bytes != ^uint64(0) || profile.Duration != maxSQLTaskProfilerDuration || profile.Tasks != 2 {
		t.Fatalf("saturated profile = %#v", profile)
	}
}
