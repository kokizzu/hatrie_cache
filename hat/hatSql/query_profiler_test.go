package hatSql

import (
	"errors"
	"testing"
	"time"
)

func TestSQLQueryProfilerRecordsBoundedSamples(t *testing.T) {
	profiler, err := NewSQLQueryProfiler(SQLQueryProfilerOptions{
		MaxQueries:         2,
		MaxSamplesPerQuery: 2,
	})
	if err != nil {
		t.Fatalf("NewSQLQueryProfiler() error = %v", err)
	}

	for index := 1; index <= 3; index++ {
		captured, err := profiler.Record("q1", SQLQueryProfileSample{
			Operator:    "scan",
			CPUTime:     time.Duration(index) * time.Millisecond,
			BlockedTime: time.Duration(index) * time.Microsecond,
			Rows:        uint64(index),
			Bytes:       uint64(index * 10),
		})
		if err != nil || !captured {
			t.Fatalf("Record(q1, %d) = captured %v, err %v", index, captured, err)
		}
	}

	profile, ok := profiler.Profile("q1")
	if !ok {
		t.Fatal("Profile(q1) not found")
	}
	if profile.QueryID != "q1" || profile.SamplesSeen != 3 || profile.SamplesDropped != 1 {
		t.Fatalf("profile counters = %#v", profile)
	}
	if len(profile.Samples) != 2 || profile.Samples[0].CPUTime != 2*time.Millisecond || profile.Samples[1].CPUTime != 3*time.Millisecond {
		t.Fatalf("bounded samples = %#v", profile.Samples)
	}

	if _, err := profiler.Record("q2", SQLQueryProfileSample{Operator: "filter"}); err != nil {
		t.Fatalf("Record(q2) error = %v", err)
	}
	if _, err := profiler.Record("q3", SQLQueryProfileSample{Operator: "project"}); err != nil {
		t.Fatalf("Record(q3) error = %v", err)
	}
	if _, ok := profiler.Profile("q1"); ok {
		t.Fatal("oldest query was not evicted")
	}
}

func TestSQLQueryProfilerSamplingIsDeterministic(t *testing.T) {
	profiler, err := NewSQLQueryProfiler(SQLQueryProfilerOptions{SampleEvery: 2})
	if err != nil {
		t.Fatalf("NewSQLQueryProfiler() error = %v", err)
	}

	for index := 0; index < 5; index++ {
		captured, err := profiler.Record("sampled", SQLQueryProfileSample{Operator: string(rune('a' + index))})
		if err != nil {
			t.Fatalf("Record(%d) error = %v", index, err)
		}
		if captured != (index%2 == 0) {
			t.Fatalf("Record(%d) captured = %v", index, captured)
		}
	}

	profile, ok := profiler.Profile("sampled")
	if !ok || len(profile.Samples) != 3 {
		t.Fatalf("sampled profile = %#v, found %v", profile, ok)
	}
	if profile.Samples[0].Operator != "a" || profile.Samples[1].Operator != "c" || profile.Samples[2].Operator != "e" {
		t.Fatalf("sampled operators = %#v", profile.Samples)
	}
	stats := profiler.Stats()
	if stats.SampleCount != 3 || stats.UnsampledCount != 2 || stats.QueryCount != 1 {
		t.Fatalf("profiler stats = %#v", stats)
	}
}

func TestSQLQueryProfilerCopiesSnapshotsAndSortsProfiles(t *testing.T) {
	profiler, err := NewSQLQueryProfiler(SQLQueryProfilerOptions{})
	if err != nil {
		t.Fatalf("NewSQLQueryProfiler() error = %v", err)
	}
	for _, queryID := range []string{"q2", "q1"} {
		if _, err := profiler.Record(queryID, SQLQueryProfileSample{Operator: "scan"}); err != nil {
			t.Fatalf("Record(%q) error = %v", queryID, err)
		}
	}

	profile, ok := profiler.Profile("q1")
	if !ok {
		t.Fatal("Profile(q1) not found")
	}
	profile.Samples[0].Operator = "mutated"
	again, ok := profiler.Profile("q1")
	if !ok || again.Samples[0].Operator != "scan" {
		t.Fatalf("profile was not copied: %#v", again)
	}

	profiles := profiler.Profiles()
	if len(profiles) != 2 || profiles[0].QueryID != "q1" || profiles[1].QueryID != "q2" {
		t.Fatalf("sorted profiles = %#v", profiles)
	}
}

func TestSQLQueryProfilerRejectsInvalidAndClosedUse(t *testing.T) {
	if _, err := NewSQLQueryProfiler(SQLQueryProfilerOptions{MaxQueries: -1}); !errors.Is(err, ErrSQLQueryProfilerLimitInvalid) {
		t.Fatalf("invalid max queries error = %v", err)
	}
	profiler, err := NewSQLQueryProfiler(SQLQueryProfilerOptions{})
	if err != nil {
		t.Fatalf("NewSQLQueryProfiler() error = %v", err)
	}
	for name, sample := range map[string]SQLQueryProfileSample{
		"operator": {CPUTime: 1},
		"cpu":      {Operator: "scan", CPUTime: -1},
		"blocked":  {Operator: "scan", BlockedTime: -1},
	} {
		if _, err := profiler.Record("q1", sample); !errors.Is(err, map[string]error{
			"operator": ErrSQLQueryProfilerOperatorRequired,
			"cpu":      ErrSQLQueryProfilerDurationInvalid,
			"blocked":  ErrSQLQueryProfilerDurationInvalid,
		}[name]) {
			t.Errorf("invalid %s error = %v", name, err)
		}
	}
	if _, err := profiler.Record("", SQLQueryProfileSample{Operator: "scan"}); !errors.Is(err, ErrSQLQueryProfilerQueryIDRequired) {
		t.Fatalf("blank query ID error = %v", err)
	}
	profiler.Close()
	if _, err := profiler.Record("q1", SQLQueryProfileSample{Operator: "scan"}); !errors.Is(err, ErrSQLQueryProfilerClosed) {
		t.Fatalf("closed profiler error = %v", err)
	}
}
