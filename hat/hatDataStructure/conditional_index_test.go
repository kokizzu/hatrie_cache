package hatDataStructure

import (
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
)

type conditionalIndexRecord struct {
	Tenant string
	Active bool
	Score  int
}

func TestConditionalFunctionalIndexAdmissionAndReplacement(t *testing.T) {
	var extractorCalls atomic.Int64
	index, err := NewConditionalFunctionalIndex(
		func(record conditionalIndexRecord) string {
			extractorCalls.Add(1)
			return record.Tenant
		},
		func(record conditionalIndexRecord) bool { return record.Active },
		4,
	)
	if err != nil {
		t.Fatalf("NewConditionalFunctionalIndex() error = %v", err)
	}

	if err := index.Upsert(1, conditionalIndexRecord{Tenant: "west", Active: false}); err != nil {
		t.Fatalf("Upsert(rejected) error = %v", err)
	}
	if got := extractorCalls.Load(); got != 0 {
		t.Fatalf("rejected value called extractor %d times", got)
	}
	if got := index.Len(); got != 0 {
		t.Fatalf("Len() after rejected insert = %d, want 0", got)
	}

	for id, record := range map[uint64]conditionalIndexRecord{
		1: {Tenant: "west", Active: true, Score: 10},
		2: {Tenant: "west", Active: false, Score: 20},
		3: {Tenant: "east", Active: true, Score: 30},
	} {
		if err := index.Upsert(id, record); err != nil {
			t.Fatalf("Upsert(%d) error = %v", id, err)
		}
	}
	if got, want := index.Len(), 2; got != want {
		t.Fatalf("Len() = %d, want %d", got, want)
	}
	if got, want := index.DistinctKeys(), 2; got != want {
		t.Fatalf("DistinctKeys() = %d, want %d", got, want)
	}
	if got, want := index.LookupIDs("west"), []uint64{1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("LookupIDs(west) = %v, want %v", got, want)
	}
	if got := testing.AllocsPerRun(100, func() {
		if !index.Contains("west", 1) {
			t.Fatal("Contains(west, 1) = false, want true")
		}
	}); got != 0 {
		t.Fatalf("Contains() allocations = %f, want 0", got)
	}

	// A replacement that stops satisfying the predicate removes the old posting.
	if err := index.Upsert(1, conditionalIndexRecord{Tenant: "west", Active: false}); err != nil {
		t.Fatalf("Upsert(rejected replacement) error = %v", err)
	}
	if got := index.Len(); got != 1 {
		t.Fatalf("Len() after rejected replacement = %d, want 1", got)
	}
	if got := index.LookupIDs("west"); len(got) != 0 {
		t.Fatalf("LookupIDs(west) after rejected replacement = %v, want empty", got)
	}

	// A replacement that starts satisfying the predicate is admitted normally.
	if err := index.Upsert(2, conditionalIndexRecord{Tenant: "west", Active: true, Score: 200}); err != nil {
		t.Fatalf("Upsert(admitted replacement) error = %v", err)
	}
	if got, want := index.LookupIDs("west"), []uint64{2}; !reflect.DeepEqual(got, want) {
		t.Fatalf("LookupIDs(west) after admitted replacement = %v, want %v", got, want)
	}
	if got, want := index.Lookup("west"), []conditionalIndexRecord{{Tenant: "west", Active: true, Score: 200}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Lookup(west) = %v, want %v", got, want)
	}

	if !index.Delete(2) {
		t.Fatal("Delete(2) = false, want true")
	}
	if index.Delete(2) {
		t.Fatal("second Delete(2) = true, want false")
	}
	if got := index.Len(); got != 1 {
		t.Fatalf("Len() after delete = %d, want 1", got)
	}
}

func TestConditionalFunctionalIndexLookupIntoAndClear(t *testing.T) {
	index, err := NewConditionalFunctionalIndex(
		func(record conditionalIndexRecord) string { return record.Tenant },
		func(record conditionalIndexRecord) bool { return record.Active },
		0,
	)
	if err != nil {
		t.Fatalf("NewConditionalFunctionalIndex() error = %v", err)
	}
	for id := uint64(1); id <= 3; id++ {
		if err := index.Upsert(id, conditionalIndexRecord{Tenant: "west", Active: true, Score: int(id)}); err != nil {
			t.Fatalf("Upsert(%d) error = %v", id, err)
		}
	}

	values := make([]conditionalIndexRecord, 0, 3)
	values = index.LookupInto("west", values)
	if got, want := len(values), 3; got != want {
		t.Fatalf("LookupInto() len = %d, want %d", got, want)
	}
	ids := make([]uint64, 0, 3)
	ids = index.LookupIDsInto("west", ids)
	if got, want := ids, []uint64{1, 2, 3}; !reflect.DeepEqual(got, want) {
		t.Fatalf("LookupIDsInto() = %v, want %v", got, want)
	}

	index.Clear()
	if got := index.Len(); got != 0 {
		t.Fatalf("Len() after Clear = %d, want 0", got)
	}
	if got := index.DistinctKeys(); got != 0 {
		t.Fatalf("DistinctKeys() after Clear = %d, want 0", got)
	}
	if got := index.LookupIDs("west"); len(got) != 0 {
		t.Fatalf("LookupIDs() after Clear = %v, want empty", got)
	}
	if err := index.Upsert(4, conditionalIndexRecord{Tenant: "east", Active: true}); err != nil {
		t.Fatalf("Upsert() after Clear error = %v", err)
	}
	if got := index.Len(); got != 1 {
		t.Fatalf("Len() after reuse = %d, want 1", got)
	}
}

func TestConditionalFunctionalIndexConstructorAndNilReceiver(t *testing.T) {
	if _, err := NewConditionalFunctionalIndex[conditionalIndexRecord, string](nil, func(conditionalIndexRecord) bool { return true }, 0); err != ErrFunctionalIndexExtractorRequired {
		t.Fatalf("nil extractor error = %v, want %v", err, ErrFunctionalIndexExtractorRequired)
	}
	if _, err := NewConditionalFunctionalIndex[conditionalIndexRecord, string](func(conditionalIndexRecord) string { return "" }, nil, 0); err != ErrConditionalFunctionalIndexPredicateRequired {
		t.Fatalf("nil predicate error = %v, want %v", err, ErrConditionalFunctionalIndexPredicateRequired)
	}

	var index *ConditionalFunctionalIndex[conditionalIndexRecord, string]
	if err := index.Upsert(1, conditionalIndexRecord{}); err != ErrConditionalFunctionalIndexNil {
		t.Fatalf("nil Upsert error = %v, want %v", err, ErrConditionalFunctionalIndexNil)
	}
	if index.Delete(1) {
		t.Fatal("nil Delete = true, want false")
	}
	if got := index.LookupIDs("west"); got != nil {
		t.Fatalf("nil LookupIDs() = %v, want nil", got)
	}
	if got := index.Lookup("west"); got != nil {
		t.Fatalf("nil Lookup() = %v, want nil", got)
	}
	if got := index.Len(); got != 0 {
		t.Fatalf("nil Len() = %d, want 0", got)
	}
	if got := index.DistinctKeys(); got != 0 {
		t.Fatalf("nil DistinctKeys() = %d, want 0", got)
	}
	index.Clear()
}

func TestConditionalFunctionalIndexConcurrentUse(t *testing.T) {
	index, err := NewConditionalFunctionalIndex(
		func(record conditionalIndexRecord) string { return record.Tenant },
		func(record conditionalIndexRecord) bool { return record.Active },
		64,
	)
	if err != nil {
		t.Fatalf("NewConditionalFunctionalIndex() error = %v", err)
	}

	var waitGroup sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		worker := worker
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			for iteration := 0; iteration < 250; iteration++ {
				id := uint64(worker*250 + iteration)
				if err := index.Upsert(id, conditionalIndexRecord{Tenant: "west", Active: iteration%2 == 0}); err != nil {
					t.Errorf("Upsert(%d) error = %v", id, err)
					return
				}
				_ = index.Contains("west", id)
				_ = index.LookupIDs("west")
			}
		}()
	}
	waitGroup.Wait()
}

func BenchmarkConditionalFunctionalIndexUpsert(b *testing.B) {
	index, err := NewConditionalFunctionalIndex(
		func(record conditionalIndexRecord) string { return record.Tenant },
		func(record conditionalIndexRecord) bool { return record.Active },
		b.N/10+1,
	)
	if err != nil {
		b.Fatal(err)
	}
	records := make([]conditionalIndexRecord, 10000)
	for i := range records {
		records[i] = conditionalIndexRecord{Tenant: "tenant", Active: i%10 == 0, Score: i}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := index.Upsert(uint64(i%len(records)), records[i%len(records)]); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFunctionalIndexUpsertAllRows(b *testing.B) {
	index, err := NewFunctionalIndex(
		func(record conditionalIndexRecord) string { return record.Tenant },
		b.N/10+1,
	)
	if err != nil {
		b.Fatal(err)
	}
	records := make([]conditionalIndexRecord, 10000)
	for i := range records {
		records[i] = conditionalIndexRecord{Tenant: "tenant", Active: i%10 == 0, Score: i}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := index.Upsert(uint64(i%len(records)), records[i%len(records)]); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkConditionalFunctionalIndexBuild(b *testing.B) {
	const rows = 10000
	b.ReportAllocs()
	for iteration := 0; iteration < b.N; iteration++ {
		index, err := NewConditionalFunctionalIndex(
			func(record conditionalIndexRecord) string { return record.Tenant },
			func(record conditionalIndexRecord) bool { return record.Active },
			rows/10,
		)
		if err != nil {
			b.Fatal(err)
		}
		for row := 0; row < rows; row++ {
			if err := index.Upsert(uint64(row), conditionalIndexRecord{Tenant: "tenant", Active: row%10 == 0, Score: row}); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkFunctionalIndexBuildAllRows(b *testing.B) {
	const rows = 10000
	b.ReportAllocs()
	for iteration := 0; iteration < b.N; iteration++ {
		index, err := NewFunctionalIndex(
			func(record conditionalIndexRecord) string { return record.Tenant },
			rows,
		)
		if err != nil {
			b.Fatal(err)
		}
		for row := 0; row < rows; row++ {
			if err := index.Upsert(uint64(row), conditionalIndexRecord{Tenant: "tenant", Active: row%10 == 0, Score: row}); err != nil {
				b.Fatal(err)
			}
		}
	}
}
