package hatCache

import (
	"reflect"
	"strconv"
	"testing"
	"time"
)

var benchmarkCuckooScalarDeleteSink bool
var benchmarkCuckooPublicDeleteSink int

func TestCuckooFilterScalarDeleteCandidateMatchesReference(t *testing.T) {
	type structuredValue struct {
		Name string `json:"name"`
	}
	values := []interface{}{
		"alpha",
		"with\"quote",
		"unicode-\u65e5",
		structuredValue{Name: "structured"},
		Map{"name": "nested"},
	}
	for _, value := range values {
		candidate := newPopulatedCuckooFilterForScalarDelete(t, value)
		reference := newPopulatedCuckooFilterForScalarDelete(t, value)
		for attempt := 0; attempt < 2; attempt++ {
			got, gotErr := cuckooFilterScalarDeleteCandidate(&candidate, value)
			want, wantErr := cuckooFilterScalarDeleteReference(&reference, value)
			if gotErr != nil || wantErr != nil || got != want {
				t.Fatalf("scalar delete %#v, attempt %d = %t/%v, want %t/%v", value, attempt, got, gotErr, want, wantErr)
			}
			if !reflect.DeepEqual(candidate, reference) {
				t.Fatalf("scalar delete %#v, attempt %d state differs", value, attempt)
			}
		}
	}
}

func TestCuckooFilterScalarDeleteCandidatePreservesEdgeCases(t *testing.T) {
	var candidateNil *cuckooFilterData
	var referenceNil *cuckooFilterData
	got, gotErr := cuckooFilterScalarDeleteCandidate(candidateNil, "value")
	want, wantErr := cuckooFilterScalarDeleteReference(referenceNil, "value")
	if got != want || gotErr != wantErr {
		t.Fatalf("nil scalar delete = %t/%v, want %t/%v", got, gotErr, want, wantErr)
	}

	candidateZero := cuckooFilterData{}
	referenceZero := cuckooFilterData{}
	got, gotErr = cuckooFilterScalarDeleteCandidate(&candidateZero, "value")
	want, wantErr = cuckooFilterScalarDeleteReference(&referenceZero, "value")
	if got != want || gotErr != wantErr || !reflect.DeepEqual(candidateZero, referenceZero) {
		t.Fatalf("zero scalar delete = %t/%v/%#v, want %t/%v/%#v", got, gotErr, candidateZero, want, wantErr, referenceZero)
	}

	candidate := newPopulatedCuckooFilterForScalarDelete(t, "value")
	reference := candidate
	got, gotErr = cuckooFilterScalarDeleteCandidate(&candidate, func() {})
	want, wantErr = cuckooFilterScalarDeleteReference(&reference, func() {})
	if gotErr == nil || wantErr == nil || got != want || !reflect.DeepEqual(candidate, reference) {
		t.Fatalf("invalid scalar delete = %t/%v/%#v, want %t/%v/%#v", got, gotErr, candidate, want, wantErr, reference)
	}
}

func TestCuckooFilterScalarDeletePublicBehavior(t *testing.T) {
	ht := newTestTrie(t)
	value := Map{"name": "stored"}
	if added, err := ht.AddCuckooFilterChecked("filter", value); err != nil || added != 1 {
		t.Fatalf("AddCuckooFilterChecked = %d/%v, want 1/nil", added, err)
	}
	value["name"] = "caller"
	if deleted, err := ht.DeleteCuckooFilterChecked("filter", Map{"name": "stored"}); err != nil || deleted != 1 {
		t.Fatalf("DeleteCuckooFilterChecked(existing) = %d/%v, want 1/nil", deleted, err)
	}
	if deleted, err := ht.DeleteCuckooFilterChecked("filter", Map{"name": "stored"}); err != nil || deleted != 0 {
		t.Fatalf("DeleteCuckooFilterChecked(missing) = %d/%v, want 0/nil", deleted, err)
	}
	if deleted, err := ht.DeleteCuckooFilterChecked("missing", "value"); err != nil || deleted != 0 {
		t.Fatalf("DeleteCuckooFilterChecked(missing key) = %d/%v, want 0/nil", deleted, err)
	}
	if deleted, err := ht.DeleteCuckooFilterChecked("filter", func() {}); err == nil || deleted != 0 {
		t.Fatalf("DeleteCuckooFilterChecked(invalid) = %d/%v, want 0/error", deleted, err)
	}
}

func BenchmarkCuckooFilterScalarDeleteAlternating(b *testing.B) {
	type structuredValue struct {
		Name string `json:"name"`
	}
	for _, benchmark := range []struct {
		name  string
		value interface{}
	}{
		{name: "SafeString", value: "value"},
		{name: "EscapedString", value: "with\"quote"},
		{name: "Structured", value: structuredValue{Name: "value"}},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			candidate := newPopulatedCuckooFilterForScalarDelete(b, benchmark.value)
			reference := newPopulatedCuckooFilterForScalarDelete(b, benchmark.value)
			key, err := cuckooFilterItemKey(benchmark.value)
			if err != nil {
				b.Fatal(err)
			}
			const operationsPerBlock = 128
			var candidateDuration time.Duration
			var referenceDuration time.Duration
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				if iteration&1 == 0 {
					candidateDuration += benchmarkCuckooScalarDeleteCandidateBlock(b, &candidate, benchmark.value, key, operationsPerBlock)
					referenceDuration += benchmarkCuckooScalarDeleteReferenceBlock(b, &reference, benchmark.value, key, operationsPerBlock)
				} else {
					referenceDuration += benchmarkCuckooScalarDeleteReferenceBlock(b, &reference, benchmark.value, key, operationsPerBlock)
					candidateDuration += benchmarkCuckooScalarDeleteCandidateBlock(b, &candidate, benchmark.value, key, operationsPerBlock)
				}
			}
			b.StopTimer()
			operations := float64(b.N * operationsPerBlock)
			b.ReportMetric(float64(candidateDuration.Nanoseconds())/operations, "candidate-ns/op")
			b.ReportMetric(float64(referenceDuration.Nanoseconds())/operations, "reference-ns/op")
		})
	}
}

func BenchmarkCuckooFilterScalarDeleteAllocations(b *testing.B) {
	type structuredValue struct {
		Name string `json:"name"`
	}
	for _, benchmark := range []struct {
		name      string
		value     interface{}
		candidate bool
	}{
		{name: "SafeStringReference", value: "value"},
		{name: "SafeStringCandidate", value: "value", candidate: true},
		{name: "StructuredReference", value: structuredValue{Name: "value"}},
		{name: "StructuredCandidate", value: structuredValue{Name: "value"}, candidate: true},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			filter := newPopulatedCuckooFilterForScalarDelete(b, benchmark.value)
			key, err := cuckooFilterItemKey(benchmark.value)
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				if benchmark.candidate {
					benchmarkCuckooScalarDeleteSink, err = cuckooFilterScalarDeleteCandidate(&filter, benchmark.value)
				} else {
					benchmarkCuckooScalarDeleteSink, err = cuckooFilterScalarDeleteReference(&filter, benchmark.value)
				}
				if err != nil {
					b.Fatal(err)
				}
				filter.addKey(key)
			}
		})
	}
}

func BenchmarkCuckooFilterScalarDeleteProductionControls(b *testing.B) {
	type structuredValue struct {
		Name string `json:"name"`
	}
	for _, benchmark := range []struct {
		name   string
		value  interface{}
		values []interface{}
		add    bool
	}{
		{name: "ExistingString", value: "value", add: true},
		{name: "ExistingStructured", value: structuredValue{Name: "value"}, add: true},
		{name: "MissingString", value: "missing"},
		{name: "Batch2", value: "value-0", values: cuckooFilterDeleteValues(1, 2), add: true},
		{name: "Batch16", value: "value-0", values: cuckooFilterDeleteValues(1, 16), add: true},
		{name: "Batch128", value: "value-0", values: cuckooFilterDeleteValues(1, 128), add: true},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			ht := CreateHatTrie()
			defer ht.Destroy()
			if err := ht.UpsertCuckooFilter("filter", 4096, 0.01); err != nil {
				b.Fatal(err)
			}
			hval := ht.Get("filter")
			filter := &ht.cuckooFilters.array[hval.Index]
			allValues := append([]interface{}{benchmark.value}, benchmark.values...)
			keys := make([][]byte, len(allValues))
			for index, value := range allValues {
				var err error
				keys[index], err = cuckooFilterItemKey(value)
				if err != nil {
					b.Fatal(err)
				}
				if benchmark.add {
					filter.addKey(keys[index])
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				var err error
				benchmarkCuckooPublicDeleteSink, err = ht.DeleteCuckooFilterChecked("filter", benchmark.value, benchmark.values...)
				if err != nil {
					b.Fatal(err)
				}
				if benchmark.add {
					for _, key := range keys {
						filter.addKey(key)
					}
				}
			}
		})
	}
}

func newPopulatedCuckooFilterForScalarDelete(tb testing.TB, value interface{}) cuckooFilterData {
	tb.Helper()
	filter, err := newCuckooFilterData(4096, 0.01)
	if err != nil {
		tb.Fatal(err)
	}
	if added, err := filter.AddChecked(value); err != nil || !added {
		tb.Fatalf("AddChecked(%#v) = %t/%v, want true/nil", value, added, err)
	}
	return filter
}

func cuckooFilterDeleteValues(start int, end int) []interface{} {
	values := make([]interface{}, 0, end-start)
	for index := start; index < end; index++ {
		values = append(values, "value-"+strconv.Itoa(index))
	}
	return values
}

func benchmarkCuckooScalarDeleteCandidateBlock(b *testing.B, filter *cuckooFilterData, value interface{}, key []byte, operations int) time.Duration {
	start := time.Now()
	for operation := 0; operation < operations; operation++ {
		var err error
		benchmarkCuckooScalarDeleteSink, err = cuckooFilterScalarDeleteCandidate(filter, value)
		if err != nil {
			b.Fatal(err)
		}
		filter.addKey(key)
	}
	return time.Since(start)
}

func benchmarkCuckooScalarDeleteReferenceBlock(b *testing.B, filter *cuckooFilterData, value interface{}, key []byte, operations int) time.Duration {
	start := time.Now()
	for operation := 0; operation < operations; operation++ {
		var err error
		benchmarkCuckooScalarDeleteSink, err = cuckooFilterScalarDeleteReference(filter, value)
		if err != nil {
			b.Fatal(err)
		}
		filter.addKey(key)
	}
	return time.Since(start)
}

func cuckooFilterScalarDeleteCandidate(filter *cuckooFilterData, value interface{}) (bool, error) {
	return filter.DeleteChecked(value)
}

func cuckooFilterScalarDeleteReference(filter *cuckooFilterData, value interface{}) (bool, error) {
	deleted, err := filter.DeleteOneChecked(value)
	return deleted > 0, err
}
