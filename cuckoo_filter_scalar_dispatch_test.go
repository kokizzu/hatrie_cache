package hatriecache

import (
	"reflect"
	"testing"
	"time"
)

var benchmarkCuckooScalarDispatchSink bool
var benchmarkCuckooBatchDispatchSink int

func TestCuckooFilterScalarAddCheckedCandidateMatchesReference(t *testing.T) {
	values := []interface{}{
		"alpha",
		"with\"quote",
		"<html>&",
		"unicode-日",
		struct {
			Name string `json:"name"`
		}{Name: "structured"},
	}
	for _, value := range values {
		candidate, err := newCuckooFilterData(1024, 0.01)
		if err != nil {
			t.Fatal(err)
		}
		reference := candidate
		for attempt := 0; attempt < 2; attempt++ {
			got, gotErr := cuckooFilterAddCheckedScalarCandidate(&candidate, value)
			want, wantErr := cuckooFilterAddCheckedScalarReference(&reference, value)
			if gotErr != nil || wantErr != nil || got != want {
				t.Fatalf("scalar AddChecked(%#v), attempt %d = %t/%v, want %t/%v", value, attempt, got, gotErr, want, wantErr)
			}
			if !reflect.DeepEqual(candidate, reference) {
				t.Fatalf("scalar AddChecked(%#v), attempt %d state differs", value, attempt)
			}
		}
	}
}

func TestCuckooFilterScalarAddCheckedCandidatePreservesEdgeCases(t *testing.T) {
	var candidateNil *cuckooFilterData
	var referenceNil *cuckooFilterData
	got, gotErr := cuckooFilterAddCheckedScalarCandidate(candidateNil, "value")
	want, wantErr := cuckooFilterAddCheckedScalarReference(referenceNil, "value")
	if got != want || gotErr != wantErr {
		t.Fatalf("nil scalar AddChecked = %t/%v, want %t/%v", got, gotErr, want, wantErr)
	}

	candidateZero := cuckooFilterData{}
	referenceZero := cuckooFilterData{}
	got, gotErr = cuckooFilterAddCheckedScalarCandidate(&candidateZero, "value")
	want, wantErr = cuckooFilterAddCheckedScalarReference(&referenceZero, "value")
	if got != want || gotErr != wantErr || !reflect.DeepEqual(candidateZero, referenceZero) {
		t.Fatalf("zero scalar AddChecked = %t/%v/%#v, want %t/%v/%#v", got, gotErr, candidateZero, want, wantErr, referenceZero)
	}

	candidate, err := newCuckooFilterData(1024, 0.01)
	if err != nil {
		t.Fatal(err)
	}
	reference := candidate
	got, gotErr = cuckooFilterAddCheckedScalarCandidate(&candidate, func() {})
	want, wantErr = cuckooFilterAddCheckedScalarReference(&reference, func() {})
	if gotErr == nil || wantErr == nil || got != want || !reflect.DeepEqual(candidate, reference) || len(candidate.fingerprints) != 0 {
		t.Fatalf("invalid scalar AddChecked = %t/%v/%#v, want %t/%v/%#v", got, gotErr, candidate, want, wantErr, reference)
	}
}

func BenchmarkCuckooFilterScalarAddCheckedAlternating(b *testing.B) {
	type structuredValue struct {
		Name string `json:"name"`
	}
	for _, benchmark := range []struct {
		name  string
		value interface{}
	}{
		{name: "SafeString", value: "key"},
		{name: "EscapedString", value: "with\"quote"},
		{name: "Structured", value: structuredValue{Name: "value"}},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			candidate, err := newCuckooFilterData(4096, 0.01)
			if err != nil {
				b.Fatal(err)
			}
			reference, err := newCuckooFilterData(4096, 0.01)
			if err != nil {
				b.Fatal(err)
			}
			const operationsPerBlock = 128
			var candidateDuration time.Duration
			var referenceDuration time.Duration
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				if iteration&1 == 0 {
					candidateDuration += benchmarkCuckooScalarCandidateBlock(b, &candidate, benchmark.value, operationsPerBlock)
					referenceDuration += benchmarkCuckooScalarReferenceBlock(b, &reference, benchmark.value, operationsPerBlock)
				} else {
					referenceDuration += benchmarkCuckooScalarReferenceBlock(b, &reference, benchmark.value, operationsPerBlock)
					candidateDuration += benchmarkCuckooScalarCandidateBlock(b, &candidate, benchmark.value, operationsPerBlock)
				}
			}
			b.StopTimer()
			operations := float64(b.N * operationsPerBlock)
			b.ReportMetric(float64(candidateDuration.Nanoseconds())/operations, "candidate-ns/op")
			b.ReportMetric(float64(referenceDuration.Nanoseconds())/operations, "reference-ns/op")
		})
	}
}

func BenchmarkCuckooFilterScalarAddCheckedAllocations(b *testing.B) {
	type structuredValue struct {
		Name string `json:"name"`
	}
	for _, benchmark := range []struct {
		name      string
		value     interface{}
		candidate bool
	}{
		{name: "SafeStringReference", value: "key"},
		{name: "SafeStringCandidate", value: "key", candidate: true},
		{name: "EscapedStringReference", value: "with\"quote"},
		{name: "EscapedStringCandidate", value: "with\"quote", candidate: true},
		{name: "StructuredReference", value: structuredValue{Name: "value"}},
		{name: "StructuredCandidate", value: structuredValue{Name: "value"}, candidate: true},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			filter, err := newCuckooFilterData(4096, 0.01)
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				if benchmark.candidate {
					benchmarkCuckooScalarDispatchSink, err = cuckooFilterAddCheckedScalarCandidate(&filter, benchmark.value)
				} else {
					benchmarkCuckooScalarDispatchSink, err = cuckooFilterAddCheckedScalarReference(&filter, benchmark.value)
				}
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkCuckooFilterAddCheckedProduction(b *testing.B) {
	filter, err := newCuckooFilterData(4096, 0.01)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		benchmarkCuckooScalarDispatchSink, err = filter.AddChecked("key")
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCuckooFilterVariadicBatchControl(b *testing.B) {
	values := cuckooFilterDispatchValues(128)
	filter, err := newCuckooFilterData(4096, 0.01)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		benchmarkCuckooBatchDispatchSink, err = filter.AddOneChecked(values[0], values[1:]...)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func cuckooFilterDispatchValues(count int) []interface{} {
	values := make([]interface{}, count)
	for index := range values {
		values[index] = "value"
	}
	return values
}

func benchmarkCuckooScalarCandidateBlock(b *testing.B, filter *cuckooFilterData, value interface{}, operations int) time.Duration {
	start := time.Now()
	for operation := 0; operation < operations; operation++ {
		added, err := cuckooFilterAddCheckedScalarCandidate(filter, value)
		if err != nil {
			b.Fatal(err)
		}
		benchmarkCuckooScalarDispatchSink = added
	}
	return time.Since(start)
}

func benchmarkCuckooScalarReferenceBlock(b *testing.B, filter *cuckooFilterData, value interface{}, operations int) time.Duration {
	start := time.Now()
	for operation := 0; operation < operations; operation++ {
		added, err := cuckooFilterAddCheckedScalarReference(filter, value)
		if err != nil {
			b.Fatal(err)
		}
		benchmarkCuckooScalarDispatchSink = added
	}
	return time.Since(start)
}

func cuckooFilterAddCheckedScalarCandidate(filter *cuckooFilterData, value interface{}) (bool, error) {
	return filter.AddChecked(value)
}

func cuckooFilterAddCheckedScalarReference(filter *cuckooFilterData, value interface{}) (bool, error) {
	added, err := filter.AddOneChecked(value)
	return added > 0, err
}
