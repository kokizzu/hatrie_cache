package hatCache

import (
	"reflect"
	"testing"
	"time"
)

var benchmarkBloomScalarDispatchSink bool
var benchmarkBloomBatchDispatchSink int

func TestBloomFilterScalarAddCheckedCandidateMatchesReference(t *testing.T) {
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
		candidate, err := newBloomFilterData(1024, 0.01)
		if err != nil {
			t.Fatal(err)
		}
		reference := candidate
		for attempt := 0; attempt < 2; attempt++ {
			got, gotErr := bloomFilterAddCheckedScalarCandidate(&candidate, value)
			want, wantErr := bloomFilterAddCheckedScalarReference(&reference, value)
			if gotErr != nil || wantErr != nil || got != want {
				t.Fatalf("scalar AddChecked(%#v), attempt %d = %t/%v, want %t/%v", value, attempt, got, gotErr, want, wantErr)
			}
			if !reflect.DeepEqual(candidate, reference) {
				t.Fatalf("scalar AddChecked(%#v), attempt %d state differs", value, attempt)
			}
		}
	}
}

func TestBloomFilterScalarAddCheckedCandidatePreservesEdgeCases(t *testing.T) {
	var candidateNil *bloomFilterData
	var referenceNil *bloomFilterData
	got, gotErr := bloomFilterAddCheckedScalarCandidate(candidateNil, "value")
	want, wantErr := bloomFilterAddCheckedScalarReference(referenceNil, "value")
	if got != want || gotErr != wantErr {
		t.Fatalf("nil scalar AddChecked = %t/%v, want %t/%v", got, gotErr, want, wantErr)
	}

	candidateZero := bloomFilterData{}
	referenceZero := bloomFilterData{}
	got, gotErr = bloomFilterAddCheckedScalarCandidate(&candidateZero, "value")
	want, wantErr = bloomFilterAddCheckedScalarReference(&referenceZero, "value")
	if got != want || gotErr != wantErr || !reflect.DeepEqual(candidateZero, referenceZero) {
		t.Fatalf("zero scalar AddChecked = %t/%v/%#v, want %t/%v/%#v", got, gotErr, candidateZero, want, wantErr, referenceZero)
	}

	candidate, err := newBloomFilterData(1024, 0.01)
	if err != nil {
		t.Fatal(err)
	}
	reference := candidate
	got, gotErr = bloomFilterAddCheckedScalarCandidate(&candidate, func() {})
	want, wantErr = bloomFilterAddCheckedScalarReference(&reference, func() {})
	if gotErr == nil || wantErr == nil || got != want || !reflect.DeepEqual(candidate, reference) || len(candidate.filter.RawWords()) != 0 {
		t.Fatalf("invalid scalar AddChecked = %t/%v/%#v, want %t/%v/%#v", got, gotErr, candidate, want, wantErr, reference)
	}
}

func BenchmarkBloomFilterScalarAddCheckedAlternating(b *testing.B) {
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
			candidate, err := newBloomFilterData(4096, 0.01)
			if err != nil {
				b.Fatal(err)
			}
			reference, err := newBloomFilterData(4096, 0.01)
			if err != nil {
				b.Fatal(err)
			}
			const operationsPerBlock = 128
			var candidateDuration time.Duration
			var referenceDuration time.Duration
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				if iteration&1 == 0 {
					candidateDuration += benchmarkBloomScalarCandidateBlock(b, &candidate, benchmark.value, operationsPerBlock)
					referenceDuration += benchmarkBloomScalarReferenceBlock(b, &reference, benchmark.value, operationsPerBlock)
				} else {
					referenceDuration += benchmarkBloomScalarReferenceBlock(b, &reference, benchmark.value, operationsPerBlock)
					candidateDuration += benchmarkBloomScalarCandidateBlock(b, &candidate, benchmark.value, operationsPerBlock)
				}
			}
			b.StopTimer()
			operations := float64(b.N * operationsPerBlock)
			b.ReportMetric(float64(candidateDuration.Nanoseconds())/operations, "candidate-ns/op")
			b.ReportMetric(float64(referenceDuration.Nanoseconds())/operations, "reference-ns/op")
		})
	}
}

func BenchmarkBloomFilterScalarAddCheckedAllocations(b *testing.B) {
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
			filter, err := newBloomFilterData(4096, 0.01)
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				if benchmark.candidate {
					benchmarkBloomScalarDispatchSink, err = bloomFilterAddCheckedScalarCandidate(&filter, benchmark.value)
				} else {
					benchmarkBloomScalarDispatchSink, err = bloomFilterAddCheckedScalarReference(&filter, benchmark.value)
				}
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkBloomFilterAddCheckedProduction(b *testing.B) {
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
			filter, err := newBloomFilterData(4096, 0.01)
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				benchmarkBloomScalarDispatchSink, err = filter.AddChecked(benchmark.value)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkBloomFilterVariadicBatchControl(b *testing.B) {
	for _, count := range []int{2, 16, 128} {
		values := bloomFilterDispatchValues(count)
		b.Run(batchSizeName(count), func(b *testing.B) {
			filter, err := newBloomFilterData(4096, 0.01)
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				benchmarkBloomBatchDispatchSink, err = filter.AddOneChecked(values[0], values[1:]...)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func bloomFilterDispatchValues(count int) []interface{} {
	values := make([]interface{}, count)
	for index := range values {
		values[index] = "value"
	}
	return values
}

func batchSizeName(count int) string {
	switch count {
	case 2:
		return "Two"
	case 16:
		return "Sixteen"
	case 128:
		return "OneHundredTwentyEight"
	default:
		panic("unsupported benchmark batch size")
	}
}

func benchmarkBloomScalarCandidateBlock(b *testing.B, filter *bloomFilterData, value interface{}, operations int) time.Duration {
	start := time.Now()
	for operation := 0; operation < operations; operation++ {
		added, err := bloomFilterAddCheckedScalarCandidate(filter, value)
		if err != nil {
			b.Fatal(err)
		}
		benchmarkBloomScalarDispatchSink = added
	}
	return time.Since(start)
}

func benchmarkBloomScalarReferenceBlock(b *testing.B, filter *bloomFilterData, value interface{}, operations int) time.Duration {
	start := time.Now()
	for operation := 0; operation < operations; operation++ {
		added, err := bloomFilterAddCheckedScalarReference(filter, value)
		if err != nil {
			b.Fatal(err)
		}
		benchmarkBloomScalarDispatchSink = added
	}
	return time.Since(start)
}

func bloomFilterAddCheckedScalarCandidate(filter *bloomFilterData, value interface{}) (bool, error) {
	return filter.AddChecked(value)
}

func bloomFilterAddCheckedScalarReference(filter *bloomFilterData, value interface{}) (bool, error) {
	added, err := filter.AddOneChecked(value)
	return added > 0, err
}
