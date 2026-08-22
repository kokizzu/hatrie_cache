package hatriecache

import (
	"reflect"
	"testing"
	"time"
)

var benchmarkHyperLogLogScalarDispatchSink bool
var benchmarkHyperLogLogBatchDispatchSink int

func TestHyperLogLogScalarAddCheckedCandidateMatchesReference(t *testing.T) {
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
		candidate, err := newHyperLogLogData(10)
		if err != nil {
			t.Fatal(err)
		}
		reference := candidate
		for attempt := 0; attempt < 2; attempt++ {
			got, gotErr := hyperLogLogAddCheckedScalarCandidate(&candidate, value)
			want, wantErr := hyperLogLogAddCheckedScalarReference(&reference, value)
			if gotErr != nil || wantErr != nil || got != want {
				t.Fatalf("scalar AddChecked(%#v), attempt %d = %t/%v, want %t/%v", value, attempt, got, gotErr, want, wantErr)
			}
			if !reflect.DeepEqual(candidate, reference) {
				t.Fatalf("scalar AddChecked(%#v), attempt %d state differs: got %#v, want %#v", value, attempt, candidate, reference)
			}
		}
	}
}

func TestHyperLogLogScalarAddCheckedCandidatePreservesEdgeCases(t *testing.T) {
	var candidateNil *hyperLogLogData
	var referenceNil *hyperLogLogData
	got, gotErr := hyperLogLogAddCheckedScalarCandidate(candidateNil, "value")
	want, wantErr := hyperLogLogAddCheckedScalarReference(referenceNil, "value")
	if got != want || gotErr != wantErr {
		t.Fatalf("nil scalar AddChecked = %t/%v, want %t/%v", got, gotErr, want, wantErr)
	}

	candidateZero := hyperLogLogData{}
	referenceZero := hyperLogLogData{}
	got, gotErr = hyperLogLogAddCheckedScalarCandidate(&candidateZero, "value")
	want, wantErr = hyperLogLogAddCheckedScalarReference(&referenceZero, "value")
	if got != want || gotErr != wantErr || !reflect.DeepEqual(candidateZero, referenceZero) {
		t.Fatalf("zero scalar AddChecked = %t/%v/%#v, want %t/%v/%#v", got, gotErr, candidateZero, want, wantErr, referenceZero)
	}

	candidate, err := newHyperLogLogData(10)
	if err != nil {
		t.Fatal(err)
	}
	reference := candidate
	got, gotErr = hyperLogLogAddCheckedScalarCandidate(&candidate, func() {})
	want, wantErr = hyperLogLogAddCheckedScalarReference(&reference, func() {})
	if gotErr == nil || wantErr == nil || got != want || !reflect.DeepEqual(candidate, reference) || len(candidate.hll.RawRegisters()) != 0 {
		t.Fatalf("invalid scalar AddChecked = %t/%v/%#v, want %t/%v/%#v", got, gotErr, candidate, want, wantErr, reference)
	}
}

func BenchmarkHyperLogLogScalarAddCheckedAlternating(b *testing.B) {
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
			candidate, err := newHyperLogLogData(10)
			if err != nil {
				b.Fatal(err)
			}
			reference, err := newHyperLogLogData(10)
			if err != nil {
				b.Fatal(err)
			}
			const operationsPerBlock = 128
			var candidateDuration time.Duration
			var referenceDuration time.Duration
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				if iteration&1 == 0 {
					candidateDuration += benchmarkHyperLogLogScalarCandidateBlock(b, &candidate, benchmark.value, operationsPerBlock)
					referenceDuration += benchmarkHyperLogLogScalarReferenceBlock(b, &reference, benchmark.value, operationsPerBlock)
				} else {
					referenceDuration += benchmarkHyperLogLogScalarReferenceBlock(b, &reference, benchmark.value, operationsPerBlock)
					candidateDuration += benchmarkHyperLogLogScalarCandidateBlock(b, &candidate, benchmark.value, operationsPerBlock)
				}
			}
			b.StopTimer()
			operations := float64(b.N * operationsPerBlock)
			b.ReportMetric(float64(candidateDuration.Nanoseconds())/operations, "candidate-ns/op")
			b.ReportMetric(float64(referenceDuration.Nanoseconds())/operations, "reference-ns/op")
		})
	}
}

func BenchmarkHyperLogLogScalarAddCheckedAllocations(b *testing.B) {
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
			hll, err := newHyperLogLogData(10)
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				if benchmark.candidate {
					benchmarkHyperLogLogScalarDispatchSink, err = hyperLogLogAddCheckedScalarCandidate(&hll, benchmark.value)
				} else {
					benchmarkHyperLogLogScalarDispatchSink, err = hyperLogLogAddCheckedScalarReference(&hll, benchmark.value)
				}
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkHyperLogLogAddCheckedProduction(b *testing.B) {
	hll, err := newHyperLogLogData(10)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		benchmarkHyperLogLogScalarDispatchSink, err = hll.AddChecked("key")
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkHyperLogLogVariadicBatchControl(b *testing.B) {
	values := hyperLogLogDispatchValues(128)
	hll, err := newHyperLogLogData(10)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		benchmarkHyperLogLogBatchDispatchSink, err = hll.AddOneChecked(values[0], values[1:]...)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func hyperLogLogDispatchValues(count int) []interface{} {
	values := make([]interface{}, count)
	for index := range values {
		values[index] = "value"
	}
	return values
}

func benchmarkHyperLogLogScalarCandidateBlock(b *testing.B, hll *hyperLogLogData, value interface{}, operations int) time.Duration {
	start := time.Now()
	for operation := 0; operation < operations; operation++ {
		changed, err := hyperLogLogAddCheckedScalarCandidate(hll, value)
		if err != nil {
			b.Fatal(err)
		}
		benchmarkHyperLogLogScalarDispatchSink = changed
	}
	return time.Since(start)
}

func benchmarkHyperLogLogScalarReferenceBlock(b *testing.B, hll *hyperLogLogData, value interface{}, operations int) time.Duration {
	start := time.Now()
	for operation := 0; operation < operations; operation++ {
		changed, err := hyperLogLogAddCheckedScalarReference(hll, value)
		if err != nil {
			b.Fatal(err)
		}
		benchmarkHyperLogLogScalarDispatchSink = changed
	}
	return time.Since(start)
}

func hyperLogLogAddCheckedScalarCandidate(hll *hyperLogLogData, value interface{}) (bool, error) {
	return hll.AddChecked(value)
}

func hyperLogLogAddCheckedScalarReference(hll *hyperLogLogData, value interface{}) (bool, error) {
	changed, err := hll.AddOneChecked(value)
	return changed > 0, err
}
