package hatriecache

import (
	"reflect"
	"testing"
	"time"
)

var benchmarkCountMinScalarDispatchSink uint64

func TestCountMinSketchScalarAddCheckedCandidateMatchesReference(t *testing.T) {
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
		candidate, err := newCountMinSketchData(256, 4)
		if err != nil {
			t.Fatal(err)
		}
		reference := candidate
		for _, count := range []uint32{0, 1, 5, 0} {
			got, gotErr := countMinSketchAddCheckedScalarCandidate(&candidate, value, count)
			want, wantErr := countMinSketchAddCheckedScalarReference(&reference, value, count)
			if gotErr != nil || wantErr != nil || got != want {
				t.Fatalf("scalar AddChecked(%#v, %d) = %d/%v, want %d/%v", value, count, got, gotErr, want, wantErr)
			}
			if !reflect.DeepEqual(candidate, reference) {
				t.Fatalf("scalar AddChecked(%#v, %d) state differs", value, count)
			}
		}
	}
}

func TestCountMinSketchScalarAddCheckedCandidatePreservesEdgeCases(t *testing.T) {
	var candidateNil *countMinSketchData
	var referenceNil *countMinSketchData
	got, gotErr := countMinSketchAddCheckedScalarCandidate(candidateNil, "value", 1)
	want, wantErr := countMinSketchAddCheckedScalarReference(referenceNil, "value", 1)
	if got != want || gotErr != wantErr {
		t.Fatalf("nil scalar AddChecked = %d/%v, want %d/%v", got, gotErr, want, wantErr)
	}

	candidateZero := countMinSketchData{}
	referenceZero := countMinSketchData{}
	got, gotErr = countMinSketchAddCheckedScalarCandidate(&candidateZero, "value", 1)
	want, wantErr = countMinSketchAddCheckedScalarReference(&referenceZero, "value", 1)
	if got != want || gotErr != wantErr || !reflect.DeepEqual(candidateZero, referenceZero) {
		t.Fatalf("zero scalar AddChecked = %d/%v/%#v, want %d/%v/%#v", got, gotErr, candidateZero, want, wantErr, referenceZero)
	}

	for _, count := range []uint32{0, 1} {
		candidate, err := newCountMinSketchData(256, 4)
		if err != nil {
			t.Fatal(err)
		}
		reference := candidate
		got, gotErr = countMinSketchAddCheckedScalarCandidate(&candidate, func() {}, count)
		want, wantErr = countMinSketchAddCheckedScalarReference(&reference, func() {}, count)
		if gotErr == nil || wantErr == nil || got != want || !reflect.DeepEqual(candidate, reference) || len(candidate.counters) != 0 {
			t.Fatalf("invalid scalar AddChecked count %d = %d/%v/%#v, want %d/%v/%#v", count, got, gotErr, candidate, want, wantErr, reference)
		}
	}
}

func BenchmarkCountMinSketchScalarAddCheckedAlternating(b *testing.B) {
	type structuredValue struct {
		Name string `json:"name"`
	}
	for _, benchmark := range []struct {
		name     string
		value    interface{}
		count    uint32
		populate bool
	}{
		{name: "SafeStringUpdate", value: "key", count: 1},
		{name: "EscapedStringUpdate", value: "with\"quote", count: 1},
		{name: "StructuredUpdate", value: structuredValue{Name: "value"}, count: 1},
		{name: "SafeStringEstimate", value: "key", populate: true},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			candidate, err := newCountMinSketchData(256, 4)
			if err != nil {
				b.Fatal(err)
			}
			reference, err := newCountMinSketchData(256, 4)
			if err != nil {
				b.Fatal(err)
			}
			if benchmark.populate {
				if _, err := candidate.AddOneChecked(benchmark.value, 1); err != nil {
					b.Fatal(err)
				}
				if _, err := reference.AddOneChecked(benchmark.value, 1); err != nil {
					b.Fatal(err)
				}
			}
			const operationsPerBlock = 128
			var candidateDuration time.Duration
			var referenceDuration time.Duration
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				if iteration&1 == 0 {
					candidateDuration += benchmarkCountMinScalarCandidateBlock(b, &candidate, benchmark.value, benchmark.count, operationsPerBlock)
					referenceDuration += benchmarkCountMinScalarReferenceBlock(b, &reference, benchmark.value, benchmark.count, operationsPerBlock)
				} else {
					referenceDuration += benchmarkCountMinScalarReferenceBlock(b, &reference, benchmark.value, benchmark.count, operationsPerBlock)
					candidateDuration += benchmarkCountMinScalarCandidateBlock(b, &candidate, benchmark.value, benchmark.count, operationsPerBlock)
				}
			}
			b.StopTimer()
			operations := float64(b.N * operationsPerBlock)
			b.ReportMetric(float64(candidateDuration.Nanoseconds())/operations, "candidate-ns/op")
			b.ReportMetric(float64(referenceDuration.Nanoseconds())/operations, "reference-ns/op")
		})
	}
}

func BenchmarkCountMinSketchScalarAddCheckedAllocations(b *testing.B) {
	type structuredValue struct {
		Name string `json:"name"`
	}
	for _, benchmark := range []struct {
		name      string
		value     interface{}
		count     uint32
		candidate bool
	}{
		{name: "SafeStringUpdateReference", value: "key", count: 1},
		{name: "SafeStringUpdateCandidate", value: "key", count: 1, candidate: true},
		{name: "EscapedStringUpdateReference", value: "with\"quote", count: 1},
		{name: "EscapedStringUpdateCandidate", value: "with\"quote", count: 1, candidate: true},
		{name: "StructuredUpdateReference", value: structuredValue{Name: "value"}, count: 1},
		{name: "StructuredUpdateCandidate", value: structuredValue{Name: "value"}, count: 1, candidate: true},
		{name: "SafeStringEstimateReference", value: "key"},
		{name: "SafeStringEstimateCandidate", value: "key", candidate: true},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			sketch, err := newCountMinSketchData(256, 4)
			if err != nil {
				b.Fatal(err)
			}
			if benchmark.count == 0 {
				if _, err := sketch.AddOneChecked(benchmark.value, 1); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				if benchmark.candidate {
					benchmarkCountMinScalarDispatchSink, err = countMinSketchAddCheckedScalarCandidate(&sketch, benchmark.value, benchmark.count)
				} else {
					benchmarkCountMinScalarDispatchSink, err = countMinSketchAddCheckedScalarReference(&sketch, benchmark.value, benchmark.count)
				}
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkCountMinSketchAddCheckedProduction(b *testing.B) {
	sketch, err := newCountMinSketchData(256, 4)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		benchmarkCountMinScalarDispatchSink, err = sketch.AddChecked("key", 1)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCountMinSketchVariadicBatchControl(b *testing.B) {
	values := countMinSketchDispatchValues(128)
	sketch, err := newCountMinSketchData(256, 4)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		benchmarkCountMinScalarDispatchSink, err = sketch.AddOneChecked(values[0], 1, values[1:]...)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func countMinSketchDispatchValues(count int) []interface{} {
	values := make([]interface{}, count)
	for index := range values {
		values[index] = "value"
	}
	return values
}

func benchmarkCountMinScalarCandidateBlock(b *testing.B, sketch *countMinSketchData, value interface{}, count uint32, operations int) time.Duration {
	start := time.Now()
	for operation := 0; operation < operations; operation++ {
		estimate, err := countMinSketchAddCheckedScalarCandidate(sketch, value, count)
		if err != nil {
			b.Fatal(err)
		}
		benchmarkCountMinScalarDispatchSink = estimate
	}
	return time.Since(start)
}

func benchmarkCountMinScalarReferenceBlock(b *testing.B, sketch *countMinSketchData, value interface{}, count uint32, operations int) time.Duration {
	start := time.Now()
	for operation := 0; operation < operations; operation++ {
		estimate, err := countMinSketchAddCheckedScalarReference(sketch, value, count)
		if err != nil {
			b.Fatal(err)
		}
		benchmarkCountMinScalarDispatchSink = estimate
	}
	return time.Since(start)
}

func countMinSketchAddCheckedScalarCandidate(sketch *countMinSketchData, value interface{}, count uint32) (uint64, error) {
	return sketch.AddChecked(value, count)
}

func countMinSketchAddCheckedScalarReference(sketch *countMinSketchData, value interface{}, count uint32) (uint64, error) {
	return sketch.AddOneChecked(value, count)
}
