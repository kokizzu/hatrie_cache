package hatriecache

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

var benchmarkTopKScalarDispatchEstimateSink TopKEstimate

const topKScalarDispatchTestMax = 4

func TestTopKGenericScalarDispatchMatchesReference(t *testing.T) {
	values := []interface{}{
		"",
		"alpha",
		"with/slash",
		"with\"quote",
		"with\\backslash",
		"with\ncontrol",
		"<html>&",
		"unicode-日",
		struct {
			Name string `json:"name"`
		}{Name: "structured"},
	}
	for _, capacity := range []uint64{1, 2, 3, 16} {
		for _, value := range values {
			got, err := newTopKData(capacity)
			if err != nil {
				t.Fatal(err)
			}
			want := got
			for _, seed := range []interface{}{"alpha", "beta", "gamma"} {
				if _, err := topKAddOneCheckedReference(&got, seed, 1); err != nil {
					t.Fatal(err)
				}
				if _, err := topKAddOneCheckedReference(&want, seed, 1); err != nil {
					t.Fatal(err)
				}
			}

			gotEstimate, gotErr := got.AddOneChecked(value, 3)
			wantEstimate, wantErr := topKAddOneCheckedReference(&want, value, 3)
			if gotErr != nil || wantErr != nil || gotEstimate != wantEstimate {
				t.Fatalf("capacity %d AddOneChecked(%#v) = %#v/%v, want %#v/%v", capacity, value, gotEstimate, gotErr, wantEstimate, wantErr)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("capacity %d AddOneChecked(%#v) state differs", capacity, value)
			}

			gotEstimate, gotErr = got.AddOneChecked(value, 0)
			wantEstimate, wantErr = topKAddOneCheckedReference(&want, value, 0)
			if gotErr != nil || wantErr != nil || gotEstimate != wantEstimate || !reflect.DeepEqual(got, want) {
				t.Fatalf("capacity %d zero-count AddOneChecked(%#v) differs", capacity, value)
			}
		}
	}

	top := newDefaultTopKData()
	if _, err := top.AddOneChecked(func() {}, 1); err == nil {
		t.Fatal("unsupported scalar value error = nil")
	}
	if estimate, err := top.AddOneChecked("alpha", 1, "beta"); err != nil || !estimate.Tracked {
		t.Fatalf("batch estimate/error = %#v/%v", estimate, err)
	}
}

func TestTopKPlainJSONStringValueMatchesCanonicalEncoding(t *testing.T) {
	for valueByte := 0; valueByte <= 0xff; valueByte++ {
		value := string([]byte{byte(valueByte)})
		if !topKPlainJSONStringValue(value) {
			continue
		}
		key, err := topKItemKey(value)
		if err != nil {
			t.Fatal(err)
		}
		if want := topKPlainJSONStringKey(value); key != want {
			t.Fatalf("byte %#x direct key = %q, canonical = %q", valueByte, want, key)
		}
	}
	if value := strings.Repeat("a", topKDirectStringMax); !topKPlainJSONStringValue(value) {
		t.Fatalf("%d-byte safe value rejected", len(value))
	}
	if value := strings.Repeat("a", topKDirectStringMax+1); topKPlainJSONStringValue(value) {
		t.Fatalf("%d-byte over-limit value accepted", len(value))
	}
}

func BenchmarkTopKGenericScalarDispatch(b *testing.B) {
	type structuredValue struct {
		Name string `json:"name"`
	}
	for _, benchmark := range []struct {
		name      string
		value     interface{}
		values    []interface{}
		count     uint64
		reference bool
	}{
		{name: "SafeDuplicate", value: "key", count: 1},
		{name: "SafeEstimate", value: "key", count: 0},
		{name: "EscapedDuplicate", value: "with\"quote", count: 1},
		{name: "SafeBoundary", value: strings.Repeat("a", topKScalarDispatchTestMax), count: 1},
		{name: "SafeBoundaryReference", value: strings.Repeat("a", topKScalarDispatchTestMax), count: 1, reference: true},
		{name: "EscapedBoundary", value: strings.Repeat("a", topKScalarDispatchTestMax-1) + "\"", count: 1},
		{name: "EscapedBoundaryReference", value: strings.Repeat("a", topKScalarDispatchTestMax-1) + "\"", count: 1, reference: true},
		{name: "SafeLong", value: strings.Repeat("a", 4096), count: 1},
		{name: "SafeLongReference", value: strings.Repeat("a", 4096), count: 1, reference: true},
		{name: "EscapedLong", value: strings.Repeat("a", 4095) + "\"", count: 1},
		{name: "EscapedLongReference", value: strings.Repeat("a", 4095) + "\"", count: 1, reference: true},
		{name: "StructuredDuplicate", value: structuredValue{Name: "value"}, count: 1},
		{name: "BatchTwo", value: "alpha", values: []interface{}{"beta"}, count: 1},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			top := newDefaultTopKData()
			if _, err := topKAddOneCheckedReference(&top, benchmark.value, 1); err != nil {
				b.Fatal(err)
			}
			for _, value := range benchmark.values {
				if _, err := topKAddOneCheckedReference(&top, value, 1); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				var err error
				if benchmark.reference {
					benchmarkTopKScalarDispatchEstimateSink, err = topKAddOneCheckedReference(&top, benchmark.value, benchmark.count, benchmark.values...)
				} else {
					benchmarkTopKScalarDispatchEstimateSink, err = top.AddOneChecked(benchmark.value, benchmark.count, benchmark.values...)
				}
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkTopKGenericScalarDispatchAlternating(b *testing.B) {
	type structuredValue struct {
		Name string `json:"name"`
	}
	for _, benchmark := range []struct {
		name   string
		value  interface{}
		values []interface{}
		count  uint64
	}{
		{name: "EscapedBoundary", value: strings.Repeat("a", topKScalarDispatchTestMax-1) + "\"", count: 1},
		{name: "SafeLong", value: strings.Repeat("a", 4096), count: 1},
		{name: "EscapedLong", value: strings.Repeat("a", 4095) + "\"", count: 1},
		{name: "StructuredDuplicate", value: structuredValue{Name: "value"}, count: 1},
		{name: "BatchTwo", value: "alpha", values: []interface{}{"beta"}, count: 1},
		{name: "EscapedEstimate", value: "a\""},
		{name: "SafeLongEstimate", value: strings.Repeat("a", 4096)},
		{name: "StructuredEstimate", value: structuredValue{Name: "value"}},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			candidate := newDefaultTopKData()
			reference := newDefaultTopKData()
			for _, top := range []*topKData{&candidate, &reference} {
				if _, err := topKAddOneCheckedReference(top, benchmark.value, 1, benchmark.values...); err != nil {
					b.Fatal(err)
				}
			}

			const operationsPerBlock = 64
			var candidateDuration time.Duration
			var referenceDuration time.Duration
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				if iteration&1 == 0 {
					candidateDuration += benchmarkTopKScalarDispatchCandidateBlock(b, &candidate, benchmark.value, benchmark.count, benchmark.values)
					referenceDuration += benchmarkTopKScalarDispatchReferenceBlock(b, &reference, benchmark.value, benchmark.count, benchmark.values)
				} else {
					referenceDuration += benchmarkTopKScalarDispatchReferenceBlock(b, &reference, benchmark.value, benchmark.count, benchmark.values)
					candidateDuration += benchmarkTopKScalarDispatchCandidateBlock(b, &candidate, benchmark.value, benchmark.count, benchmark.values)
				}
			}
			b.StopTimer()
			operations := float64(b.N * operationsPerBlock)
			b.ReportMetric(float64(candidateDuration.Nanoseconds())/operations, "candidate-ns/op")
			b.ReportMetric(float64(referenceDuration.Nanoseconds())/operations, "reference-ns/op")
		})
	}
}

func benchmarkTopKScalarDispatchCandidateBlock(b *testing.B, top *topKData, value interface{}, count uint64, values []interface{}) time.Duration {
	start := time.Now()
	for operation := 0; operation < 64; operation++ {
		estimate, err := top.AddOneChecked(value, count, values...)
		if err != nil {
			b.Fatal(err)
		}
		benchmarkTopKScalarDispatchEstimateSink = estimate
	}
	return time.Since(start)
}

func benchmarkTopKScalarDispatchReferenceBlock(b *testing.B, top *topKData, value interface{}, count uint64, values []interface{}) time.Duration {
	start := time.Now()
	for operation := 0; operation < 64; operation++ {
		estimate, err := topKAddOneCheckedReference(top, value, count, values...)
		if err != nil {
			b.Fatal(err)
		}
		benchmarkTopKScalarDispatchEstimateSink = estimate
	}
	return time.Since(start)
}

func topKAddOneCheckedReference(top *topKData, value interface{}, count uint64, values ...interface{}) (TopKEstimate, error) {
	if top == nil || top.capacity == 0 {
		return TopKEstimate{}, nil
	}
	if count == 0 {
		key, err := topKLastItemKey(value, values...)
		if err != nil {
			return TopKEstimate{}, err
		}
		return top.estimateKey(key), nil
	}
	prepared, err := prepareTopKItems(value, values...)
	if err != nil {
		return TopKEstimate{}, err
	}
	estimate := TopKEstimate{}
	for _, item := range prepared {
		estimate = top.addPrepared(item, count)
	}
	return estimate, nil
}
