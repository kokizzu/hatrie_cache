package hatCache

import (
	"encoding/json"
	"testing"
	"time"
)

var (
	benchmarkCanonicalStringLookupBoolSink bool
	benchmarkCanonicalStringLookupEstimate uint64
)

func TestPublicCanonicalStringLookupsPreserveEncodedSemantics(t *testing.T) {
	values := []interface{}{
		"",
		"ordinary-value",
		"with space",
		"<html>&value>",
		`with"quote`,
		`with\backslash`,
		"unicode-日本",
		Map{"id": json.Number("7")},
		Slice{"nested", json.Number("9")},
	}

	ht := newTestTrie(t)
	if err := ht.UpsertBloomFilter("bloom", 128, 0.001); err != nil {
		t.Fatal(err)
	}
	if err := ht.UpsertCuckooFilter("cuckoo", 128, 0.001); err != nil {
		t.Fatal(err)
	}
	if err := ht.UpsertXorFilter("xor", uint64(len(values))); err != nil {
		t.Fatal(err)
	}
	if err := ht.UpsertCountMinSketch("cms", 256, 4); err != nil {
		t.Fatal(err)
	}
	if err := ht.UpsertTopK("topk", uint64(len(values))); err != nil {
		t.Fatal(err)
	}

	for _, value := range values {
		if added, err := ht.AddBloomFilterChecked("bloom", value); err != nil || added != 1 {
			t.Fatalf("AddBloomFilterChecked(%#v) = %d/%v, want 1/nil", value, added, err)
		}
		if added, err := ht.AddCuckooFilterChecked("cuckoo", value); err != nil || added != 1 {
			t.Fatalf("AddCuckooFilterChecked(%#v) = %d/%v, want 1/nil", value, added, err)
		}
		if added, err := ht.AddXorFilterChecked("xor", value); err != nil || added != 1 {
			t.Fatalf("AddXorFilterChecked(%#v) = %d/%v, want 1/nil", value, added, err)
		}
		if estimate, err := ht.IncrementCountMinSketchChecked("cms", value, 3); err != nil || estimate < 3 {
			t.Fatalf("IncrementCountMinSketchChecked(%#v) = %d/%v, want >=3/nil", value, estimate, err)
		}
		if estimate, err := ht.AddTopKChecked("topk", value, 3); err != nil || !estimate.Tracked {
			t.Fatalf("AddTopKChecked(%#v) = %#v/%v, want tracked/nil", value, estimate, err)
		}
	}
	if _, ok, err := ht.BuildXorFilter("xor"); err != nil || !ok {
		t.Fatalf("BuildXorFilter() = ok %v/error %v, want true/nil", ok, err)
	}

	for _, value := range values {
		if hit, err := ht.HasBloomFilterChecked("bloom", value); err != nil || !hit {
			t.Fatalf("HasBloomFilterChecked(%#v) = %v/%v, want true/nil", value, hit, err)
		}
		if hit, err := ht.HasCuckooFilterChecked("cuckoo", value); err != nil || !hit {
			t.Fatalf("HasCuckooFilterChecked(%#v) = %v/%v, want true/nil", value, hit, err)
		}
		if hit, queryable, err := ht.HasXorFilterChecked("xor", value); err != nil || !queryable || !hit {
			t.Fatalf("HasXorFilterChecked(%#v) = %v/%v/%v, want true/true/nil", value, hit, queryable, err)
		}
		if estimate, ok, err := ht.EstimateCountMinSketchChecked("cms", value); err != nil || !ok || estimate < 3 {
			t.Fatalf("EstimateCountMinSketchChecked(%#v) = %d/%v/%v, want >=3/true/nil", value, estimate, ok, err)
		}
		if estimate, err := ht.EstimateTopKChecked("topk", value); err != nil || !estimate.Tracked || estimate.Count < 3 {
			t.Fatalf("EstimateTopKChecked(%#v) = %#v/%v, want tracked >=3/nil", value, estimate, err)
		}
	}
}

func TestPublicCanonicalStringLookupsPreserveMissingAndInvalidSemantics(t *testing.T) {
	ht := newTestTrie(t)
	const value = "ordinary-value"

	if hit, err := ht.HasBloomFilterChecked("missing", value); err != nil || hit {
		t.Fatalf("missing Bloom lookup = %v/%v, want false/nil", hit, err)
	}
	if hit, err := ht.HasCuckooFilterChecked("missing", value); err != nil || hit {
		t.Fatalf("missing Cuckoo lookup = %v/%v, want false/nil", hit, err)
	}
	if hit, queryable, err := ht.HasXorFilterChecked("missing", value); err != nil || hit || queryable {
		t.Fatalf("missing XOR lookup = %v/%v/%v, want false/false/nil", hit, queryable, err)
	}
	if estimate, ok, err := ht.EstimateCountMinSketchChecked("missing", value); err != nil || ok || estimate != 0 {
		t.Fatalf("missing Count-Min lookup = %d/%v/%v, want 0/false/nil", estimate, ok, err)
	}
	if estimate, err := ht.EstimateTopKChecked("missing", value); err != nil || estimate != (TopKEstimate{}) {
		t.Fatalf("missing Top-K lookup = %#v/%v, want zero/nil", estimate, err)
	}

	unsupported := func() {}
	if _, err := ht.HasBloomFilterChecked("missing", unsupported); err == nil {
		t.Fatal("unsupported Bloom value unexpectedly succeeded")
	}
	if _, err := ht.HasCuckooFilterChecked("missing", unsupported); err == nil {
		t.Fatal("unsupported Cuckoo value unexpectedly succeeded")
	}
	if _, _, err := ht.HasXorFilterChecked("missing", unsupported); err == nil {
		t.Fatal("unsupported XOR value unexpectedly succeeded")
	}
	if _, _, err := ht.EstimateCountMinSketchChecked("missing", unsupported); err == nil {
		t.Fatal("unsupported Count-Min value unexpectedly succeeded")
	}
	if _, err := ht.EstimateTopKChecked("missing", unsupported); err == nil {
		t.Fatal("unsupported Top-K value unexpectedly succeeded")
	}
}

func TestJSONPlainStringKeyFastPathMatchesEncodingJSON(t *testing.T) {
	values := []string{"", "ordinary-value", "with space"}
	for value := 0; value < 256; value++ {
		values = append(values, string([]byte{byte(value)}))
	}
	for _, value := range values {
		if jsonPlainStringNeedsCanonicalKey(value) {
			continue
		}
		encoded, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		if got := jsonPlainStringKey(value); got != string(encoded) {
			t.Fatalf("jsonPlainStringKey(%q) = %q, want %q", value, got, encoded)
		}
	}
}

func TestPublicCanonicalStringLookupsAreAllocationFree(t *testing.T) {
	const value = "ordinary-value"
	ht := newCanonicalStringLookupBenchmarkTrie(t)

	tests := []struct {
		name string
		call func()
	}{
		{name: "Bloom", call: func() { benchmarkCanonicalStringLookupBoolSink, _ = ht.HasBloomFilterChecked("bloom", value) }},
		{name: "Cuckoo", call: func() { benchmarkCanonicalStringLookupBoolSink, _ = ht.HasCuckooFilterChecked("cuckoo", value) }},
		{name: "CountMin", call: func() { benchmarkCanonicalStringLookupEstimate, _, _ = ht.EstimateCountMinSketchChecked("cms", value) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if allocations := testing.AllocsPerRun(1000, test.call); allocations != 0 {
				t.Fatalf("safe string lookup allocations = %.2f, want 0", allocations)
			}
		})
	}
}

func BenchmarkPublicCanonicalStringLookups(b *testing.B) {
	const value = "ordinary-value"
	structured := Map{"id": json.Number("7")}
	escaped := `with"quote`

	benchmarks := []struct {
		name    string
		prepare func(*testing.B) func()
	}{
		{name: "BloomStringHit", prepare: func(b *testing.B) func() {
			ht := newCanonicalStringLookupBenchmarkTrie(b)
			return func() { benchmarkCanonicalStringLookupBoolSink, _ = ht.HasBloomFilterChecked("bloom", value) }
		}},
		{name: "CuckooStringHit", prepare: func(b *testing.B) func() {
			ht := newCanonicalStringLookupBenchmarkTrie(b)
			return func() { benchmarkCanonicalStringLookupBoolSink, _ = ht.HasCuckooFilterChecked("cuckoo", value) }
		}},
		{name: "CountMinStringEstimate", prepare: func(b *testing.B) func() {
			ht := newCanonicalStringLookupBenchmarkTrie(b)
			return func() { benchmarkCanonicalStringLookupEstimate, _, _ = ht.EstimateCountMinSketchChecked("cms", value) }
		}},
		{name: "BloomStructuredControl", prepare: func(b *testing.B) func() {
			ht := newCanonicalStringLookupBenchmarkTrie(b)
			if _, err := ht.AddBloomFilterChecked("bloom", structured); err != nil {
				b.Fatal(err)
			}
			return func() { benchmarkCanonicalStringLookupBoolSink, _ = ht.HasBloomFilterChecked("bloom", structured) }
		}},
		{name: "CuckooEscapedControl", prepare: func(b *testing.B) func() {
			ht := newCanonicalStringLookupBenchmarkTrie(b)
			if _, err := ht.AddCuckooFilterChecked("cuckoo", escaped); err != nil {
				b.Fatal(err)
			}
			return func() { benchmarkCanonicalStringLookupBoolSink, _ = ht.HasCuckooFilterChecked("cuckoo", escaped) }
		}},
		{name: "CountMinStructuredControl", prepare: func(b *testing.B) func() {
			ht := newCanonicalStringLookupBenchmarkTrie(b)
			if _, err := ht.IncrementCountMinSketchChecked("cms", structured, 1); err != nil {
				b.Fatal(err)
			}
			return func() {
				benchmarkCanonicalStringLookupEstimate, _, _ = ht.EstimateCountMinSketchChecked("cms", structured)
			}
		}},
	}

	for _, benchmark := range benchmarks {
		b.Run(benchmark.name, func(b *testing.B) {
			call := benchmark.prepare(b)
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				call()
			}
		})
	}
}

func BenchmarkPublicCanonicalStringLookupFallbackAlternating(b *testing.B) {
	structured := Map{"id": json.Number("7")}
	escaped := `with"quote`

	ht := newCanonicalStringLookupBenchmarkTrie(b)
	if _, err := ht.AddBloomFilterChecked("bloom", structured); err != nil {
		b.Fatal(err)
	}
	if _, err := ht.AddCuckooFilterChecked("cuckoo", escaped); err != nil {
		b.Fatal(err)
	}
	if _, err := ht.IncrementCountMinSketchChecked("cms", structured, 1); err != nil {
		b.Fatal(err)
	}
	benchmarks := []struct {
		name      string
		candidate func()
		reference func()
	}{
		{
			name: "BloomStructured",
			candidate: func() {
				benchmarkCanonicalStringLookupBoolSink, _ = ht.HasBloomFilterChecked("bloom", structured)
			},
			reference: func() {
				benchmarkCanonicalStringLookupBoolSink, _ = publicBloomFilterLookupReference(ht, "bloom", structured)
			},
		},
		{
			name: "CuckooEscaped",
			candidate: func() {
				benchmarkCanonicalStringLookupBoolSink, _ = ht.HasCuckooFilterChecked("cuckoo", escaped)
			},
			reference: func() {
				benchmarkCanonicalStringLookupBoolSink, _ = publicCuckooFilterLookupReference(ht, "cuckoo", escaped)
			},
		},
		{
			name: "CountMinStructured",
			candidate: func() {
				benchmarkCanonicalStringLookupEstimate, _, _ = ht.EstimateCountMinSketchChecked("cms", structured)
			},
			reference: func() {
				benchmarkCanonicalStringLookupEstimate, _, _ = publicCountMinSketchLookupReference(ht, "cms", structured)
			},
		},
	}

	for _, benchmark := range benchmarks {
		b.Run(benchmark.name, func(b *testing.B) {
			const operationsPerBlock = 64
			var candidateDuration time.Duration
			var referenceDuration time.Duration
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				if iteration&1 == 0 {
					candidateDuration += benchmarkCanonicalStringLookupBlock(benchmark.candidate, operationsPerBlock)
					referenceDuration += benchmarkCanonicalStringLookupBlock(benchmark.reference, operationsPerBlock)
				} else {
					referenceDuration += benchmarkCanonicalStringLookupBlock(benchmark.reference, operationsPerBlock)
					candidateDuration += benchmarkCanonicalStringLookupBlock(benchmark.candidate, operationsPerBlock)
				}
			}
			b.StopTimer()
			operations := float64(b.N * operationsPerBlock)
			b.ReportMetric(float64(candidateDuration.Nanoseconds())/operations, "candidate-ns/op")
			b.ReportMetric(float64(referenceDuration.Nanoseconds())/operations, "reference-ns/op")
		})
	}
}

func benchmarkCanonicalStringLookupBlock(call func(), operations int) time.Duration {
	start := time.Now()
	for operation := 0; operation < operations; operation++ {
		call()
	}
	return time.Since(start)
}

func publicBloomFilterLookupReference(ht *HatTrie, key string, val interface{}) (bool, error) {
	if ht == nil {
		return false, ErrNilHatTrie
	}
	if partition := ht.localPartitionForKey(key); partition != nil {
		return publicBloomFilterLookupReference(partition, key, val)
	}
	valueKey, err := bloomFilterItemKey(val)
	if err != nil {
		return false, err
	}
	ht.mu.Lock()
	defer ht.mu.Unlock()
	hval, err := ht.getLockedChecked(key)
	if err != nil {
		ht.recordReadLocked(false, key)
		return false, err
	}
	if !hval.IsBloomFilter() {
		ht.recordReadLocked(false, key)
		return false, nil
	}
	hit := ht.bloomFilters.array[hval.Index].containsKey(valueKey)
	ht.recordReadLocked(hit, key)
	return hit, nil
}

func publicCuckooFilterLookupReference(ht *HatTrie, key string, val interface{}) (bool, error) {
	if ht == nil {
		return false, ErrNilHatTrie
	}
	if partition := ht.localPartitionForKey(key); partition != nil {
		return publicCuckooFilterLookupReference(partition, key, val)
	}
	valueKey, err := cuckooFilterItemKey(val)
	if err != nil {
		return false, err
	}
	ht.mu.Lock()
	defer ht.mu.Unlock()
	hval, err := ht.getLockedChecked(key)
	if err != nil {
		ht.recordReadLocked(false, key)
		return false, err
	}
	if !hval.IsCuckooFilter() {
		ht.recordReadLocked(false, key)
		return false, nil
	}
	hit := ht.cuckooFilters.array[hval.Index].containsKey(valueKey)
	ht.recordReadLocked(hit, key)
	return hit, nil
}

func publicCountMinSketchLookupReference(ht *HatTrie, key string, val interface{}) (uint64, bool, error) {
	if ht == nil {
		return 0, false, ErrNilHatTrie
	}
	if partition := ht.localPartitionForKey(key); partition != nil {
		return publicCountMinSketchLookupReference(partition, key, val)
	}
	valueKey, err := countMinSketchItemKey(val)
	if err != nil {
		return 0, false, err
	}
	ht.mu.Lock()
	defer ht.mu.Unlock()
	hval, err := ht.getLockedChecked(key)
	if err != nil {
		ht.recordReadLocked(false, key)
		return 0, false, err
	}
	if !hval.IsCountMinSketch() {
		ht.recordReadLocked(false, key)
		return 0, false, nil
	}
	estimate := ht.countMinSketches.array[hval.Index].estimateKey(valueKey)
	ht.recordReadLocked(true, key)
	return estimate, true, nil
}

func newCanonicalStringLookupBenchmarkTrie(tb testing.TB) *HatTrie {
	tb.Helper()
	ht := CreateHatTrie()
	tb.Cleanup(ht.Destroy)
	const value = "ordinary-value"
	if _, err := ht.AddBloomFilterChecked("bloom", value); err != nil {
		tb.Fatal(err)
	}
	if _, err := ht.AddCuckooFilterChecked("cuckoo", value); err != nil {
		tb.Fatal(err)
	}
	if _, err := ht.IncrementCountMinSketchChecked("cms", value, 1); err != nil {
		tb.Fatal(err)
	}
	return ht
}
