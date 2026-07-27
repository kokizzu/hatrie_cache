package hatriecache

import "testing"

var benchmarkCountMinSketchUint64Sink uint64

func TestCountMinSketchRowsMatchReferenceIndexes(t *testing.T) {
	const (
		width = uint64(13)
		depth = uint8(7)
	)
	key := []byte("count-min-reference-key")
	sketch, err := newCountMinSketchData(width, depth)
	if err != nil {
		t.Fatal(err)
	}
	if got := sketch.addKey(key, 3); got != 3 {
		t.Fatalf("estimate after add = %d, want 3", got)
	}

	first := bloomFilterFNV64a(key)
	step := bloomFilterFNV64(key)
	if step == 0 {
		step = bloomFilterFNVPrime64
	}
	step |= 1
	expected := make(map[uint64]struct{}, depth)
	for row := uint8(0); row < depth; row++ {
		column := (first + uint64(row)*step) % width
		expected[uint64(row)*width+column] = struct{}{}
	}
	for index, counter := range sketch.counters {
		_, ok := expected[uint64(index)]
		if ok && counter != 3 {
			t.Fatalf("counter[%d] = %d, want 3", index, counter)
		}
		if !ok && counter != 0 {
			t.Fatalf("counter[%d] = %d outside reference indexes", index, counter)
		}
	}
	if got := sketch.estimateKey(key); got != 3 {
		t.Fatalf("reference estimate = %d, want 3", got)
	}

	generic, err := newCountMinSketchData(width, depth)
	if err != nil {
		t.Fatal(err)
	}
	exact, err := newCountMinSketchData(width, depth)
	if err != nil {
		t.Fatal(err)
	}
	value := "plain-value"
	encoded, err := countMinSketchItemKey(value)
	if err != nil {
		t.Fatal(err)
	}
	generic.addKey(encoded, 5)
	exact.addJSONString(value, 5)
	if len(generic.counters) != len(exact.counters) {
		t.Fatalf("counter lengths differ: generic=%d exact=%d", len(generic.counters), len(exact.counters))
	}
	for index := range generic.counters {
		if generic.counters[index] != exact.counters[index] {
			t.Fatalf("counter[%d]: generic=%d exact=%d", index, generic.counters[index], exact.counters[index])
		}
	}
}

func BenchmarkCountMinSketchDirectRows(b *testing.B) {
	const payloadCount = 1 << 8
	payloads := benchmarkPayloads(payloadCount, 16)
	mask := payloadCount - 1
	sketch := newDefaultCountMinSketchData()
	for _, payload := range payloads {
		sketch.addKey(payload, 1)
	}

	b.Run("Estimate", func(b *testing.B) {
		b.ReportAllocs()
		for iteration := 0; iteration < b.N; iteration++ {
			benchmarkCountMinSketchUint64Sink = sketch.estimateKey(payloads[(iteration*2654435761)&mask])
		}
	})
	b.Run("Increment", func(b *testing.B) {
		b.ReportAllocs()
		for iteration := 0; iteration < b.N; iteration++ {
			benchmarkCountMinSketchUint64Sink = sketch.addKey(payloads[iteration&mask], 1)
		}
	})
}

func BenchmarkCountMinSketchJSONStringRows(b *testing.B) {
	const value = "plain-value"
	callbackSketch := newDefaultCountMinSketchData()
	directSketch := newDefaultCountMinSketchData()
	benchmarkCountMinSketchAddJSONStringCallback(&callbackSketch, value, 1)
	directSketch.addJSONString(value, 1)

	b.Run("EstimateCallback", func(b *testing.B) {
		b.ReportAllocs()
		for iteration := 0; iteration < b.N; iteration++ {
			benchmarkCountMinSketchUint64Sink = benchmarkCountMinSketchEstimateJSONStringCallback(&callbackSketch, value)
		}
	})
	b.Run("EstimateDirect", func(b *testing.B) {
		b.ReportAllocs()
		for iteration := 0; iteration < b.N; iteration++ {
			benchmarkCountMinSketchUint64Sink = directSketch.estimateJSONString(value)
		}
	})
	b.Run("IncrementCallback", func(b *testing.B) {
		b.ReportAllocs()
		for iteration := 0; iteration < b.N; iteration++ {
			benchmarkCountMinSketchUint64Sink = benchmarkCountMinSketchAddJSONStringCallback(&callbackSketch, value, 1)
		}
	})
	b.Run("IncrementDirect", func(b *testing.B) {
		b.ReportAllocs()
		for iteration := 0; iteration < b.N; iteration++ {
			benchmarkCountMinSketchUint64Sink = directSketch.addJSONString(value, 1)
		}
	})
}

func benchmarkCountMinSketchEstimateJSONStringCallback(sketch *countMinSketchData, value string) uint64 {
	estimate := maxCountMinSketchCounter
	benchmarkCountMinSketchVisitJSONStringIndexes(sketch, value, func(index uint64) {
		if sketch.counters[index] < estimate {
			estimate = sketch.counters[index]
		}
	})
	return uint64(estimate)
}

func benchmarkCountMinSketchAddJSONStringCallback(sketch *countMinSketchData, value string, count uint32) uint64 {
	sketch.ensureCounters()
	estimate := uint64(maxCountMinSketchCounter)
	benchmarkCountMinSketchVisitJSONStringIndexes(sketch, value, func(index uint64) {
		next := saturatingAddUint32(sketch.counters[index], count)
		sketch.counters[index] = next
		if uint64(next) < estimate {
			estimate = uint64(next)
		}
	})
	sketch.total = saturatingAddUint64(sketch.total, uint64(count))
	return estimate
}

func benchmarkCountMinSketchVisitJSONStringIndexes(sketch *countMinSketchData, value string, visit func(uint64)) {
	first := bloomFilterFNV64aJSONString(value)
	step := bloomFilterFNV64JSONString(value)
	if step == 0 {
		step = bloomFilterFNVPrime64
	}
	step |= 1
	for row := uint8(0); row < sketch.depth; row++ {
		column := (first + uint64(row)*step) % sketch.width
		visit(uint64(row)*sketch.width + column)
	}
}
