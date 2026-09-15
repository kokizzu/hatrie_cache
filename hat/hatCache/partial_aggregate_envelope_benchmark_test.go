package hatCache_test

import (
	"strconv"
	"testing"

	json "github.com/goccy/go-json"
	hatCache "hatrie_cache/hat/hatCache"
)

var (
	benchmarkAggregateCMSWire []byte
	benchmarkAggregateCMS     hatCache.CountMinSketch
)

func benchmarkCountMinSketchState(b *testing.B) hatCache.CountMinSketch {
	b.Helper()
	sketch, err := hatCache.NewCountMinSketch(2048, 4)
	if err != nil {
		b.Fatal(err)
	}
	for index := 0; index < 20000; index++ {
		sketch.Add("aggregate-key-"+strconv.Itoa(index), 1)
	}
	return sketch
}

func BenchmarkCountMinSketchAggregateStateMarshalCompact(b *testing.B) {
	sketch := benchmarkCountMinSketchState(b)
	wire, err := sketch.MarshalAggregateState()
	if err != nil {
		b.Fatal(err)
	}
	b.ReportMetric(float64(len(wire)), "wire_bytes")
	b.SetBytes(int64(len(wire)))
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		wire, err = sketch.MarshalAggregateState()
		if err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(len(wire)), "wire_bytes")
	benchmarkAggregateCMSWire = wire
}

func BenchmarkCountMinSketchAggregateStateMarshalJSON(b *testing.B) {
	sketch := benchmarkCountMinSketchState(b)
	snapshot := sketch.Snapshot()
	wire, err := json.Marshal(snapshot)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportMetric(float64(len(wire)), "wire_bytes")
	b.SetBytes(int64(len(wire)))
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		wire, err = json.Marshal(snapshot)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(len(wire)), "wire_bytes")
	benchmarkAggregateCMSWire = wire
}

func BenchmarkCountMinSketchAggregateStateUnmarshalCompact(b *testing.B) {
	sketch := benchmarkCountMinSketchState(b)
	wire, err := sketch.MarshalAggregateState()
	if err != nil {
		b.Fatal(err)
	}
	b.ReportMetric(float64(len(wire)), "wire_bytes")
	b.SetBytes(int64(len(wire)))
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		benchmarkAggregateCMS, err = hatCache.NewCountMinSketchFromAggregateState(wire)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(len(wire)), "wire_bytes")
}

func BenchmarkCountMinSketchAggregateStateUnmarshalJSON(b *testing.B) {
	sketch := benchmarkCountMinSketchState(b)
	wire, err := json.Marshal(sketch.Snapshot())
	if err != nil {
		b.Fatal(err)
	}
	b.ReportMetric(float64(len(wire)), "wire_bytes")
	b.SetBytes(int64(len(wire)))
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		var snapshot hatCache.CountMinSketchSnapshot
		if err := json.Unmarshal(wire, &snapshot); err != nil {
			b.Fatal(err)
		}
		benchmarkAggregateCMS, err = hatCache.NewCountMinSketchFromSnapshot(snapshot)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(len(wire)), "wire_bytes")
}
