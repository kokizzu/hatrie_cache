package hatDataStructure_test

import (
	"strconv"
	"testing"

	json "github.com/goccy/go-json"
	hatDataStructure "hatrie_cache/hat/hatDataStructure"
)

var (
	benchmarkAggregateHLLWire []byte
	benchmarkAggregateHLL     hatDataStructure.HyperLogLog
)

func benchmarkHyperLogLogState(b *testing.B) hatDataStructure.HyperLogLog {
	b.Helper()
	hll, err := hatDataStructure.NewHyperLogLog(14)
	if err != nil {
		b.Fatal(err)
	}
	for index := 0; index < 20000; index++ {
		hll.AddJSONString("aggregate-key-" + strconv.Itoa(index))
	}
	return hll
}

func BenchmarkHyperLogLogAggregateStateMarshalCompact(b *testing.B) {
	hll := benchmarkHyperLogLogState(b)
	wire, err := hll.MarshalAggregateState()
	if err != nil {
		b.Fatal(err)
	}
	b.ReportMetric(float64(len(wire)), "wire_bytes")
	b.SetBytes(int64(len(wire)))
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		wire, err = hll.MarshalAggregateState()
		if err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(len(wire)), "wire_bytes")
	benchmarkAggregateHLLWire = wire
}

func BenchmarkHyperLogLogAggregateStateMarshalJSON(b *testing.B) {
	hll := benchmarkHyperLogLogState(b)
	snapshot := hll.Snapshot()
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
	benchmarkAggregateHLLWire = wire
}

func BenchmarkHyperLogLogAggregateStateUnmarshalCompact(b *testing.B) {
	hll := benchmarkHyperLogLogState(b)
	wire, err := hll.MarshalAggregateState()
	if err != nil {
		b.Fatal(err)
	}
	b.ReportMetric(float64(len(wire)), "wire_bytes")
	b.SetBytes(int64(len(wire)))
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		benchmarkAggregateHLL, err = hatDataStructure.NewHyperLogLogFromAggregateState(wire)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(len(wire)), "wire_bytes")
}

func BenchmarkHyperLogLogAggregateStateUnmarshalJSON(b *testing.B) {
	hll := benchmarkHyperLogLogState(b)
	wire, err := json.Marshal(hll.Snapshot())
	if err != nil {
		b.Fatal(err)
	}
	b.ReportMetric(float64(len(wire)), "wire_bytes")
	b.SetBytes(int64(len(wire)))
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		var snapshot hatDataStructure.HyperLogLogSnapshot
		if err := json.Unmarshal(wire, &snapshot); err != nil {
			b.Fatal(err)
		}
		benchmarkAggregateHLL, err = hatDataStructure.NewHyperLogLogFromSnapshot(snapshot)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(len(wire)), "wire_bytes")
}
