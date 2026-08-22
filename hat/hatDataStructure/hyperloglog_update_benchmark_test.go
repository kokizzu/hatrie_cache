package hatDataStructure

import (
	"strconv"
	"testing"
)

var benchmarkHyperLogLogChangedSink bool

func BenchmarkHyperLogLogRawUniqueAdd(b *testing.B) {
	values := make([]string, b.N)
	for idx := range values {
		values[idx] = "raw-unique:" + strconv.Itoa(idx)
	}
	hll, err := newHyperLogLogData(MaxHyperLogLogPrecision)
	if err != nil {
		b.Fatal(err)
	}
	hll.ensureRegisters()

	b.ReportAllocs()
	b.ResetTimer()
	for idx := 0; idx < b.N; idx++ {
		benchmarkHyperLogLogChangedSink = hll.addJSONString(values[idx])
	}
}

func BenchmarkHyperLogLogRawBatchAddAndCount4096(b *testing.B) {
	const valuesPerBatch = 4096
	values := make([]string, valuesPerBatch)
	for idx := range values {
		values[idx] = "raw-batch:" + strconv.Itoa(idx)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		b.StopTimer()
		hll, err := newHyperLogLogData(DefaultHyperLogLogPrecision)
		if err != nil {
			b.Fatal(err)
		}
		hll.ensureRegisters()
		b.StartTimer()
		for _, value := range values {
			benchmarkHyperLogLogChangedSink = hll.addJSONString(value)
		}
		benchmarkHyperLogLogCountSink = hll.Count()
	}
}
