package hatDataStructure

import (
	"fmt"
	"testing"
)

var benchmarkHyperLogLogMergeSink uint64

func BenchmarkHyperLogLogMergeC223(b *testing.B) {
	const valueCount = 4096
	values := make([][]byte, valueCount)
	rawBytes := 0
	for index := range values {
		values[index] = []byte(fmt.Sprintf("partition-value-%08d", index))
		rawBytes += len(values[index])
	}
	source := NewDefaultHyperLogLog()
	for _, value := range values {
		source.AddBytes(value)
	}
	stateBytes := len(source.RawRegisters())
	stateWireBytes := len(source.Snapshot().Registers)

	b.Run("replay_raw_values", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(rawBytes))
		b.ResetTimer()
		b.ReportMetric(float64(rawBytes), "raw-bytes/op")
		for range b.N {
			target := NewDefaultHyperLogLog()
			for _, value := range values {
				target.AddBytes(value)
			}
			benchmarkHyperLogLogMergeSink = target.Count()
		}
	})

	b.Run("merge_fixed_state", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(stateWireBytes))
		b.ResetTimer()
		b.ReportMetric(float64(stateBytes), "register-bytes/op")
		b.ReportMetric(float64(stateWireBytes), "snapshot-bytes/op")
		for range b.N {
			target := NewDefaultHyperLogLog()
			if err := target.Merge(source); err != nil {
				b.Fatal(err)
			}
			benchmarkHyperLogLogMergeSink = target.Count()
		}
	})
}
