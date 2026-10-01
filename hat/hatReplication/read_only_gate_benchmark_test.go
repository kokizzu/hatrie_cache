package hatReplication

import (
	"sync/atomic"
	"testing"
)

var readOnlyBenchmarkSink uint32

func BenchmarkDirectReadOnlyCheck(b *testing.B) {
	var readOnly uint32
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if atomic.LoadUint32(&readOnly) != 0 {
			readOnlyBenchmarkSink++
		}
	}
}

func BenchmarkReadOnlyGateCheckWritable(b *testing.B) {
	gate, _ := NewReadOnlyGate(false)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := gate.CheckMutation(); err != nil {
			readOnlyBenchmarkSink++
		}
	}
}

func BenchmarkReadOnlyGateCheckBlocked(b *testing.B) {
	gate, _ := NewReadOnlyGate(true)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := gate.CheckMutation(); err == nil {
			readOnlyBenchmarkSink++
		}
	}
}

func BenchmarkReadOnlyGateCheckReplication(b *testing.B) {
	gate, permit := NewReadOnlyGate(true)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := gate.CheckReplication(permit); err != nil {
			readOnlyBenchmarkSink++
		}
	}
}
