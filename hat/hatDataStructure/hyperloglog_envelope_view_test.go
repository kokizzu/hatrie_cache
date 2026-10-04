package hatDataStructure

import (
	"strconv"
	"testing"
)

var hyperLogLogEnvelopeViewSink HyperLogLog

func TestHyperLogLogEnvelopeViewAllocationBudget(t *testing.T) {
	hll, err := NewHyperLogLog(10)
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 2048; index++ {
		hll.AddJSONString(strconv.Itoa(index))
	}
	encoded, err := hll.MarshalAggregateState()
	if err != nil {
		t.Fatal(err)
	}

	allocations := testing.AllocsPerRun(20, func() {
		decoded, err := NewHyperLogLogFromAggregateState(encoded)
		if err != nil {
			t.Fatal(err)
		}
		hyperLogLogEnvelopeViewSink = decoded
	})
	if allocations > 2 {
		t.Fatalf("HyperLogLog aggregate decode allocations = %.0f, want <= 2", allocations)
	}
}

func BenchmarkHyperLogLogEnvelopeView(b *testing.B) {
	for _, precision := range []uint8{10, 14, 16} {
		b.Run("precision-"+strconv.Itoa(int(precision)), func(b *testing.B) {
			hll, err := NewHyperLogLog(precision)
			if err != nil {
				b.Fatal(err)
			}
			for index := 0; index < hyperLogLogRegisterCount(precision)*2; index++ {
				hll.AddJSONString(strconv.Itoa(index))
			}
			encoded, err := hll.MarshalAggregateState()
			if err != nil {
				b.Fatal(err)
			}
			b.SetBytes(int64(len(encoded)))
			b.ReportAllocs()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				decoded, err := NewHyperLogLogFromAggregateState(encoded)
				if err != nil {
					b.Fatal(err)
				}
				hyperLogLogEnvelopeViewSink = decoded
			}
		})
	}
}
