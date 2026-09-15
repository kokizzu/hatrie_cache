package hatDataStructure_test

import (
	"encoding/json"
	"strconv"
	"testing"

	"hatrie_cache/hat/hatDataStructure"
)

var benchmarkTDigestResult hatDataStructure.TDigest

func benchmarkTDigestState(b *testing.B, size int) (hatDataStructure.TDigest, []byte, []byte) {
	b.Helper()
	digest, err := hatDataStructure.NewTDigest(100)
	if err != nil {
		b.Fatal(err)
	}
	values := make([]float64, size)
	for index := range values {
		values[index] = float64((index*37)%10000) + float64(index%11)/11
	}
	digest.AddValidBatch(values)
	compact, err := digest.MarshalAggregateState()
	if err != nil {
		b.Fatal(err)
	}
	jsonWire, err := json.Marshal(digest.Snapshot())
	if err != nil {
		b.Fatal(err)
	}
	return digest, compact, jsonWire
}

func BenchmarkTDigestAggregateStateCodec(b *testing.B) {
	for _, size := range []int{4096, 16384} {
		digest, compact, jsonWire := benchmarkTDigestState(b, size)

		b.Run("MarshalCompact/"+strconv.Itoa(size), func(b *testing.B) {
			b.ReportAllocs()
			b.ReportMetric(float64(len(compact)), "wire-bytes/op")
			for index := 0; index < b.N; index++ {
				encoded, err := digest.MarshalAggregateState()
				if err != nil {
					b.Fatal(err)
				}
				benchmarkTDigestResult = digest
				_ = encoded
			}
		})

		b.Run("MarshalJSON/"+strconv.Itoa(size), func(b *testing.B) {
			b.ReportAllocs()
			b.ReportMetric(float64(len(jsonWire)), "wire-bytes/op")
			for index := 0; index < b.N; index++ {
				encoded, err := json.Marshal(digest.Snapshot())
				if err != nil {
					b.Fatal(err)
				}
				benchmarkTDigestResult = digest
				_ = encoded
			}
		})

		b.Run("UnmarshalCompact/"+strconv.Itoa(size), func(b *testing.B) {
			b.ReportAllocs()
			b.ReportMetric(float64(len(compact)), "wire-bytes/op")
			for index := 0; index < b.N; index++ {
				decoded, err := hatDataStructure.NewTDigestFromAggregateState(compact)
				if err != nil {
					b.Fatal(err)
				}
				benchmarkTDigestResult = decoded
			}
		})

		b.Run("UnmarshalJSON/"+strconv.Itoa(size), func(b *testing.B) {
			b.ReportAllocs()
			b.ReportMetric(float64(len(jsonWire)), "wire-bytes/op")
			for index := 0; index < b.N; index++ {
				var snapshot hatDataStructure.TDigestSnapshot
				if err := json.Unmarshal(jsonWire, &snapshot); err != nil {
					b.Fatal(err)
				}
				decoded, err := hatDataStructure.NewTDigestFromSnapshot(snapshot)
				if err != nil {
					b.Fatal(err)
				}
				benchmarkTDigestResult = decoded
			}
		})
	}
}
