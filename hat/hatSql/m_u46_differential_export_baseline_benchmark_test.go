//go:build mu46baseline

package hatSql

import (
	"testing"

	json "github.com/goccy/go-json"
)

func BenchmarkDifferentialCheckpointBaselineJSONEncode(b *testing.B) {
	checkpoint := differentialCheckpointBenchmarkFixture()
	payload, err := json.Marshal(checkpoint)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ReportMetric(float64(len(payload)), "payload-bytes")
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := json.Marshal(checkpoint); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDifferentialCheckpointBaselineJSONDecode(b *testing.B) {
	checkpoint := differentialCheckpointBenchmarkFixture()
	payload, err := json.Marshal(checkpoint)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ReportMetric(float64(len(payload)), "payload-bytes")
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		var decoded DifferentialCheckpoint
		if err := json.Unmarshal(payload, &decoded); err != nil {
			b.Fatal(err)
		}
	}
}
