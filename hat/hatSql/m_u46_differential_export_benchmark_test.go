//go:build mu46 || mu46baseline

package hatSql

import "testing"

func BenchmarkDifferentialCheckpointHDF1Encode(b *testing.B) {
	checkpoint := differentialCheckpointBenchmarkFixture()
	payload, err := EncodeDifferentialCheckpoint(checkpoint)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ReportMetric(float64(len(payload)), "payload-bytes")
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := EncodeDifferentialCheckpoint(checkpoint); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDifferentialCheckpointHDF1Decode(b *testing.B) {
	checkpoint := differentialCheckpointBenchmarkFixture()
	payload, err := EncodeDifferentialCheckpoint(checkpoint)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ReportMetric(float64(len(payload)), "payload-bytes")
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := DecodeDifferentialCheckpoint(payload); err != nil {
			b.Fatal(err)
		}
	}
}

func differentialCheckpointBenchmarkFixture() DifferentialCheckpoint {
	rows := make([]DifferentialRow, 256)
	for index := range rows {
		rows[index] = DifferentialRow{
			Key:  "key-" + benchmarkDifferentialCheckpointDecimal(index),
			Time: uint64(index % 64),
			Diff: int64(index%5) - 2,
			Row: Row{
				"active": index%2 == 0,
				"name":   "customer-" + benchmarkDifferentialCheckpointDecimal(index%32),
				"score":  float64(index) * 1.25,
				"value":  int64(index * 10),
			},
		}
	}
	return DifferentialCheckpoint{Frontier: 128, Rows: rows}
}

func benchmarkDifferentialCheckpointDecimal(value int) string {
	const digits = "0123456789"
	if value == 0 {
		return "0"
	}
	var reversed [20]byte
	position := len(reversed)
	for value > 0 {
		position--
		reversed[position] = digits[value%10]
		value /= 10
	}
	return string(reversed[position:])
}
