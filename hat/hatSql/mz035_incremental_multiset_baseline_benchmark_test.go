package hatSql

import "testing"

var mz035BaselineMultisetSink []DifferentialRow

func BenchmarkMZ035RebuildMultisetBaseline(b *testing.B) {
	seed := mz035BenchmarkMultisetSeed()
	delta := []DifferentialRow{
		{Key: "key-0001", Time: 100, Diff: 1, Row: Row{"id": int64(1), "value": "v-1"}},
	}
	input := make([]DifferentialRow, 0, len(seed)+len(delta))
	b.ReportAllocs()
	for range b.N {
		input = input[:0]
		input = append(input, seed...)
		input = append(input, delta...)
		result, err := ConsolidateDifferentialRows(input)
		if err != nil {
			b.Fatal(err)
		}
		mz035BaselineMultisetSink = result
	}
}

func mz035BenchmarkMultisetSeed() []DifferentialRow {
	seed := make([]DifferentialRow, 1024)
	for index := range seed {
		keyID := index % 256
		seed[index] = DifferentialRow{
			Key:  "key-" + mz035BenchmarkInteger(keyID),
			Time: uint64(index % 16),
			Diff: 1,
			Row:  Row{"id": int64(keyID), "value": "v-" + mz035BenchmarkInteger(keyID)},
		}
	}
	return seed
}

func mz035BenchmarkInteger(value int) string {
	if value < 10 {
		return "00" + string(rune('0'+value))
	}
	if value < 100 {
		return "0" + string(rune('0'+value/10)) + string(rune('0'+value%10))
	}
	return string(rune('0'+value/100)) + string(rune('0'+(value/10)%10)) + string(rune('0'+value%10))
}
