package hatCodec

import (
	"encoding/binary"
	"errors"
	"testing"
)

var compactTimestampBenchmarkSink []int64

var errCompactTimestampBenchmarkInvalid = errors.New("invalid raw timestamp block")

func benchmarkCompactTimestampValues() []int64 {
	values := make([]int64, 4096)
	start := int64(1700000000000000000)
	for index := range values {
		values[index] = start + int64(index)*1000000
	}
	return values
}

func benchmarkCompactTimestampIrregularValues() []int64 {
	values := make([]int64, 4096)
	state := uint64(0x9e3779b97f4a7c15)
	for index := range values {
		state = state*6364136223846793005 + 1442695040888963407
		values[index] = int64(state)
	}
	return values
}

func encodeRawTimestampBenchmark(values []int64) []byte {
	encoded := make([]byte, binary.MaxVarintLen64+len(values)*8)
	offset := binary.PutUvarint(encoded, uint64(len(values)))
	for _, value := range values {
		binary.LittleEndian.PutUint64(encoded[offset:], uint64(value))
		offset += 8
	}
	return encoded[:offset]
}

func decodeRawTimestampBenchmark(encoded []byte) ([]int64, error) {
	count, offset := binary.Uvarint(encoded)
	if offset <= 0 || count != uint64((len(encoded)-offset)/8) || (len(encoded)-offset)%8 != 0 {
		return nil, errCompactTimestampBenchmarkInvalid
	}
	values := make([]int64, int(count))
	for index := range values {
		values[index] = int64(binary.LittleEndian.Uint64(encoded[offset:]))
		offset += 8
	}
	return values, nil
}

func BenchmarkCompactTimestampBaselineEncode(b *testing.B) {
	values := benchmarkCompactTimestampValues()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		encoded := encodeRawTimestampBenchmark(values)
		if len(encoded) == 0 {
			b.Fatal("empty encoding")
		}
	}
}

func BenchmarkCompactTimestampBaselineDecode(b *testing.B) {
	encoded := encodeRawTimestampBenchmark(benchmarkCompactTimestampValues())
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		values, err := decodeRawTimestampBenchmark(encoded)
		if err != nil {
			b.Fatal(err)
		}
		compactTimestampBenchmarkSink = values
	}
}

func BenchmarkCompactTimestampEncode(b *testing.B) {
	values := benchmarkCompactTimestampValues()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		encoded, err := EncodeCompactTimestamps(values)
		if err != nil {
			b.Fatal(err)
		}
		if len(encoded) == 0 {
			b.Fatal("empty encoding")
		}
	}
}

func BenchmarkCompactTimestampDecode(b *testing.B) {
	encoded, err := EncodeCompactTimestamps(benchmarkCompactTimestampValues())
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		values, err := DecodeCompactTimestamps(encoded)
		if err != nil {
			b.Fatal(err)
		}
		compactTimestampBenchmarkSink = values
	}
}

func BenchmarkCompactTimestampIrregularBaselineEncode(b *testing.B) {
	values := benchmarkCompactTimestampIrregularValues()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		encoded := encodeRawTimestampBenchmark(values)
		if len(encoded) == 0 {
			b.Fatal("empty encoding")
		}
	}
}

func BenchmarkCompactTimestampIrregularEncode(b *testing.B) {
	values := benchmarkCompactTimestampIrregularValues()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		encoded, err := EncodeCompactTimestamps(values)
		if err != nil {
			b.Fatal(err)
		}
		if len(encoded) == 0 {
			b.Fatal("empty encoding")
		}
	}
}
