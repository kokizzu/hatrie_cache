package hatCodec

import "testing"

var (
	defaultValueSuppressionBenchmarkBytesSink  []byte
	defaultValueSuppressionBenchmarkValuesSink []uint64
)

func benchmarkDefaultValueSuppressionValues() []uint64 {
	values := make([]uint64, 4096)
	for index := 0; index < len(values); index += 32 {
		values[index] = uint64(1<<40) + uint64(index*17)
	}
	return values
}

func BenchmarkDefaultValueSuppressionBaselineEncode(b *testing.B) {
	values := benchmarkDefaultValueSuppressionValues()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		encoded, err := EncodeBitPackedUint64(values)
		if err != nil {
			b.Fatal(err)
		}
		defaultValueSuppressionBenchmarkBytesSink = encoded[:0]
	}
}

func BenchmarkDefaultValueSuppressionBaselineDecode(b *testing.B) {
	encoded, err := EncodeBitPackedUint64(benchmarkDefaultValueSuppressionValues())
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		decoded, err := DecodeBitPackedUint64(encoded)
		if err != nil {
			b.Fatal(err)
		}
		defaultValueSuppressionBenchmarkValuesSink = decoded
	}
}

func BenchmarkDefaultValueSuppressionCompactEncode(b *testing.B) {
	values := benchmarkDefaultValueSuppressionValues()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		encoded, err := EncodeCompactUint64(values)
		if err != nil {
			b.Fatal(err)
		}
		defaultValueSuppressionBenchmarkBytesSink = encoded[:0]
	}
}

func BenchmarkDefaultValueSuppressionCompactDecode(b *testing.B) {
	encoded, err := EncodeCompactUint64(benchmarkDefaultValueSuppressionValues())
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		decoded, err := DecodeCompactUint64(encoded)
		if err != nil {
			b.Fatal(err)
		}
		defaultValueSuppressionBenchmarkValuesSink = decoded
	}
}

func BenchmarkDefaultValueSuppressionDenseBaselineEncode(b *testing.B) {
	values := make([]uint64, 4096)
	for index := range values {
		values[index] = uint64(index%255) + 1
	}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		encoded, err := EncodeBitPackedUint64(values)
		if err != nil {
			b.Fatal(err)
		}
		defaultValueSuppressionBenchmarkBytesSink = encoded[:0]
	}
}

func BenchmarkDefaultValueSuppressionDenseCompactEncode(b *testing.B) {
	values := make([]uint64, 4096)
	for index := range values {
		values[index] = uint64(index%255) + 1
	}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		encoded, err := EncodeCompactUint64(values)
		if err != nil {
			b.Fatal(err)
		}
		defaultValueSuppressionBenchmarkBytesSink = encoded[:0]
	}
}
