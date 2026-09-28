package hatCodec

import (
	"encoding/binary"
	"errors"
	"strconv"
	"testing"
)

var (
	lowCardinalityDictionaryBenchmarkBytesSink  []byte
	lowCardinalityDictionaryBenchmarkValuesSink []string
)

var errLowCardinalityDictionaryBenchmarkInvalid = errors.New("invalid raw string block")

func benchmarkLowCardinalityDictionaryValues() []string {
	values := make([]string, 4096)
	for index := range values {
		values[index] = "region=" + string(rune('a'+index%16)) + ";status=active"
	}
	return values
}

func benchmarkLowCardinalityDictionaryUniqueValues() []string {
	values := make([]string, 4096)
	for index := range values {
		values[index] = "unique-key-" + strconv.Itoa(index)
	}
	return values
}

func encodeRawStringBlockBenchmark(values []string) []byte {
	size := binary.MaxVarintLen64
	for _, value := range values {
		size += binary.MaxVarintLen64 + len(value)
	}
	encoded := make([]byte, size)
	offset := binary.PutUvarint(encoded, uint64(len(values)))
	for _, value := range values {
		offset += binary.PutUvarint(encoded[offset:], uint64(len(value)))
		offset += copy(encoded[offset:], value)
	}
	return encoded[:offset]
}

func decodeRawStringBlockBenchmark(encoded []byte) ([]string, error) {
	count, offset := binary.Uvarint(encoded)
	if offset <= 0 {
		return nil, errLowCardinalityDictionaryBenchmarkInvalid
	}
	values := make([]string, int(count))
	for index := range values {
		length, width := binary.Uvarint(encoded[offset:])
		if width <= 0 {
			return nil, errLowCardinalityDictionaryBenchmarkInvalid
		}
		offset += width
		if length > uint64(len(encoded)-offset) {
			return nil, errLowCardinalityDictionaryBenchmarkInvalid
		}
		values[index] = string(encoded[offset : offset+int(length)])
		offset += int(length)
	}
	if offset != len(encoded) {
		return nil, errLowCardinalityDictionaryBenchmarkInvalid
	}
	return values, nil
}

func BenchmarkLowCardinalityDictionaryBaselineEncode(b *testing.B) {
	values := benchmarkLowCardinalityDictionaryValues()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		encoded := encodeRawStringBlockBenchmark(values)
		lowCardinalityDictionaryBenchmarkBytesSink = encoded[:0]
	}
}

func BenchmarkLowCardinalityDictionaryBaselineDecode(b *testing.B) {
	encoded := encodeRawStringBlockBenchmark(benchmarkLowCardinalityDictionaryValues())
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		values, err := decodeRawStringBlockBenchmark(encoded)
		if err != nil {
			b.Fatal(err)
		}
		lowCardinalityDictionaryBenchmarkValuesSink = values
	}
}

func BenchmarkLowCardinalityDictionaryEncode(b *testing.B) {
	values := benchmarkLowCardinalityDictionaryValues()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		encoded, err := EncodeLowCardinalityStrings(values)
		if err != nil {
			b.Fatal(err)
		}
		lowCardinalityDictionaryBenchmarkBytesSink = encoded[:0]
	}
}

func BenchmarkLowCardinalityDictionaryDecode(b *testing.B) {
	encoded, err := EncodeLowCardinalityStrings(benchmarkLowCardinalityDictionaryValues())
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		values, err := DecodeLowCardinalityStrings(encoded)
		if err != nil {
			b.Fatal(err)
		}
		lowCardinalityDictionaryBenchmarkValuesSink = values
	}
}

func BenchmarkLowCardinalityDictionaryUniqueBaselineEncode(b *testing.B) {
	values := benchmarkLowCardinalityDictionaryUniqueValues()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		encoded := encodeRawStringBlockBenchmark(values)
		lowCardinalityDictionaryBenchmarkBytesSink = encoded[:0]
	}
}

func BenchmarkLowCardinalityDictionaryUniqueEncode(b *testing.B) {
	values := benchmarkLowCardinalityDictionaryUniqueValues()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		encoded, err := EncodeLowCardinalityStrings(values)
		if err != nil {
			b.Fatal(err)
		}
		lowCardinalityDictionaryBenchmarkBytesSink = encoded[:0]
	}
}
