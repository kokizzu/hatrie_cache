package hatCodec

import (
	"encoding/binary"
	"testing"
)

var int64DeltaBenchmarkSink []byte
var int64DeltaBenchmarkValuesSink []int64

func BenchmarkInt64DeltaBaselineEncodeMonotonic(b *testing.B) {
	benchmarkInt64DeltaEncode(b, benchmarkInt64DeltaMonotonicValues(), encodeInt64DeltaRawBaseline)
}

func BenchmarkInt64DeltaEncodeMonotonic(b *testing.B) {
	benchmarkInt64DeltaEncode(b, benchmarkInt64DeltaMonotonicValues(), EncodeInt64Delta)
}

func BenchmarkInt64DeltaBaselineDecodeMonotonic(b *testing.B) {
	benchmarkInt64DeltaDecode(b, benchmarkInt64DeltaMonotonicValues(), encodeInt64DeltaRawBaseline, decodeInt64DeltaRawBaseline)
}

func BenchmarkInt64DeltaDecodeMonotonic(b *testing.B) {
	benchmarkInt64DeltaDecode(b, benchmarkInt64DeltaMonotonicValues(), EncodeInt64Delta, DecodeInt64Delta)
}

func BenchmarkInt64DeltaBaselineEncodeNoisy(b *testing.B) {
	benchmarkInt64DeltaEncode(b, benchmarkInt64DeltaNoisyValues(), encodeInt64DeltaRawBaseline)
}

func BenchmarkInt64DeltaEncodeNoisy(b *testing.B) {
	benchmarkInt64DeltaEncode(b, benchmarkInt64DeltaNoisyValues(), EncodeInt64Delta)
}

func BenchmarkInt64DeltaBaselineDecodeNoisy(b *testing.B) {
	benchmarkInt64DeltaDecode(b, benchmarkInt64DeltaNoisyValues(), encodeInt64DeltaRawBaseline, decodeInt64DeltaRawBaseline)
}

func BenchmarkInt64DeltaDecodeNoisy(b *testing.B) {
	benchmarkInt64DeltaDecode(b, benchmarkInt64DeltaNoisyValues(), EncodeInt64Delta, DecodeInt64Delta)
}

func BenchmarkInt64DeltaBaselineEncodeRandom(b *testing.B) {
	benchmarkInt64DeltaEncode(b, benchmarkInt64DeltaRandomValues(), encodeInt64DeltaRawBaseline)
}

func BenchmarkInt64DeltaEncodeRandom(b *testing.B) {
	benchmarkInt64DeltaEncode(b, benchmarkInt64DeltaRandomValues(), EncodeInt64Delta)
}

func BenchmarkInt64DeltaBaselineDecodeRandom(b *testing.B) {
	benchmarkInt64DeltaDecode(b, benchmarkInt64DeltaRandomValues(), encodeInt64DeltaRawBaseline, decodeInt64DeltaRawBaseline)
}

func BenchmarkInt64DeltaDecodeRandom(b *testing.B) {
	benchmarkInt64DeltaDecode(b, benchmarkInt64DeltaRandomValues(), EncodeInt64Delta, DecodeInt64Delta)
}

func benchmarkInt64DeltaEncode(b *testing.B, values []int64, encode func([]int64) []byte) {
	b.ReportAllocs()
	b.SetBytes(int64(len(values) * 8))
	for i := 0; i < b.N; i++ {
		int64DeltaBenchmarkSink = encode(values)
	}
}

func benchmarkInt64DeltaDecode(b *testing.B, values []int64, encode func([]int64) []byte, decode func([]byte, []int64) ([]int64, error)) {
	frame := encode(values)
	destination := make([]int64, 0, len(values))
	b.ReportAllocs()
	b.SetBytes(int64(len(frame)))
	for i := 0; i < b.N; i++ {
		var err error
		destination, err = decode(frame, destination)
		if err != nil {
			b.Fatal(err)
		}
		int64DeltaBenchmarkValuesSink = destination
	}
}

func benchmarkInt64DeltaMonotonicValues() []int64 {
	values := make([]int64, 4096)
	for i := range values {
		values[i] = int64(i) * 10
	}
	return values
}

func benchmarkInt64DeltaNoisyValues() []int64 {
	values := make([]int64, 4096)
	for i := range values {
		values[i] = int64(i)*10 + int64((i%7)-3)
	}
	return values
}

func benchmarkInt64DeltaRandomValues() []int64 {
	values := make([]int64, 4096)
	for i := range values {
		values[i] = int64(uint64(i)*0x9e3779b97f4a7c15 + 0x123456789abcdef0)
	}
	return values
}

func encodeInt64DeltaRawBaseline(values []int64) []byte {
	frame := make([]byte, int64DeltaHeader+binary.MaxVarintLen64+len(values)*8)
	copy(frame, []byte("HID1"))
	frame[4] = int64DeltaVersion
	frame[5] = int64DeltaModeRaw
	offset := int64DeltaHeader + binary.PutUvarint(frame[int64DeltaHeader:], uint64(len(values)))
	for _, value := range values {
		binary.LittleEndian.PutUint64(frame[offset:], uint64(value))
		offset += 8
	}
	return frame[:offset]
}

func decodeInt64DeltaRawBaseline(frame []byte, dst []int64) ([]int64, error) {
	countValue, countBytes := binary.Uvarint(frame[int64DeltaHeader:])
	if countBytes <= 0 || countValue > uint64(int64DeltaMaxInt) {
		return nil, errInt64DeltaFrame
	}
	count := int(countValue)
	offset := int64DeltaHeader + countBytes
	if len(frame)-offset != count*8 {
		return nil, errInt64DeltaFrame
	}
	if cap(dst) < count {
		dst = make([]int64, count)
	} else {
		dst = dst[:count]
	}
	for i := range dst {
		dst[i] = int64(binary.LittleEndian.Uint64(frame[offset+i*8:]))
	}
	return dst, nil
}
