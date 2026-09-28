package hatCodec

import (
	"encoding/binary"
	"math"
	"testing"
)

var float64XORBenchmarkSink []byte
var float64XORBenchmarkValuesSink []float64

func BenchmarkFloat64XORBaselineEncodeRepeated(b *testing.B) {
	benchmarkFloat64XOREncode(b, benchmarkFloat64XORRepeatedValues(), encodeFloat64XORRawBaseline)
}

func BenchmarkFloat64XOREncodeRepeated(b *testing.B) {
	benchmarkFloat64XOREncode(b, benchmarkFloat64XORRepeatedValues(), EncodeFloat64XOR)
}

func BenchmarkFloat64XORBaselineDecodeRepeated(b *testing.B) {
	benchmarkFloat64XORDecode(b, benchmarkFloat64XORRepeatedValues(), encodeFloat64XORRawBaseline, decodeFloat64XORRawBaseline)
}

func BenchmarkFloat64XORDecodeRepeated(b *testing.B) {
	benchmarkFloat64XORDecode(b, benchmarkFloat64XORRepeatedValues(), EncodeFloat64XOR, DecodeFloat64XOR)
}

func BenchmarkFloat64XORBaselineEncodeSmooth(b *testing.B) {
	benchmarkFloat64XOREncode(b, benchmarkFloat64XORSmoothValues(), encodeFloat64XORRawBaseline)
}

func BenchmarkFloat64XOREncodeSmooth(b *testing.B) {
	benchmarkFloat64XOREncode(b, benchmarkFloat64XORSmoothValues(), EncodeFloat64XOR)
}

func BenchmarkFloat64XORBaselineDecodeSmooth(b *testing.B) {
	benchmarkFloat64XORDecode(b, benchmarkFloat64XORSmoothValues(), encodeFloat64XORRawBaseline, decodeFloat64XORRawBaseline)
}

func BenchmarkFloat64XORDecodeSmooth(b *testing.B) {
	benchmarkFloat64XORDecode(b, benchmarkFloat64XORSmoothValues(), EncodeFloat64XOR, DecodeFloat64XOR)
}

func BenchmarkFloat64XORBaselineEncodeRandom(b *testing.B) {
	benchmarkFloat64XOREncode(b, benchmarkFloat64XORRandomValues(), encodeFloat64XORRawBaseline)
}

func BenchmarkFloat64XOREncodeRandom(b *testing.B) {
	benchmarkFloat64XOREncode(b, benchmarkFloat64XORRandomValues(), EncodeFloat64XOR)
}

func BenchmarkFloat64XORBaselineDecodeRandom(b *testing.B) {
	benchmarkFloat64XORDecode(b, benchmarkFloat64XORRandomValues(), encodeFloat64XORRawBaseline, decodeFloat64XORRawBaseline)
}

func BenchmarkFloat64XORDecodeRandom(b *testing.B) {
	benchmarkFloat64XORDecode(b, benchmarkFloat64XORRandomValues(), EncodeFloat64XOR, DecodeFloat64XOR)
}

func benchmarkFloat64XOREncode(b *testing.B, values []float64, encode func([]float64) []byte) {
	b.ReportAllocs()
	b.SetBytes(int64(len(values) * 8))
	for i := 0; i < b.N; i++ {
		float64XORBenchmarkSink = encode(values)
	}
}

func benchmarkFloat64XORDecode(b *testing.B, values []float64, encode func([]float64) []byte, decode func([]byte, []float64) ([]float64, error)) {
	frame := encode(values)
	destination := make([]float64, 0, len(values))
	b.ReportAllocs()
	b.SetBytes(int64(len(frame)))
	for i := 0; i < b.N; i++ {
		var err error
		destination, err = decode(frame, destination)
		if err != nil {
			b.Fatal(err)
		}
		float64XORBenchmarkValuesSink = destination
	}
}

func benchmarkFloat64XORRepeatedValues() []float64 {
	values := make([]float64, 4096)
	for i := range values {
		values[i] = 42.25
	}
	return values
}

func benchmarkFloat64XORSmoothValues() []float64 {
	values := make([]float64, 4096)
	for i := range values {
		values[i] = float64(i) / 10
	}
	return values
}

func benchmarkFloat64XORRandomValues() []float64 {
	values := make([]float64, 4096)
	for i := range values {
		values[i] = math.Float64frombits(uint64(i)*0x9e3779b97f4a7c15 + 0x3ff0000000000000)
	}
	return values
}

func encodeFloat64XORRawBaseline(values []float64) []byte {
	frame := make([]byte, float64XORHeader+binary.MaxVarintLen64+len(values)*8)
	copy(frame, []byte("HGF1"))
	frame[4] = float64XORVersion
	frame[5] = float64XORModeRaw
	offset := float64XORHeader + binary.PutUvarint(frame[float64XORHeader:], uint64(len(values)))
	for _, value := range values {
		binary.LittleEndian.PutUint64(frame[offset:], math.Float64bits(value))
		offset += 8
	}
	return frame[:offset]
}

func decodeFloat64XORRawBaseline(frame []byte, dst []float64) ([]float64, error) {
	countValue, countBytes := binary.Uvarint(frame[float64XORHeader:])
	if countBytes <= 0 || countValue > uint64(float64XORMaxInt) {
		return nil, errFloat64XORFrame
	}
	count := int(countValue)
	offset := float64XORHeader + countBytes
	if len(frame)-offset != count*8 {
		return nil, errFloat64XORFrame
	}
	if cap(dst) < count {
		dst = make([]float64, count)
	} else {
		dst = dst[:count]
	}
	for i := range dst {
		dst[i] = math.Float64frombits(binary.LittleEndian.Uint64(frame[offset+i*8:]))
	}
	return dst, nil
}
