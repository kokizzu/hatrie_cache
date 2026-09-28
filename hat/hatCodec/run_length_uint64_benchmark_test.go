package hatCodec

import (
	"encoding/binary"
	"testing"
)

var runLengthUint64BenchmarkSink []byte
var runLengthUint64BenchmarkValuesSink []uint64

func BenchmarkRunLengthUint64BaselineEncodeRepeated(b *testing.B) {
	values := benchmarkRunLengthUint64RepeatedValues()
	b.ReportAllocs()
	b.SetBytes(int64(len(values) * 8))
	for i := 0; i < b.N; i++ {
		runLengthUint64BenchmarkSink = encodeRunLengthUint64Baseline(values)
	}
}

func BenchmarkRunLengthUint64EncodeRepeated(b *testing.B) {
	values := benchmarkRunLengthUint64RepeatedValues()
	b.ReportAllocs()
	b.SetBytes(int64(len(values) * 8))
	for i := 0; i < b.N; i++ {
		runLengthUint64BenchmarkSink = EncodeRunLengthUint64(values)
	}
}

func BenchmarkRunLengthUint64BaselineDecodeRepeated(b *testing.B) {
	values := benchmarkRunLengthUint64RepeatedValues()
	frame := encodeRunLengthUint64Baseline(values)
	destination := make([]uint64, 0, len(values))
	b.ReportAllocs()
	b.SetBytes(int64(len(frame)))
	for i := 0; i < b.N; i++ {
		var err error
		destination, err = decodeRunLengthUint64Baseline(frame, destination)
		if err != nil {
			b.Fatal(err)
		}
		runLengthUint64BenchmarkValuesSink = destination
	}
}

func BenchmarkRunLengthUint64DecodeRepeated(b *testing.B) {
	values := benchmarkRunLengthUint64RepeatedValues()
	frame := EncodeRunLengthUint64(values)
	destination := make([]uint64, 0, len(values))
	b.ReportAllocs()
	b.SetBytes(int64(len(frame)))
	for i := 0; i < b.N; i++ {
		var err error
		destination, err = DecodeRunLengthUint64(frame, destination)
		if err != nil {
			b.Fatal(err)
		}
		runLengthUint64BenchmarkValuesSink = destination
	}
}

func BenchmarkRunLengthUint64BaselineEncodeUnique(b *testing.B) {
	values := benchmarkRunLengthUint64UniqueValues()
	b.ReportAllocs()
	b.SetBytes(int64(len(values) * 8))
	for i := 0; i < b.N; i++ {
		runLengthUint64BenchmarkSink = encodeRunLengthUint64Baseline(values)
	}
}

func BenchmarkRunLengthUint64EncodeUnique(b *testing.B) {
	values := benchmarkRunLengthUint64UniqueValues()
	b.ReportAllocs()
	b.SetBytes(int64(len(values) * 8))
	for i := 0; i < b.N; i++ {
		runLengthUint64BenchmarkSink = EncodeRunLengthUint64(values)
	}
}

func BenchmarkRunLengthUint64BaselineDecodeUnique(b *testing.B) {
	values := benchmarkRunLengthUint64UniqueValues()
	frame := encodeRunLengthUint64Baseline(values)
	destination := make([]uint64, 0, len(values))
	b.ReportAllocs()
	b.SetBytes(int64(len(frame)))
	for i := 0; i < b.N; i++ {
		var err error
		destination, err = decodeRunLengthUint64Baseline(frame, destination)
		if err != nil {
			b.Fatal(err)
		}
		runLengthUint64BenchmarkValuesSink = destination
	}
}

func BenchmarkRunLengthUint64DecodeUnique(b *testing.B) {
	values := benchmarkRunLengthUint64UniqueValues()
	frame := EncodeRunLengthUint64(values)
	destination := make([]uint64, 0, len(values))
	b.ReportAllocs()
	b.SetBytes(int64(len(frame)))
	for i := 0; i < b.N; i++ {
		var err error
		destination, err = DecodeRunLengthUint64(frame, destination)
		if err != nil {
			b.Fatal(err)
		}
		runLengthUint64BenchmarkValuesSink = destination
	}
}

func benchmarkRunLengthUint64RepeatedValues() []uint64 {
	values := make([]uint64, 4096)
	for i := range values {
		values[i] = uint64(i / 32)
	}
	return values
}

func benchmarkRunLengthUint64UniqueValues() []uint64 {
	values := make([]uint64, 4096)
	for i := range values {
		values[i] = uint64(i)*0x9e3779b97f4a7c15 + 0x123456789abcdef0
	}
	return values
}

func encodeRunLengthUint64Baseline(values []uint64) []byte {
	frame := make([]byte, runLengthUint64Header+binary.MaxVarintLen64+len(values)*8)
	copy(frame, []byte("HCR1"))
	frame[4] = runLengthUint64Version
	frame[5] = runLengthUint64RawMode
	offset := runLengthUint64Header + binary.PutUvarint(frame[runLengthUint64Header:], uint64(len(values)))
	for _, value := range values {
		binary.LittleEndian.PutUint64(frame[offset:], value)
		offset += 8
	}
	return frame[:offset]
}

func decodeRunLengthUint64Baseline(frame []byte, dst []uint64) ([]uint64, error) {
	countValue, countBytes := binary.Uvarint(frame[runLengthUint64Header:])
	if countBytes <= 0 {
		return nil, errRunLengthUint64Frame
	}
	count := int(countValue)
	offset := runLengthUint64Header + countBytes
	if cap(dst) < count {
		dst = make([]uint64, count)
	} else {
		dst = dst[:count]
	}
	for i := range dst {
		dst[i] = binary.LittleEndian.Uint64(frame[offset+i*8:])
	}
	return dst, nil
}
