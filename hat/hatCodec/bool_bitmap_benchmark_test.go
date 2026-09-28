package hatCodec

import (
	"encoding/binary"
	"testing"
)

var boolBitmapBenchmarkSink []byte
var boolBitmapBenchmarkValuesSink []bool

func BenchmarkBoolBitmapBaselineEncode(b *testing.B) {
	values := benchmarkBoolBitmapValues()
	b.ReportAllocs()
	b.SetBytes(int64(len(values)))
	for i := 0; i < b.N; i++ {
		boolBitmapBenchmarkSink = encodeBoolBitmapBaseline(values)
	}
}

func BenchmarkBoolBitmapEncode(b *testing.B) {
	values := benchmarkBoolBitmapValues()
	b.ReportAllocs()
	b.SetBytes(int64(len(values)))
	for i := 0; i < b.N; i++ {
		boolBitmapBenchmarkSink = EncodeBoolBitmap(values)
	}
}

func BenchmarkBoolBitmapBaselineDecode(b *testing.B) {
	values := benchmarkBoolBitmapValues()
	frame := encodeBoolBitmapBaseline(values)
	destination := make([]bool, 0, len(values))
	b.ReportAllocs()
	b.SetBytes(int64(len(frame)))
	for i := 0; i < b.N; i++ {
		var err error
		destination, err = decodeBoolBitmapBaseline(frame, destination)
		if err != nil {
			b.Fatal(err)
		}
		boolBitmapBenchmarkValuesSink = destination
	}
}

func BenchmarkBoolBitmapDecode(b *testing.B) {
	values := benchmarkBoolBitmapValues()
	frame := EncodeBoolBitmap(values)
	destination := make([]bool, 0, len(values))
	b.ReportAllocs()
	b.SetBytes(int64(len(frame)))
	for i := 0; i < b.N; i++ {
		var err error
		destination, err = DecodeBoolBitmap(frame, destination)
		if err != nil {
			b.Fatal(err)
		}
		boolBitmapBenchmarkValuesSink = destination
	}
}

func benchmarkBoolBitmapValues() []bool {
	values := make([]bool, 4096)
	for i := range values {
		values[i] = i%5 == 0
	}
	return values
}

func encodeBoolBitmapBaseline(values []bool) []byte {
	frame := make([]byte, boolBitmapHeader+binary.MaxVarintLen64+len(values))
	copy(frame, []byte("HCB1"))
	frame[4] = boolBitmapVersion
	offset := boolBitmapHeader + binary.PutUvarint(frame[boolBitmapHeader:], uint64(len(values)))
	for _, value := range values {
		if value {
			frame[offset] = 1
		}
		offset++
	}
	return frame[:offset]
}

func decodeBoolBitmapBaseline(frame []byte, dst []bool) ([]bool, error) {
	countValue, countBytes := binary.Uvarint(frame[boolBitmapHeader:])
	if countBytes <= 0 || countValue > uint64(boolBitmapMaxInt) {
		return nil, errBoolBitmapFrame
	}
	count := int(countValue)
	payloadOffset := boolBitmapHeader + countBytes
	if len(frame)-payloadOffset != count {
		return nil, errBoolBitmapFrame
	}
	if cap(dst) < count {
		dst = make([]bool, count)
	} else {
		dst = dst[:count]
	}
	for i := range dst {
		dst[i] = frame[payloadOffset+i] != 0
	}
	return dst, nil
}
