package hatDataStructure

import (
	"encoding/binary"
	"testing"
)

func BenchmarkC247EncodeUint64Monotone1M(b *testing.B) {
	values := c247MonotoneUint64Values(1 << 20)
	b.ReportAllocs()
	b.SetBytes(int64(len(values) * 8))
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		payload, _, err := EncodeUint64(values)
		if err != nil {
			b.Fatal(err)
		}
		if len(payload) == 0 {
			b.Fatal("empty payload")
		}
	}
}

func BenchmarkC247EncodeUint64RawControl1M(b *testing.B) {
	values := c247RandomUint64Values(1 << 20)
	b.ReportAllocs()
	b.SetBytes(int64(len(values) * 8))
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		payload := make([]byte, uint64RawEncodedSize(len(values)))
		offset := writeUint64CodecHeader(payload, Uint64EncodingRaw, len(values))
		for _, value := range values {
			binary.LittleEndian.PutUint64(payload[offset:], value)
			offset += 8
		}
		if len(payload) == 0 {
			b.Fatal("empty payload")
		}
	}
}

func BenchmarkC247EncodeUint64RandomAdaptive1M(b *testing.B) {
	values := c247RandomUint64Values(1 << 20)
	b.ReportAllocs()
	b.SetBytes(int64(len(values) * 8))
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		payload, mode, err := EncodeUint64(values)
		if err != nil {
			b.Fatal(err)
		}
		if mode != Uint64EncodingRaw || len(payload) == 0 {
			b.Fatalf("mode/size = %v/%d, want raw/non-empty", mode, len(payload))
		}
	}
}

func BenchmarkC247DecodeUint64Delta1M(b *testing.B) {
	values := c247MonotoneUint64Values(1 << 20)
	payload, mode, err := EncodeUint64(values)
	if err != nil || mode != Uint64EncodingDelta {
		b.Fatalf("EncodeUint64() = mode %v error %v, want delta/nil", mode, err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		decoded, _, err := DecodeUint64(payload)
		if err != nil || len(decoded) != len(values) {
			b.Fatalf("DecodeUint64() = %d/%v, want %d/nil", len(decoded), err, len(values))
		}
	}
}

func BenchmarkC247DecodeUint64Raw1M(b *testing.B) {
	values := c247RandomUint64Values(1 << 20)
	payload, mode, err := EncodeUint64(values)
	if err != nil || mode != Uint64EncodingRaw {
		b.Fatalf("EncodeUint64() = mode %v error %v, want raw/nil", mode, err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		decoded, _, err := DecodeUint64(payload)
		if err != nil || len(decoded) != len(values) {
			b.Fatalf("DecodeUint64() = %d/%v, want %d/nil", len(decoded), err, len(values))
		}
	}
}

func c247MonotoneUint64Values(count int) []uint64 {
	values := make([]uint64, count)
	for index := range values {
		values[index] = uint64(index*3 + index/100)
	}
	return values
}

func c247RandomUint64Values(count int) []uint64 {
	values := make([]uint64, count)
	state := uint64(0x9e3779b97f4a7c15)
	for index := range values {
		state ^= state << 7
		state ^= state >> 9
		state ^= state << 8
		values[index] = state
	}
	return values
}
