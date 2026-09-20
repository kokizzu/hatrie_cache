package hatDataStructure

import (
	"math"
	"testing"
)

func BenchmarkC247GorillaEncodeSmooth1M(b *testing.B) {
	values := c247SmoothFloat64Values(1 << 20)
	b.ReportAllocs()
	b.SetBytes(int64(len(values) * 8))
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		payload, mode, err := EncodeFloat64(values)
		if err != nil || mode != Float64EncodingGorilla || len(payload) == 0 {
			b.Fatalf("EncodeFloat64() = %v/%d/%v, want gorilla/non-empty/nil", mode, len(payload), err)
		}
	}
}

func BenchmarkC247GorillaRawEncodeControl1M(b *testing.B) {
	values := c247SmoothFloat64Values(1 << 20)
	b.ReportAllocs()
	b.SetBytes(int64(len(values) * 8))
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		payload := encodeFloat64RawPayload(values, float64RawEncodedSize(len(values)))
		if len(payload) == 0 {
			b.Fatal("empty payload")
		}
	}
}

func BenchmarkC247GorillaEncodeRandomAdaptive1M(b *testing.B) {
	values := c247RandomFloat64Values(1 << 20)
	b.ReportAllocs()
	b.SetBytes(int64(len(values) * 8))
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		payload, mode, err := EncodeFloat64(values)
		if err != nil || mode != Float64EncodingRaw || len(payload) == 0 {
			b.Fatalf("EncodeFloat64() = %v/%d/%v, want raw/non-empty/nil", mode, len(payload), err)
		}
	}
}

func BenchmarkC247GorillaDecode1M(b *testing.B) {
	values := c247SmoothFloat64Values(1 << 20)
	payload, err := EncodeFloat64Gorilla(values)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		decoded, mode, err := DecodeFloat64(payload)
		if err != nil || mode != Float64EncodingGorilla || len(decoded) != len(values) {
			b.Fatalf("DecodeFloat64() = %v/%d/%v, want gorilla/%d/nil", mode, len(decoded), err, len(values))
		}
	}
}

func BenchmarkC247GorillaRawDecodeControl1M(b *testing.B) {
	values := c247SmoothFloat64Values(1 << 20)
	payload := encodeFloat64RawPayload(values, float64RawEncodedSize(len(values)))
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		decoded, mode, err := DecodeFloat64(payload)
		if err != nil || mode != Float64EncodingRaw || len(decoded) != len(values) {
			b.Fatalf("DecodeFloat64() = %v/%d/%v, want raw/%d/nil", mode, len(decoded), err, len(values))
		}
	}
}

func c247SmoothFloat64Values(count int) []float64 {
	values := make([]float64, count)
	for index := range values {
		values[index] = float64(index / 64)
	}
	return values
}

func c247RandomFloat64Values(count int) []float64 {
	values := make([]float64, count)
	state := uint64(0x9e3779b97f4a7c15)
	for index := range values {
		state ^= state << 7
		state ^= state >> 9
		state ^= state << 8
		values[index] = math.Float64frombits(state)
	}
	return values
}
