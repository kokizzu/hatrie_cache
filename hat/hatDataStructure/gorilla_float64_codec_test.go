package hatDataStructure

import (
	"errors"
	"math"
	"testing"
)

func TestC247GorillaFloat64CodecUsesGorillaForSmoothValues(t *testing.T) {
	values := []float64{1000, 1000.001, 1000.002, 1000.003, 1000.004}

	payload, mode, err := EncodeFloat64(values)
	if err != nil {
		t.Fatalf("EncodeFloat64() error = %v", err)
	}
	if mode != Float64EncodingGorilla {
		t.Fatalf("EncodeFloat64() mode = %v, want gorilla", mode)
	}
	decoded, decodedMode, err := DecodeFloat64(payload)
	if err != nil {
		t.Fatalf("DecodeFloat64() error = %v", err)
	}
	assertC247Float64BitsEqual(t, decoded, values)
	if decodedMode != mode {
		t.Fatalf("DecodeFloat64() mode = %v, want %v", decodedMode, mode)
	}
	if len(payload) >= float64RawEncodedSize(len(values)) {
		t.Fatalf("Gorilla payload size = %d, want less than raw size %d", len(payload), float64RawEncodedSize(len(values)))
	}
}

func TestC247GorillaFloat64CodecFallsBackForHighEntropyValues(t *testing.T) {
	values := []float64{
		math.Float64frombits(0x0123456789abcdef),
		math.Float64frombits(0xfedcba9876543210),
		math.Float64frombits(0x13579bdf2468ace0),
		math.Float64frombits(0x0f0e0d0c0b0a0908),
	}

	payload, mode, err := EncodeFloat64(values)
	if err != nil {
		t.Fatalf("EncodeFloat64() error = %v", err)
	}
	if mode != Float64EncodingRaw {
		t.Fatalf("EncodeFloat64() mode = %v, want raw", mode)
	}
	decoded, decodedMode, err := DecodeFloat64(payload)
	if err != nil {
		t.Fatalf("DecodeFloat64() error = %v", err)
	}
	assertC247Float64BitsEqual(t, decoded, values)
	if decodedMode != mode {
		t.Fatalf("DecodeFloat64() mode = %v, want %v", decodedMode, mode)
	}
}

func TestC247GorillaFloat64CodecPreservesSpecialBitPatterns(t *testing.T) {
	values := []float64{
		math.Copysign(0, -1),
		math.Inf(1),
		math.Inf(-1),
		math.Float64frombits(0x7ff8000000000001),
		math.Float64frombits(0x7ff0000000000001),
	}

	payload, err := EncodeFloat64Gorilla(values)
	if err != nil {
		t.Fatalf("EncodeFloat64Gorilla() error = %v", err)
	}
	decoded, mode, err := DecodeFloat64(payload)
	if err != nil {
		t.Fatalf("DecodeFloat64() error = %v", err)
	}
	assertC247Float64BitsEqual(t, decoded, values)
	if mode != Float64EncodingGorilla {
		t.Fatalf("DecodeFloat64() mode = %v, want gorilla", mode)
	}
}

func TestC247GorillaFloat64CodecRejectsMalformedPayloads(t *testing.T) {
	valid, err := EncodeFloat64Gorilla([]float64{1, 2, 3})
	if err != nil {
		t.Fatalf("EncodeFloat64Gorilla() error = %v", err)
	}
	tests := map[string][]byte{
		"short payload": valid[:len(valid)-1],
		"bad magic":     append([]byte("BAD!"), valid[4:]...),
		"trailing byte": append(append([]byte(nil), valid...), 0),
	}
	for name, payload := range tests {
		t.Run(name, func(t *testing.T) {
			if _, _, err := DecodeFloat64(payload); err == nil {
				t.Fatal("DecodeFloat64() error = nil, want malformed payload error")
			}
		})
	}
}

func TestC247GorillaFloat64CodecHandlesEmptyInput(t *testing.T) {
	payload, mode, err := EncodeFloat64(nil)
	if err != nil {
		t.Fatalf("EncodeFloat64(nil) error = %v", err)
	}
	decoded, decodedMode, err := DecodeFloat64(payload)
	if err != nil {
		t.Fatalf("DecodeFloat64(empty) error = %v", err)
	}
	if decoded != nil || mode != Float64EncodingRaw || decodedMode != mode {
		t.Fatalf("empty round trip = %#v/%v/%v, want nil/raw/raw", decoded, mode, decodedMode)
	}
}

func TestC247GorillaFloat64CodecRejectsTooManyValues(t *testing.T) {
	values := make([]float64, float64CodecMaxValues+1)
	if _, err := EncodeFloat64Gorilla(values); !errors.Is(err, ErrFloat64CodecTooManyValues) {
		t.Fatalf("EncodeFloat64Gorilla() error = %v, want ErrFloat64CodecTooManyValues", err)
	}
}

func assertC247Float64BitsEqual(t *testing.T, got, want []float64) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("decoded length = %d, want %d", len(got), len(want))
	}
	for index := range want {
		if math.Float64bits(got[index]) != math.Float64bits(want[index]) {
			t.Fatalf("decoded[%d] bits = %#x, want %#x", index, math.Float64bits(got[index]), math.Float64bits(want[index]))
		}
	}
}
