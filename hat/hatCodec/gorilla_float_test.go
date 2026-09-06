package hatCodec_test

import (
	"errors"
	"math"
	"testing"

	hatCodec "hatrie_cache/hat/hatCodec"
)

func TestGorillaFloat64RoundTripPreservesBits(t *testing.T) {
	values := []float64{
		0,
		0,
		1.5,
		1.5,
		math.Copysign(0, -1),
		math.Inf(1),
		math.Float64frombits(0x7ff8000000000042),
	}
	encoded, err := hatCodec.EncodeGorillaFloat64(values)
	if err != nil {
		t.Fatalf("EncodeGorillaFloat64() error = %v", err)
	}
	decoded, err := hatCodec.DecodeGorillaFloat64(encoded)
	if err != nil {
		t.Fatalf("DecodeGorillaFloat64() error = %v", err)
	}
	if len(decoded) != len(values) {
		t.Fatalf("decoded length = %d, want %d", len(decoded), len(values))
	}
	for index := range values {
		if math.Float64bits(decoded[index]) != math.Float64bits(values[index]) {
			t.Fatalf("decoded[%d] bits = %#x, want %#x", index, math.Float64bits(decoded[index]), math.Float64bits(values[index]))
		}
	}
}

func TestGorillaFloat64RejectsTruncatedAndInvalidInput(t *testing.T) {
	encoded, err := hatCodec.EncodeGorillaFloat64([]float64{1, 2})
	if err != nil {
		t.Fatalf("EncodeGorillaFloat64() error = %v", err)
	}
	if _, err := hatCodec.DecodeGorillaFloat64(encoded[:len(encoded)-1]); !errors.Is(err, hatCodec.ErrGorillaFloatInvalid) {
		t.Fatalf("DecodeGorillaFloat64(truncated) error = %v, want invalid error", err)
	}
	invalid := append([]byte(nil), encoded...)
	invalid[0] = 3
	if _, err := hatCodec.DecodeGorillaFloat64(invalid); !errors.Is(err, hatCodec.ErrGorillaFloatInvalid) {
		t.Fatalf("DecodeGorillaFloat64(invalid count) error = %v, want invalid error", err)
	}
}

func TestGorillaFloat64EmptyRoundTrip(t *testing.T) {
	encoded, err := hatCodec.EncodeGorillaFloat64(nil)
	if err != nil {
		t.Fatalf("EncodeGorillaFloat64(empty) error = %v", err)
	}
	decoded, err := hatCodec.DecodeGorillaFloat64(encoded)
	if err != nil {
		t.Fatalf("DecodeGorillaFloat64(empty) error = %v", err)
	}
	if len(decoded) != 0 {
		t.Fatalf("decoded empty length = %d, want 0", len(decoded))
	}
}

func BenchmarkEncodeGorillaFloat64(b *testing.B) {
	values := make([]float64, 1024)
	for index := range values {
		values[index] = float64(index / 8)
	}
	b.ReportAllocs()
	for range b.N {
		if _, err := hatCodec.EncodeGorillaFloat64(values); err != nil {
			b.Fatal(err)
		}
	}
}
