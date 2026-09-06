package hatCodec_test

import (
	"errors"
	"math"
	"reflect"
	"testing"

	hatCodec "hatrie_cache/hat/hatCodec"
)

func TestBitPackedUint64RoundTripUsesValueWidth(t *testing.T) {
	values := []uint64{0, 1, 3, 3, 15, 16, 255, 1024}
	encoded, err := hatCodec.EncodeBitPackedUint64(values)
	if err != nil {
		t.Fatalf("EncodeBitPackedUint64() error = %v", err)
	}
	decoded, err := hatCodec.DecodeBitPackedUint64(encoded)
	if err != nil {
		t.Fatalf("DecodeBitPackedUint64() error = %v", err)
	}
	if !reflect.DeepEqual(decoded, values) {
		t.Fatalf("DecodeBitPackedUint64() = %v, want %v", decoded, values)
	}
	if len(encoded) >= len(values)*8 {
		t.Fatalf("encoded length = %d, want less than raw length %d", len(encoded), len(values)*8)
	}
}

func TestBitPackedUint64HandlesZeroAndFullWidth(t *testing.T) {
	for name, values := range map[string][]uint64{
		"zero":       {0, 0, 0, 0},
		"full width": {0, math.MaxUint64},
	} {
		t.Run(name, func(t *testing.T) {
			encoded, err := hatCodec.EncodeBitPackedUint64(values)
			if err != nil {
				t.Fatalf("EncodeBitPackedUint64() error = %v", err)
			}
			decoded, err := hatCodec.DecodeBitPackedUint64(encoded)
			if err != nil {
				t.Fatalf("DecodeBitPackedUint64() error = %v", err)
			}
			if !reflect.DeepEqual(decoded, values) {
				t.Fatalf("decoded = %v, want %v", decoded, values)
			}
		})
	}
}

func TestBitPackedUint64RejectsCorruptionAndTrailingBytes(t *testing.T) {
	encoded, err := hatCodec.EncodeBitPackedUint64([]uint64{1, 2, 3})
	if err != nil {
		t.Fatalf("EncodeBitPackedUint64() error = %v", err)
	}
	for name, corrupted := range map[string][]byte{
		"truncated": encoded[:len(encoded)-1],
		"trailing":  append(append([]byte(nil), encoded...), 0),
		"invalid width": func() []byte {
			copyOfEncoded := append([]byte(nil), encoded...)
			copyOfEncoded[1] = 65
			return copyOfEncoded
		}(),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := hatCodec.DecodeBitPackedUint64(corrupted); !errors.Is(err, hatCodec.ErrBitPackedNumericInvalid) {
				t.Fatalf("DecodeBitPackedUint64() error = %v, want ErrBitPackedNumericInvalid", err)
			}
		})
	}
}

func BenchmarkEncodeBitPackedUint64(b *testing.B) {
	values := make([]uint64, 1024)
	for index := range values {
		values[index] = uint64(index % 256)
	}
	b.ReportAllocs()
	for range b.N {
		if _, err := hatCodec.EncodeBitPackedUint64(values); err != nil {
			b.Fatal(err)
		}
	}
}
