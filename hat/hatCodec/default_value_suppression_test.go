package hatCodec

import (
	"bytes"
	"encoding/binary"
	"errors"
	"reflect"
	"testing"
)

func TestDefaultValueSuppressionCompactSelectsSparseEncoding(t *testing.T) {
	values := benchmarkDefaultValueSuppressionValues()
	if got := SelectCompactUint64Encoding(values); got != CompactUint64EncodingDefaultSuppressed {
		t.Fatalf("SelectCompactUint64Encoding() = %v, want default suppression", got)
	}
	encoded, err := EncodeCompactUint64(values)
	if err != nil {
		t.Fatalf("EncodeCompactUint64() error = %v", err)
	}
	if got, want := len(encoded), 1287; got != want {
		t.Fatalf("compact encoded length = %d, want %d", got, want)
	}
	decoded, err := DecodeCompactUint64(encoded)
	if err != nil {
		t.Fatalf("DecodeCompactUint64() error = %v", err)
	}
	if !reflect.DeepEqual(decoded, values) {
		t.Fatalf("DecodeCompactUint64() = %v, want %v", decoded, values)
	}
	legacy, err := EncodeBitPackedUint64(values)
	if err != nil {
		t.Fatalf("EncodeBitPackedUint64() error = %v", err)
	}
	if len(encoded) >= len(legacy) {
		t.Fatalf("compact length = %d, legacy length = %d", len(encoded), len(legacy))
	}
}

func TestDefaultValueSuppressionCompactKeepsDenseFallback(t *testing.T) {
	values := make([]uint64, 64)
	for index := range values {
		values[index] = uint64(index + 1)
	}
	if got := SelectCompactUint64Encoding(values); got != CompactUint64EncodingBitPacked {
		t.Fatalf("SelectCompactUint64Encoding() = %v, want bit-packed", got)
	}
	encoded, err := EncodeCompactUint64(values)
	if err != nil {
		t.Fatalf("EncodeCompactUint64() error = %v", err)
	}
	legacy, err := EncodeBitPackedUint64(values)
	if err != nil {
		t.Fatalf("EncodeBitPackedUint64() error = %v", err)
	}
	if !bytes.Equal(encoded[len(compactUint64Magic)+1:], legacy) {
		t.Fatal("compact bit-packed payload differs from the existing format")
	}
}

func TestDefaultValueSuppressionCompactPreservesBitPackedWidths(t *testing.T) {
	tests := []struct {
		name   string
		values []uint64
		want   CompactUint64Encoding
	}{
		{name: "one-bit", values: []uint64{1, 1, 1, 1, 1, 1, 1, 1}, want: CompactUint64EncodingBitPacked},
		{name: "eight-bit", values: []uint64{1, 3, 15, 31, 63, 127, 191, 255}, want: CompactUint64EncodingBitPacked},
		{name: "sixteen-bit", values: []uint64{1, 255, 1023, 4095, 16383, 32767, 49151, 65535}, want: CompactUint64EncodingBitPacked},
		{name: "thirty-two-bit", values: []uint64{1, 65535, 1<<20 - 1, 1<<24 - 1, 1<<28 - 1, 1<<30 - 1, 1<<31 - 1, 1<<32 - 1}, want: CompactUint64EncodingBitPacked},
		{name: "sixty-four-bit", values: []uint64{1, 1 << 32, 1 << 40, 1 << 48, 1 << 56, 1 << 60, 1<<63 - 1, ^uint64(0)}, want: CompactUint64EncodingRaw},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := SelectCompactUint64Encoding(test.values); got != test.want {
				t.Fatalf("SelectCompactUint64Encoding() = %v, want %v", got, test.want)
			}
			encoded, err := EncodeCompactUint64(test.values)
			if err != nil {
				t.Fatalf("EncodeCompactUint64() error = %v", err)
			}
			legacy, err := EncodeBitPackedUint64(test.values)
			if err != nil {
				t.Fatalf("EncodeBitPackedUint64() error = %v", err)
			}
			if test.want == CompactUint64EncodingBitPacked && !bytes.Equal(encoded[len(compactUint64Magic)+1:], legacy) {
				t.Fatal("compact bit-packed payload differs from the existing format")
			}
		})
	}
}

func TestDefaultValueSuppressionCompactRejectsMalformedPayload(t *testing.T) {
	encoded, err := EncodeCompactUint64(benchmarkDefaultValueSuppressionValues())
	if err != nil {
		t.Fatalf("EncodeCompactUint64() error = %v", err)
	}
	for _, malformed := range [][]byte{
		encoded[:len(encoded)-1],
		append(append([]byte(nil), encoded...), 0),
		[]byte("bad"),
	} {
		if _, err := DecodeCompactUint64(malformed); !errors.Is(err, ErrCompactUint64Invalid) {
			t.Fatalf("DecodeCompactUint64() error = %v, want ErrCompactUint64Invalid", err)
		}
	}

	sparse := make([]uint64, 4097)
	sparse[0] = 1 << 40
	sparseEncoded, err := EncodeCompactUint64(sparse)
	if err != nil {
		t.Fatalf("EncodeCompactUint64(sparse) error = %v", err)
	}
	bitmapOffset := compactUint64Header + binary.PutUvarint(make([]byte, binary.MaxVarintLen64), uint64(len(sparse)))
	bitmapBytes := (len(sparse) + 7) / 8
	padding := append([]byte(nil), sparseEncoded...)
	padding[bitmapOffset+bitmapBytes-1] |= 0x80
	if _, err := DecodeCompactUint64(padding); !errors.Is(err, ErrCompactUint64Invalid) {
		t.Fatalf("DecodeCompactUint64(padding) error = %v, want ErrCompactUint64Invalid", err)
	}
	zeroValue := append([]byte(nil), sparseEncoded...)
	zeroValue[bitmapOffset+bitmapBytes] = 0
	if _, err := DecodeCompactUint64(zeroValue); !errors.Is(err, ErrCompactUint64Invalid) {
		t.Fatalf("DecodeCompactUint64(zero value) error = %v, want ErrCompactUint64Invalid", err)
	}
}
