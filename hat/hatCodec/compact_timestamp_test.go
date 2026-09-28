package hatCodec

import (
	"errors"
	"math"
	"reflect"
	"testing"
)

func TestCompactTimestampSelectsDoubleDeltaForRegularValues(t *testing.T) {
	values := benchmarkCompactTimestampValues()
	if got := SelectCompactTimestampEncoding(values); got != CompactTimestampEncodingDoubleDelta {
		t.Fatalf("SelectCompactTimestampEncoding() = %v, want double-delta", got)
	}
	encoded, err := EncodeCompactTimestamps(values)
	if err != nil {
		t.Fatalf("EncodeCompactTimestamps() error = %v", err)
	}
	if got, want := len(encoded), 4113; got != want {
		t.Fatalf("double-delta encoded length = %d, want %d", got, want)
	}
	decoded, err := DecodeCompactTimestamps(encoded)
	if err != nil {
		t.Fatalf("DecodeCompactTimestamps() error = %v", err)
	}
	if !reflect.DeepEqual(decoded, values) {
		t.Fatalf("DecodeCompactTimestamps() = %v, want %v", decoded, values)
	}
	legacy := encodeRawTimestampBenchmark(values)
	if len(encoded) >= len(legacy) {
		t.Fatalf("double-delta length = %d, raw length = %d", len(encoded), len(legacy))
	}
}

func TestCompactTimestampKeepsRawForWideIrregularValues(t *testing.T) {
	values := []int64{math.MinInt64, -1, 0, 1, math.MaxInt64, 1 << 60, -(1 << 60), 123456789}
	if got := SelectCompactTimestampEncoding(values); got != CompactTimestampEncodingRaw {
		t.Fatalf("SelectCompactTimestampEncoding() = %v, want raw", got)
	}
	encoded, err := EncodeCompactTimestamps(values)
	if err != nil {
		t.Fatalf("EncodeCompactTimestamps() error = %v", err)
	}
	decoded, err := DecodeCompactTimestamps(encoded)
	if err != nil {
		t.Fatalf("DecodeCompactTimestamps() error = %v", err)
	}
	if !reflect.DeepEqual(decoded, values) {
		t.Fatalf("DecodeCompactTimestamps() = %v, want %v", decoded, values)
	}
}

func TestCompactTimestampRoundTripsOverflowingDifferences(t *testing.T) {
	values := []int64{math.MaxInt64, math.MinInt64, math.MaxInt64 - 1, math.MinInt64 + 1}
	encoded, err := EncodeCompactTimestamps(values)
	if err != nil {
		t.Fatalf("EncodeCompactTimestamps() error = %v", err)
	}
	decoded, err := DecodeCompactTimestamps(encoded)
	if err != nil {
		t.Fatalf("DecodeCompactTimestamps() error = %v", err)
	}
	if !reflect.DeepEqual(decoded, values) {
		t.Fatalf("DecodeCompactTimestamps() = %v, want %v", decoded, values)
	}
}

func TestCompactTimestampRejectsMalformedInput(t *testing.T) {
	encoded, err := EncodeCompactTimestamps(benchmarkCompactTimestampValues())
	if err != nil {
		t.Fatalf("EncodeCompactTimestamps() error = %v", err)
	}
	for _, malformed := range [][]byte{
		encoded[:len(encoded)-1],
		append(append([]byte(nil), encoded...), 0),
		[]byte("bad"),
	} {
		if _, err := DecodeCompactTimestamps(malformed); !errors.Is(err, ErrCompactTimestampInvalid) {
			t.Fatalf("DecodeCompactTimestamps() error = %v, want ErrCompactTimestampInvalid", err)
		}
	}
}
