package hatCodec

import (
	"errors"
	"reflect"
	"testing"
)

func TestLowCardinalityDictionarySelectsRepeatedValues(t *testing.T) {
	values := benchmarkLowCardinalityDictionaryValues()
	if got := SelectLowCardinalityStringEncoding(values); got != LowCardinalityStringEncodingDictionary {
		t.Fatalf("SelectLowCardinalityStringEncoding() = %v, want dictionary", got)
	}
	encoded, err := EncodeLowCardinalityStrings(values)
	if err != nil {
		t.Fatalf("EncodeLowCardinalityStrings() error = %v", err)
	}
	if got, want := len(encoded), 4472; got != want {
		t.Fatalf("dictionary encoded length = %d, want %d", got, want)
	}
	decoded, err := DecodeLowCardinalityStrings(encoded)
	if err != nil {
		t.Fatalf("DecodeLowCardinalityStrings() error = %v", err)
	}
	if !reflect.DeepEqual(decoded, values) {
		t.Fatalf("DecodeLowCardinalityStrings() = %v, want %v", decoded, values)
	}
	legacy := encodeRawStringBlockBenchmark(values)
	if len(encoded) >= len(legacy) {
		t.Fatalf("dictionary length = %d, raw length = %d", len(encoded), len(legacy))
	}
}

func TestLowCardinalityDictionaryKeepsRawFallback(t *testing.T) {
	values := make([]string, 16)
	for index := range values {
		values[index] = string(make([]byte, 64)) + string(rune(index))
	}
	if got := SelectLowCardinalityStringEncoding(values); got != LowCardinalityStringEncodingRaw {
		t.Fatalf("SelectLowCardinalityStringEncoding() = %v, want raw", got)
	}
	encoded, err := EncodeLowCardinalityStrings(values)
	if err != nil {
		t.Fatalf("EncodeLowCardinalityStrings() error = %v", err)
	}
	if got, want := len(encoded), len(encodeRawStringBlockBenchmark(values))+len(lowCardinalityStringMagic)+1; got != want {
		t.Fatalf("raw fallback encoded length = %d, want %d", got, want)
	}
	decoded, err := DecodeLowCardinalityStrings(encoded)
	if err != nil {
		t.Fatalf("DecodeLowCardinalityStrings() error = %v", err)
	}
	if !reflect.DeepEqual(decoded, values) {
		t.Fatalf("DecodeLowCardinalityStrings() = %v, want %v", decoded, values)
	}
}

func TestLowCardinalityDictionaryRejectsMalformedInput(t *testing.T) {
	encoded, err := EncodeLowCardinalityStrings(benchmarkLowCardinalityDictionaryValues())
	if err != nil {
		t.Fatalf("EncodeLowCardinalityStrings() error = %v", err)
	}
	for _, malformed := range [][]byte{
		encoded[:len(encoded)-1],
		append(append([]byte(nil), encoded...), 0),
		[]byte("bad"),
	} {
		if _, err := DecodeLowCardinalityStrings(malformed); !errors.Is(err, ErrLowCardinalityStringInvalid) {
			t.Fatalf("DecodeLowCardinalityStrings() error = %v, want ErrLowCardinalityStringInvalid", err)
		}
	}
}
