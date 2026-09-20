package hatDataStructure

import (
	"errors"
	"reflect"
	"testing"
)

func TestC247AdaptiveUint64CodecUsesDeltaForMonotoneValues(t *testing.T) {
	values := []uint64{10, 11, 12, 20, 21, 1000}

	encoded, mode, err := EncodeUint64(values)
	if err != nil {
		t.Fatalf("EncodeUint64() error = %v", err)
	}
	if mode != Uint64EncodingDelta {
		t.Fatalf("EncodeUint64() mode = %v, want delta", mode)
	}
	decoded, decodedMode, err := DecodeUint64(encoded)
	if err != nil {
		t.Fatalf("DecodeUint64() error = %v", err)
	}
	if decodedMode != mode || !reflect.DeepEqual(decoded, values) {
		t.Fatalf("DecodeUint64() = %#v/%v, want %#v/%v", decoded, decodedMode, values, mode)
	}
	if len(encoded) >= uint64RawEncodedSize(len(values)) {
		t.Fatalf("delta encoding size = %d, want less than raw size %d", len(encoded), uint64RawEncodedSize(len(values)))
	}
}

func TestC247AdaptiveUint64CodecFallsBackForUnorderedValues(t *testing.T) {
	values := []uint64{9, 1, 8, 2, 7, 3}

	encoded, mode, err := EncodeUint64(values)
	if err != nil {
		t.Fatalf("EncodeUint64() error = %v", err)
	}
	if mode != Uint64EncodingRaw {
		t.Fatalf("EncodeUint64() mode = %v, want raw", mode)
	}
	decoded, decodedMode, err := DecodeUint64(encoded)
	if err != nil {
		t.Fatalf("DecodeUint64() error = %v", err)
	}
	if decodedMode != mode || !reflect.DeepEqual(decoded, values) {
		t.Fatalf("DecodeUint64() = %#v/%v, want %#v/%v", decoded, decodedMode, values, mode)
	}
}

func TestC247DeltaUint64RejectsDecreasingValues(t *testing.T) {
	if _, err := EncodeUint64Delta([]uint64{1, 3, 2}); !errors.Is(err, ErrUint64CodecNotMonotone) {
		t.Fatalf("EncodeUint64Delta() error = %v, want ErrUint64CodecNotMonotone", err)
	}
}

func TestC247Uint64CodecRejectsMalformedPayloads(t *testing.T) {
	valid, _, err := EncodeUint64([]uint64{1, 2})
	if err != nil {
		t.Fatalf("EncodeUint64() error = %v", err)
	}
	tests := map[string][]byte{
		"short header":  valid[:len(valid)-1],
		"bad magic":     append([]byte("BAD!"), valid[4:]...),
		"trailing byte": append(append([]byte(nil), valid...), 0),
	}
	for name, payload := range tests {
		t.Run(name, func(t *testing.T) {
			if _, _, err := DecodeUint64(payload); err == nil {
				t.Fatal("DecodeUint64() error = nil, want malformed payload error")
			}
		})
	}
}

func TestC247Uint64CodecHandlesEmptyInput(t *testing.T) {
	encoded, mode, err := EncodeUint64(nil)
	if err != nil {
		t.Fatalf("EncodeUint64(nil) error = %v", err)
	}
	decoded, decodedMode, err := DecodeUint64(encoded)
	if err != nil {
		t.Fatalf("DecodeUint64(empty) error = %v", err)
	}
	if decoded != nil || mode != Uint64EncodingRaw || decodedMode != mode {
		t.Fatalf("empty round trip = %#v/%v/%v, want nil/raw/raw", decoded, mode, decodedMode)
	}
}
