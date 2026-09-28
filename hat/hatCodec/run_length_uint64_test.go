package hatCodec

import (
	"encoding/binary"
	"math"
	"testing"
)

func TestRunLengthUint64RoundTrip(t *testing.T) {
	tests := []struct {
		name   string
		values []uint64
	}{
		{name: "empty"},
		{name: "single", values: []uint64{42}},
		{name: "mixed", values: []uint64{0, 0, 1, 2, 3, 3, 3, math.MaxUint64, math.MaxUint64, 9}},
		{name: "all equal", values: []uint64{math.MaxUint64, math.MaxUint64, math.MaxUint64, math.MaxUint64}},
		{name: "alternating", values: []uint64{1, 2, 1, 2, 1, 2, 1, 2}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			frame := EncodeRunLengthUint64(tt.values)
			got, err := DecodeRunLengthUint64(frame, nil)
			if err != nil {
				t.Fatalf("DecodeRunLengthUint64() error = %v", err)
			}
			if len(got) != len(tt.values) {
				t.Fatalf("decoded length = %d, want %d", len(got), len(tt.values))
			}
			for i := range tt.values {
				if got[i] != tt.values[i] {
					t.Fatalf("decoded[%d] = %d, want %d", i, got[i], tt.values[i])
				}
			}
		})
	}
}

func TestRunLengthUint64ChoosesRLEOnlyWhenSmaller(t *testing.T) {
	repeated := make([]uint64, 1024)
	for i := range repeated {
		repeated[i] = 7
	}
	repeatedFrame := EncodeRunLengthUint64(repeated)
	if len(repeatedFrame) < 6 || repeatedFrame[5] != 1 {
		t.Fatalf("repeated frame mode = %d, want RLE mode", repeatedFrame[5])
	}

	unique := make([]uint64, 1024)
	for i := range unique {
		unique[i] = uint64(i)
	}
	uniqueFrame := EncodeRunLengthUint64(unique)
	if len(uniqueFrame) < 6 || uniqueFrame[5] != 0 {
		t.Fatalf("unique frame mode = %d, want raw mode", uniqueFrame[5])
	}
}

func TestRunLengthUint64ReusesDestination(t *testing.T) {
	values := make([]uint64, 128)
	for i := range values {
		values[i] = uint64(i / 4)
	}
	frame := EncodeRunLengthUint64(values)
	destination := make([]uint64, 0, len(values))
	got, err := DecodeRunLengthUint64(frame, destination)
	if err != nil {
		t.Fatalf("DecodeRunLengthUint64() error = %v", err)
	}
	if len(got) != len(values) || &got[:1][0] != &destination[:1][0] {
		t.Fatal("decoder did not reuse the supplied destination")
	}
}

func TestRunLengthUint64RejectsMalformedFrames(t *testing.T) {
	valid := EncodeRunLengthUint64([]uint64{1, 1, 2})
	tests := [][]byte{
		{},
		[]byte("HCR"),
		[]byte{'H', 'C', 'R', 'X', 1, 0, 0},
		[]byte{'H', 'C', 'R', '1', 2, 0, 0},
		[]byte{'H', 'C', 'R', '1', 1, 2, 0},
		[]byte{'H', 'C', 'R', '1', 1, 0, 0x80, 0x00},
		[]byte{'H', 'C', 'R', '1', 1, 1, 1, 0},
		valid[:len(valid)-1],
	}
	for i, frame := range tests {
		if _, err := DecodeRunLengthUint64(frame, nil); err == nil {
			t.Errorf("malformed frame %d was accepted", i)
		}
	}
}

func TestRunLengthUint64RejectsTrailingBytes(t *testing.T) {
	frame := EncodeRunLengthUint64([]uint64{1, 1, 2, 2})
	frame = append(frame, 0)
	if _, err := DecodeRunLengthUint64(frame, nil); err == nil {
		t.Fatal("decoder accepted trailing bytes")
	}
}

func TestRunLengthUint64RejectsHugeCountsBeforeAllocation(t *testing.T) {
	count := uint64(1) << 62
	for _, mode := range []byte{runLengthUint64RawMode, runLengthUint64RLEMode} {
		frame := []byte{'H', 'C', 'R', '1', runLengthUint64Version, mode}
		frame = appendRunLengthUint64Uvarint(frame, count)
		if _, err := DecodeRunLengthUint64(frame, nil); err == nil {
			t.Fatalf("mode %d accepted an enormous truncated count", mode)
		}
	}
}

func TestRunLengthUint64RawPayloadIsLittleEndian(t *testing.T) {
	values := []uint64{0x0102030405060708}
	frame := EncodeRunLengthUint64(values)
	if frame[5] != 0 {
		t.Fatal("single value unexpectedly selected RLE mode")
	}
	encoded := binary.LittleEndian.Uint64(frame[len(frame)-8:])
	if encoded != values[0] {
		t.Fatalf("raw payload value = %#x, want %#x", encoded, values[0])
	}
}

func TestRunLengthUint64KnownFrameSizes(t *testing.T) {
	repeated := make([]uint64, 4096)
	for i := range repeated {
		repeated[i] = uint64(i / 32)
	}
	if got, want := len(EncodeRunLengthUint64(repeated)), 264; got != want {
		t.Fatalf("repeated frame size = %d, want %d", got, want)
	}

	unique := make([]uint64, 4096)
	for i := range unique {
		unique[i] = uint64(i)*0x9e3779b97f4a7c15 + 0x123456789abcdef0
	}
	if got, want := len(EncodeRunLengthUint64(unique)), 32776; got != want {
		t.Fatalf("unique frame size = %d, want %d", got, want)
	}
}
