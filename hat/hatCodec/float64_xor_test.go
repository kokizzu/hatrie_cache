package hatCodec

import (
	"encoding/binary"
	"math"
	"testing"
)

func TestFloat64XORRoundTripPreservesBits(t *testing.T) {
	tests := []struct {
		name   string
		values []float64
	}{
		{name: "empty"},
		{name: "special", values: []float64{0, math.Copysign(0, -1), math.Inf(1), math.Inf(-1), math.Float64frombits(0x7ff8000000000042)}},
		{name: "repeated", values: []float64{3.5, 3.5, 3.5, 3.5}},
		{name: "changing", values: []float64{1, 1.5, 2, 2.5, 3, 3.5}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			frame := EncodeFloat64XOR(tt.values)
			got, err := DecodeFloat64XOR(frame, nil)
			if err != nil {
				t.Fatalf("DecodeFloat64XOR() error = %v", err)
			}
			if len(got) != len(tt.values) {
				t.Fatalf("decoded length = %d, want %d", len(got), len(tt.values))
			}
			for i := range tt.values {
				if math.Float64bits(got[i]) != math.Float64bits(tt.values[i]) {
					t.Fatalf("decoded[%d] bits = %#x, want %#x", i, math.Float64bits(got[i]), math.Float64bits(tt.values[i]))
				}
			}
		})
	}
}

func TestFloat64XORSelectsCompactAndRawModes(t *testing.T) {
	repeated := make([]float64, 4096)
	for i := range repeated {
		repeated[i] = 42.25
	}
	repeatedFrame := EncodeFloat64XOR(repeated)
	if len(repeatedFrame) < 6 || repeatedFrame[5] != float64XORModeXOR {
		t.Fatalf("repeated frame mode = %d, want XOR mode", repeatedFrame[5])
	}

	unique := make([]float64, 4096)
	for i := range unique {
		unique[i] = math.Float64frombits(uint64(i)*0x9e3779b97f4a7c15 + 0x3ff0000000000000)
	}
	uniqueFrame := EncodeFloat64XOR(unique)
	if len(uniqueFrame) < 6 || uniqueFrame[5] != float64XORModeRaw {
		t.Fatalf("unique frame mode = %d, want raw mode", uniqueFrame[5])
	}
}

func TestFloat64XORReusesDestination(t *testing.T) {
	values := make([]float64, 128)
	for i := range values {
		values[i] = float64(i / 4)
	}
	destination := make([]float64, 0, len(values))
	got, err := DecodeFloat64XOR(EncodeFloat64XOR(values), destination)
	if err != nil {
		t.Fatalf("DecodeFloat64XOR() error = %v", err)
	}
	if &got[:1][0] != &destination[:1][0] {
		t.Fatal("decoder did not reuse the supplied destination")
	}
}

func TestFloat64XORRejectsMalformedFrames(t *testing.T) {
	valid := EncodeFloat64XOR([]float64{1, 1, 2, 2})
	invalidDescriptor := []byte{'H', 'G', 'F', '1', 1, 1, 1, 0, 0x91}
	tests := [][]byte{
		{},
		[]byte("HGF"),
		[]byte{'H', 'G', 'F', 'X', 1, 0, 0},
		[]byte{'H', 'G', 'F', '1', 2, 0, 0},
		[]byte{'H', 'G', 'F', '1', 1, 2, 0},
		[]byte{'H', 'G', 'F', '1', 1, 0, 0x80, 0x00},
		[]byte{'H', 'G', 'F', '1', 1, 1, 1, 2},
		invalidDescriptor,
		valid[:len(valid)-1],
		append(append([]byte(nil), valid...), 0),
	}
	for i, frame := range tests {
		if _, err := DecodeFloat64XOR(frame, nil); err == nil {
			t.Errorf("malformed frame %d was accepted", i)
		}
	}
}

func TestFloat64XORRejectsHugeCountsBeforeAllocation(t *testing.T) {
	for _, mode := range []byte{float64XORModeRaw, float64XORModeXOR} {
		frame := []byte{'H', 'G', 'F', '1', float64XORVersion, mode}
		var count [10]byte
		n := binary.PutUvarint(count[:], uint64(1)<<62)
		frame = append(frame, count[:n]...)
		if _, err := DecodeFloat64XOR(frame, nil); err == nil {
			t.Fatalf("mode %d accepted an enormous truncated count", mode)
		}
	}
}

func TestFloat64XORKnownFrameSize(t *testing.T) {
	values := make([]float64, 4096)
	for i := range values {
		values[i] = 42.25
	}
	if got, want := len(EncodeFloat64XOR(values)), 4111; got != want {
		t.Fatalf("repeated frame size = %d, want %d", got, want)
	}
}
