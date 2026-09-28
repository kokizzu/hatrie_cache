package hatCodec

import (
	"math"
	"testing"
)

func TestInt64DeltaRoundTrip(t *testing.T) {
	tests := []struct {
		name   string
		values []int64
	}{
		{name: "empty"},
		{name: "single", values: []int64{42}},
		{name: "increasing", values: []int64{10, 11, 12, 13, 14}},
		{name: "negative", values: []int64{0, -1, -3, -6, -10}},
		{name: "overflow transitions", values: []int64{math.MaxInt64, math.MinInt64, math.MaxInt64, math.MinInt64}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			frame := EncodeInt64Delta(tt.values)
			got, err := DecodeInt64Delta(frame, nil)
			if err != nil {
				t.Fatalf("DecodeInt64Delta() error = %v", err)
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

func TestInt64DeltaSelectsCompactAndRawModes(t *testing.T) {
	increasing := make([]int64, 4096)
	for i := range increasing {
		increasing[i] = int64(i) * 10
	}
	compactFrame := EncodeInt64Delta(increasing)
	if len(compactFrame) < 6 || compactFrame[5] != int64DeltaModeByte {
		t.Fatalf("increasing frame mode = %d, want byte-delta mode", compactFrame[5])
	}

	widerDeltas := make([]int64, 4096)
	for i := range widerDeltas {
		widerDeltas[i] = int64(i) * 1000
	}
	widerFrame := EncodeInt64Delta(widerDeltas)
	if len(widerFrame) < 6 || widerFrame[5] != int64DeltaModeDelta {
		t.Fatalf("wide-delta frame mode = %d, want varint-delta mode", widerFrame[5])
	}

	unique := make([]int64, 4096)
	for i := range unique {
		unique[i] = int64(uint64(i)*0x9e3779b97f4a7c15 + 0x123456789abcdef0)
	}
	rawFrame := EncodeInt64Delta(unique)
	if len(rawFrame) < 6 || rawFrame[5] != int64DeltaModeRaw {
		t.Fatalf("unique frame mode = %d, want raw mode", rawFrame[5])
	}
}

func TestInt64DeltaReusesDestination(t *testing.T) {
	values := make([]int64, 128)
	for i := range values {
		values[i] = int64(i / 4)
	}
	destination := make([]int64, 0, len(values))
	got, err := DecodeInt64Delta(EncodeInt64Delta(values), destination)
	if err != nil {
		t.Fatalf("DecodeInt64Delta() error = %v", err)
	}
	if &got[:1][0] != &destination[:1][0] {
		t.Fatal("decoder did not reuse the supplied destination")
	}
}

func TestInt64DeltaRejectsMalformedFrames(t *testing.T) {
	valid := EncodeInt64Delta([]int64{1, 2, 3, 4})
	tests := [][]byte{
		{},
		[]byte("HID"),
		[]byte{'H', 'I', 'D', 'X', 1, 0, 0},
		[]byte{'H', 'I', 'D', '1', 2, 0, 0},
		[]byte{'H', 'I', 'D', '1', 1, 3, 0},
		[]byte{'H', 'I', 'D', '1', 1, 0, 0x80, 0x00},
		[]byte{'H', 'I', 'D', '1', 1, 1, 1},
		valid[:len(valid)-1],
		append(append([]byte(nil), valid...), 0),
	}
	for i, frame := range tests {
		if _, err := DecodeInt64Delta(frame, nil); err == nil {
			t.Errorf("malformed frame %d was accepted", i)
		}
	}
}

func TestInt64DeltaRejectsHugeCountsBeforeAllocation(t *testing.T) {
	for _, mode := range []byte{int64DeltaModeRaw, int64DeltaModeDelta, int64DeltaModeByte} {
		frame := []byte{'H', 'I', 'D', '1', int64DeltaVersion, mode}
		frame = appendInt64DeltaUvarint(frame, uint64(1)<<62)
		if _, err := DecodeInt64Delta(frame, nil); err == nil {
			t.Fatalf("mode %d accepted an enormous truncated count", mode)
		}
	}
}

func TestInt64DeltaKnownFrameSize(t *testing.T) {
	values := make([]int64, 4096)
	for i := range values {
		values[i] = int64(i)
	}
	if got, want := len(EncodeInt64Delta(values)), 4111; got != want {
		t.Fatalf("increasing frame size = %d, want %d", got, want)
	}
}
