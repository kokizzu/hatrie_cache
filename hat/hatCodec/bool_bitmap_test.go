package hatCodec

import "testing"

func TestBoolBitmapRoundTrip(t *testing.T) {
	tests := []struct {
		name   string
		values []bool
	}{
		{name: "empty"},
		{name: "single false", values: []bool{false}},
		{name: "single true", values: []bool{true}},
		{name: "mixed", values: []bool{true, false, true, true, false, false, true, false, true}},
		{name: "all true", values: []bool{true, true, true, true, true, true, true, true}},
		{name: "all false", values: make([]bool, 17)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			frame := EncodeBoolBitmap(tt.values)
			got, err := DecodeBoolBitmap(frame, nil)
			if err != nil {
				t.Fatalf("DecodeBoolBitmap() error = %v", err)
			}
			if len(got) != len(tt.values) {
				t.Fatalf("decoded length = %d, want %d", len(got), len(tt.values))
			}
			for i := range tt.values {
				if got[i] != tt.values[i] {
					t.Fatalf("decoded[%d] = %t, want %t", i, got[i], tt.values[i])
				}
			}
		})
	}
}

func TestBoolBitmapReusesDestination(t *testing.T) {
	values := make([]bool, 128)
	for i := range values {
		values[i] = i%3 == 0
	}
	destination := make([]bool, 0, len(values))
	got, err := DecodeBoolBitmap(EncodeBoolBitmap(values), destination)
	if err != nil {
		t.Fatalf("DecodeBoolBitmap() error = %v", err)
	}
	if &got[:1][0] != &destination[:1][0] {
		t.Fatal("decoder did not reuse the supplied destination")
	}
}

func TestBoolBitmapRejectsMalformedFrames(t *testing.T) {
	valid := EncodeBoolBitmap([]bool{true, false, true})
	padding := append([]byte(nil), valid...)
	padding[len(padding)-1] |= 0x80
	tests := [][]byte{
		{},
		[]byte("HCB"),
		[]byte{'H', 'C', 'B', 'X', 1, 0},
		[]byte{'H', 'C', 'B', '1', 2, 0},
		[]byte{'H', 'C', 'B', '1', 1, 0x80, 0x00},
		valid[:len(valid)-1],
		append(append([]byte(nil), valid...), 0),
		padding,
	}
	for i, frame := range tests {
		if _, err := DecodeBoolBitmap(frame, nil); err == nil {
			t.Errorf("malformed frame %d was accepted", i)
		}
	}
}

func TestBoolBitmapRejectsHugeCountsBeforeAllocation(t *testing.T) {
	frame := []byte{'H', 'C', 'B', '1', 1}
	frame = appendBoolBitmapUvarint(frame, uint64(1)<<62)
	if _, err := DecodeBoolBitmap(frame, nil); err == nil {
		t.Fatal("decoder accepted an enormous truncated count")
	}
}

func TestBoolBitmapKnownFrameSize(t *testing.T) {
	values := make([]bool, 4096)
	for i := range values {
		values[i] = i%5 == 0
	}
	if got, want := len(EncodeBoolBitmap(values)), 519; got != want {
		t.Fatalf("frame size = %d, want %d", got, want)
	}
}
