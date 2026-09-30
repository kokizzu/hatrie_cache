package hatDataStructure

import "testing"

func TestCursorTokenEncodeIntoMatchesEncodeAndReusesBuffer(t *testing.T) {
	codec, err := NewCursorTokenCodec([]byte("0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	key := []byte("2026-09-30T00:00:00Z")
	want, err := codec.Encode("orders_by_id", 7, key, 42)
	if err != nil {
		t.Fatal(err)
	}

	destination := make([]byte, 0, 1024)
	got, err := codec.EncodeInto(destination, "orders_by_id", 7, key, 42)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != want {
		t.Fatalf("EncodeInto() = %q, Encode() = %q", got, want)
	}
	if len(got) == 0 || &got[0] != &destination[:1][0] {
		t.Fatal("EncodeInto() did not reuse destination backing storage")
	}
	decoded, err := codec.Decode(string(got))
	if err != nil || decoded.Index != "orders_by_id" || decoded.SchemaVersion != 7 || decoded.ID != 42 || string(decoded.Key) != string(key) {
		t.Fatalf("Decode(EncodeInto()) = %#v, %v", decoded, err)
	}

	second, err := codec.EncodeInto(got[:0], "orders_by_id", 8, []byte("next"), 99)
	if err != nil {
		t.Fatal(err)
	}
	wantSecond, err := codec.Encode("orders_by_id", 8, []byte("next"), 99)
	if err != nil || string(second) != wantSecond {
		t.Fatalf("reused EncodeInto() = %q, Encode() = %q, err=%v", second, wantSecond, err)
	}
}

func TestCursorTokenEncodeIntoRejectsInvalidInput(t *testing.T) {
	codec, err := NewCursorTokenCodec([]byte("0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := codec.EncodeInto(make([]byte, 0, 128), "", 1, nil, 1); err == nil {
		t.Fatal("empty index was accepted")
	}
}

func TestCursorTokenEncodeIntoHandlesMaximumKeySize(t *testing.T) {
	codec, err := NewCursorTokenCodec([]byte("0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	key := make([]byte, MaxCursorTokenKeyBytes)
	for index := range key {
		key[index] = byte(index)
	}
	token, err := codec.EncodeInto(nil, "orders_by_id", 9, key, 77)
	if err != nil {
		t.Fatal(err)
	}
	if len(token) > MaxCursorTokenBytes {
		t.Fatalf("encoded maximum token length = %d, exceeds %d", len(token), MaxCursorTokenBytes)
	}
	decoded, err := codec.Decode(string(token))
	if err != nil || decoded.SchemaVersion != 9 || decoded.ID != 77 || string(decoded.Key) != string(key) {
		t.Fatalf("Decode(maximum EncodeInto()) = schema=%d id=%d key=%d/%d err=%v", decoded.SchemaVersion, decoded.ID, len(decoded.Key), len(key), err)
	}
}
