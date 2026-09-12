package hatDataStructure

import (
	"bytes"
	"errors"
	"testing"
)

func TestCursorTokenRoundTripBindsIndexAndVersion(t *testing.T) {
	codec, err := NewCursorTokenCodec([]byte("0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	key := []byte("middle")
	token, err := codec.Encode("orders_by_id", 7, key, 42)
	if err != nil {
		t.Fatal(err)
	}
	key[0] = 'x'
	decoded, err := codec.DecodeFor(token, "orders_by_id", 7)
	if err != nil {
		t.Fatalf("DecodeFor() error = %v", err)
	}
	if decoded.Index != "orders_by_id" || decoded.SchemaVersion != 7 || !bytes.Equal(decoded.Key, []byte("middle")) || decoded.ID != 42 {
		t.Fatalf("decoded token = %#v", decoded)
	}
	decoded.Key[0] = 'x'
	again, err := codec.Decode(token)
	if err != nil || !bytes.Equal(again.Key, []byte("middle")) {
		t.Fatalf("Decode() after returned-key mutation = %#v/%v", again, err)
	}
	if _, err := codec.DecodeFor(token, "orders_by_name", 7); !errors.Is(err, ErrCursorTokenBindingMismatch) {
		t.Fatalf("wrong index error = %v, want ErrCursorTokenBindingMismatch", err)
	}
	if _, err := codec.DecodeFor(token, "orders_by_id", 8); !errors.Is(err, ErrCursorTokenBindingMismatch) {
		t.Fatalf("wrong version error = %v, want ErrCursorTokenBindingMismatch", err)
	}

	tampered := []byte(token)
	tamperPosition := len(tampered) - 2
	if tampered[tamperPosition] == 'A' {
		tampered[tamperPosition] = 'B'
	} else {
		tampered[tamperPosition] = 'A'
	}
	if _, err := codec.Decode(string(tampered)); !errors.Is(err, ErrCursorTokenAuthentication) {
		t.Fatalf("tampered token error = %v, want ErrCursorTokenAuthentication", err)
	}
	other, err := NewCursorTokenCodec([]byte("fedcba9876543210"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := other.Decode(token); !errors.Is(err, ErrCursorTokenAuthentication) {
		t.Fatalf("wrong secret error = %v, want ErrCursorTokenAuthentication", err)
	}
}

func TestOrderedSnapshotCursorSeekAfterEntryDoesNotSkipDuplicateKeys(t *testing.T) {
	index, err := NewOrderedIndex(func(value int) string {
		if value < 3 {
			return "a"
		}
		return "b"
	}, func(left, right string) int {
		if left < right {
			return -1
		}
		if left > right {
			return 1
		}
		return 0
	}, 4)
	if err != nil {
		t.Fatal(err)
	}
	for id, value := range []int{1, 2, 3, 4} {
		if err := index.Upsert(uint64(id+1), value); err != nil {
			t.Fatal(err)
		}
	}
	cursor, ok := index.SnapshotCursor()
	if !ok {
		t.Fatal("SnapshotCursor() = false")
	}
	defer cursor.Close()
	if err := cursor.SeekAfterEntry("a", 2); err != nil {
		t.Fatalf("SeekAfterEntry() error = %v", err)
	}
	entry, next, err := cursor.Next()
	if err != nil || !next || entry.ID != 3 || entry.Value != 3 {
		t.Fatalf("Next() = %#v/%v/%v, want ID=3 value=3", entry, next, err)
	}
}
