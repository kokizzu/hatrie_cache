package hatSql

import (
	"bytes"
	"errors"
	"testing"
)

func TestMZ010SubscriptionWireKeyringRotatesWithPreviousKeyGrace(t *testing.T) {
	oldKey := []byte("subscription-key-v1")
	nextKey := []byte("subscription-key-v2")
	envelope := SQLSubscriptionWireEnvelope{
		Mode:           SQLSubscriptionModeDifferential,
		SubscriptionID: "people-live",
		Sequence:       7,
		Diff:           1,
		Payload:        []byte(`{"id":7}`),
	}

	ring, err := NewSQLSubscriptionWireKeyring(oldKey)
	if err != nil {
		t.Fatalf("NewSQLSubscriptionWireKeyring() error = %v", err)
	}
	oldWire, err := ring.Seal(envelope)
	if err != nil {
		t.Fatalf("Seal() before rotation error = %v", err)
	}
	if err := ring.Rotate(nextKey); err != nil {
		t.Fatalf("Rotate() error = %v", err)
	}

	if got, err := ring.Open(oldWire); err != nil || !bytes.Equal(got.Payload, envelope.Payload) {
		t.Fatalf("Open() previous-key wire = %#v, %v", got, err)
	}
	newWire, err := ring.Seal(envelope)
	if err != nil {
		t.Fatalf("Seal() after rotation error = %v", err)
	}
	if bytes.Equal(oldWire, newWire) {
		t.Fatal("rotated key must change the authenticated wire bytes")
	}
	if _, err := OpenSQLSubscriptionWireEnvelope(oldKey, newWire); !errors.Is(err, ErrSQLSubscriptionWireEnvelopeAuthentication) {
		t.Fatalf("old key opened new wire with error = %v", err)
	}
	if got, err := ring.Open(newWire); err != nil || got.Sequence != envelope.Sequence {
		t.Fatalf("Open() active-key wire = %#v, %v", got, err)
	}
}

func TestMZ010SubscriptionWireKeyringKeepsBoundedPreviousKeys(t *testing.T) {
	keys := [][]byte{
		[]byte("subscription-key-v1"),
		[]byte("subscription-key-v2"),
		[]byte("subscription-key-v3"),
		[]byte("subscription-key-v4"),
		[]byte("subscription-key-v5"),
		[]byte("subscription-key-v6"),
	}
	ring, err := NewSQLSubscriptionWireKeyring(keys[0])
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range keys[1:] {
		if err := ring.Rotate(key); err != nil {
			t.Fatalf("Rotate(%q) error = %v", key, err)
		}
	}
	envelope := SQLSubscriptionWireEnvelope{
		Mode:           SQLSubscriptionModeSnapshot,
		SubscriptionID: "snapshot",
		Sequence:       1,
		Payload:        []byte(`[]`),
	}
	for index, key := range keys {
		wire, sealErr := SealSQLSubscriptionWireEnvelope(key, envelope)
		if sealErr != nil {
			t.Fatal(sealErr)
		}
		_, openErr := ring.Open(wire)
		if index < len(keys)-MaxSQLSubscriptionWirePreviousKeys-1 && !errors.Is(openErr, ErrSQLSubscriptionWireEnvelopeAuthentication) {
			t.Fatalf("evicted key %d error = %v, want authentication error", index, openErr)
		}
		if index >= len(keys)-MaxSQLSubscriptionWirePreviousKeys-1 && openErr != nil {
			t.Fatalf("retained key %d error = %v", index, openErr)
		}
	}
}

func TestMZ010SubscriptionWireKeyringRejectsInvalidConfiguration(t *testing.T) {
	if _, err := NewSQLSubscriptionWireKeyring(nil); !errors.Is(err, ErrSQLSubscriptionWireKeyringInvalid) {
		t.Fatalf("empty active key error = %v", err)
	}
	tooMany := make([][]byte, MaxSQLSubscriptionWirePreviousKeys+1)
	for index := range tooMany {
		tooMany[index] = []byte{byte(index + 1)}
	}
	if _, err := NewSQLSubscriptionWireKeyring([]byte("active"), tooMany...); !errors.Is(err, ErrSQLSubscriptionWireKeyringInvalid) {
		t.Fatalf("too many previous keys error = %v", err)
	}
	ring, err := NewSQLSubscriptionWireKeyring([]byte("active"))
	if err != nil {
		t.Fatal(err)
	}
	if err := ring.Rotate(nil); !errors.Is(err, ErrSQLSubscriptionWireKeyringInvalid) {
		t.Fatalf("empty rotation key error = %v", err)
	}
	if _, err := ring.Open([]byte("short")); !errors.Is(err, ErrSQLSubscriptionWireEnvelopeInvalid) {
		t.Fatalf("malformed wire error = %v", err)
	}
}
