package hatSql

import (
	"bytes"
	"errors"
	"testing"
)

func TestMZ010SQLSubscriptionWireEnvelopeRoundTrip(t *testing.T) {
	key := []byte("subscription-signing-key")
	want := SQLSubscriptionWireEnvelope{
		Mode:           SQLSubscriptionModeDifferential,
		SubscriptionID: "people-live",
		Sequence:       42,
		Diff:           -3,
		Payload:        []byte(`{"id":7,"name":"Ada"}`),
	}

	wire, err := SealSQLSubscriptionWireEnvelope(key, want)
	if err != nil {
		t.Fatalf("SealSQLSubscriptionWireEnvelope() error = %v", err)
	}
	got, err := OpenSQLSubscriptionWireEnvelope(key, wire)
	if err != nil {
		t.Fatalf("OpenSQLSubscriptionWireEnvelope() error = %v", err)
	}
	if got.Mode != want.Mode || got.SubscriptionID != want.SubscriptionID || got.Sequence != want.Sequence || got.Diff != want.Diff || !bytes.Equal(got.Payload, want.Payload) {
		t.Fatalf("round trip mismatch: got %#v want %#v", got, want)
	}

	wireAgain, err := SealSQLSubscriptionWireEnvelope(key, want)
	if err != nil {
		t.Fatalf("second SealSQLSubscriptionWireEnvelope() error = %v", err)
	}
	if !bytes.Equal(wire, wireAgain) {
		t.Fatal("identical input must produce deterministic wire output")
	}
}

func TestMZ010SQLSubscriptionWireEnvelopeRejectsTamperingAndWrongKey(t *testing.T) {
	wire, err := SealSQLSubscriptionWireEnvelope([]byte("correct-key"), SQLSubscriptionWireEnvelope{
		Mode:           SQLSubscriptionModeSnapshot,
		SubscriptionID: "snapshot",
		Sequence:       9,
		Payload:        []byte(`[{"id":1}]`),
	})
	if err != nil {
		t.Fatalf("SealSQLSubscriptionWireEnvelope() error = %v", err)
	}

	for _, name := range []string{"wrong key", "tampered header", "tampered payload"} {
		candidate := append([]byte(nil), wire...)
		key := []byte("correct-key")
		switch name {
		case "wrong key":
			key = []byte("wrong-key")
		case "tampered header":
			candidate[5] ^= 1
		case "tampered payload":
			candidate[len(candidate)-SQLSubscriptionWireSignatureBytes-1] ^= 1
		}
		if _, err := OpenSQLSubscriptionWireEnvelope(key, candidate); !errors.Is(err, ErrSQLSubscriptionWireEnvelopeAuthentication) {
			t.Fatalf("%s: error = %v, want authentication error", name, err)
		}
	}
}

func TestMZ010SQLSubscriptionWireEnvelopeRejectsInvalidInput(t *testing.T) {
	for _, key := range [][]byte{nil, {}} {
		if _, err := SealSQLSubscriptionWireEnvelope(key, SQLSubscriptionWireEnvelope{Mode: SQLSubscriptionModeSnapshot, SubscriptionID: "id"}); !errors.Is(err, ErrSQLSubscriptionWireEnvelopeInvalid) {
			t.Fatalf("empty key: error = %v", err)
		}
	}
	for _, envelope := range []SQLSubscriptionWireEnvelope{
		{Mode: SQLSubscriptionMode("unknown"), SubscriptionID: "id"},
		{Mode: SQLSubscriptionModeSnapshot},
		{Mode: SQLSubscriptionModeSnapshot, SubscriptionID: "id", Diff: 1},
		{Mode: SQLSubscriptionModeDifferential, SubscriptionID: string(bytes.Repeat([]byte{'x'}, MaxSQLSubscriptionWireSubscriptionIDBytes+1))},
		{Mode: SQLSubscriptionModeSnapshot, SubscriptionID: "id", Payload: make([]byte, MaxSQLSubscriptionWirePayloadBytes+1)},
	} {
		if _, err := SealSQLSubscriptionWireEnvelope([]byte("key"), envelope); !errors.Is(err, ErrSQLSubscriptionWireEnvelopeInvalid) {
			t.Fatalf("envelope %#v: error = %v", envelope, err)
		}
	}

	if _, err := OpenSQLSubscriptionWireEnvelope([]byte("key"), []byte("short")); !errors.Is(err, ErrSQLSubscriptionWireEnvelopeInvalid) {
		t.Fatalf("short wire: error = %v", err)
	}
}
