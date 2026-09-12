package hatReplication

import (
	"bytes"
	"errors"
	"testing"
)

func TestChangefeedCheckpointRoundTripBindsSourceAndAdvances(t *testing.T) {
	checkpoint, err := NewChangefeedCheckpoint("orders", 10)
	if err != nil {
		t.Fatalf("NewChangefeedCheckpoint() error = %v", err)
	}
	frontier := NewChangefeedFrontier(10)
	progress, err := frontier.Advance(12)
	if err != nil {
		t.Fatalf("Advance() error = %v", err)
	}
	checkpoint, err = checkpoint.Advance(progress)
	if err != nil || checkpoint.Sequence != 12 {
		t.Fatalf("checkpoint Advance() = %#v, %v, want sequence 12", checkpoint, err)
	}
	encoded, err := checkpoint.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary() error = %v", err)
	}
	decoded, err := UnmarshalChangefeedCheckpoint(encoded)
	if err != nil {
		t.Fatalf("UnmarshalChangefeedCheckpoint() error = %v", err)
	}
	if decoded != checkpoint {
		t.Fatalf("decoded checkpoint = %#v, want %#v", decoded, checkpoint)
	}
	encoded[encodedHeaderSourceOffset] ^= 1
	if decoded.Source != "orders" {
		t.Fatalf("decoded source changed after encoded mutation = %q", decoded.Source)
	}
	if _, err := checkpoint.Advance(ChangefeedProgress{Sequence: 11, Progressed: true}); !errors.Is(err, ErrChangefeedCheckpointRegressed) {
		t.Fatalf("regressed checkpoint error = %v, want ErrChangefeedCheckpointRegressed", err)
	}
	if _, err := checkpoint.Advance(ChangefeedProgress{Sequence: 13}); !errors.Is(err, ErrChangefeedProgressInvalid) {
		t.Fatalf("non-progress checkpoint error = %v, want ErrChangefeedProgressInvalid", err)
	}
	if _, err := NewChangefeedCheckpoint(" ", 1); !errors.Is(err, ErrChangefeedCheckpointSourceRequired) {
		t.Fatalf("empty source error = %v, want ErrChangefeedCheckpointSourceRequired", err)
	}
	if _, err := UnmarshalChangefeedCheckpoint(bytes.Repeat([]byte{'x'}, MaxChangefeedCheckpointBytes+1)); !errors.Is(err, ErrChangefeedCheckpointInvalid) {
		t.Fatalf("oversized checkpoint error = %v, want ErrChangefeedCheckpointInvalid", err)
	}
}

func TestChangefeedCheckpointRejectsMalformedFrames(t *testing.T) {
	checkpoint, err := NewChangefeedCheckpoint("orders", 1)
	if err != nil {
		t.Fatalf("NewChangefeedCheckpoint() error = %v", err)
	}
	encoded, err := checkpoint.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary() error = %v", err)
	}
	for name, malformed := range map[string][]byte{
		"short":    encoded[:len(encoded)-1],
		"magic":    append([]byte("bad!"), encoded[4:]...),
		"trailing": append(append([]byte(nil), encoded...), 0),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := UnmarshalChangefeedCheckpoint(malformed); !errors.Is(err, ErrChangefeedCheckpointInvalid) {
				t.Fatalf("UnmarshalChangefeedCheckpoint() error = %v, want ErrChangefeedCheckpointInvalid", err)
			}
		})
	}
}
