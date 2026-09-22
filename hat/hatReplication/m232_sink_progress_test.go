package hatReplication

import (
	"bytes"
	"errors"
	"reflect"
	"testing"
)

func TestSinkProgressEmitterCouplesOutputAndFrontier(t *testing.T) {
	emitter, err := NewSinkProgressEmitter("orders", 0)
	if err != nil {
		t.Fatal(err)
	}
	records := []ExactlyOnceUpsertSinkRecord{
		{Sequence: 1, OutputID: "order-1", Key: []byte("1"), Value: []byte("open")},
		{Sequence: 2, OutputID: "order-2", Key: []byte("2"), Value: []byte("paid")},
	}
	envelope, err := emitter.Emit(records, 2)
	if err != nil {
		t.Fatal(err)
	}
	if envelope.Source != "orders" || envelope.Frontier != 2 || !reflect.DeepEqual(envelope.Records, records) {
		t.Fatalf("envelope = %#v", envelope)
	}
	records[0].Value[0] = 'x'
	if got := envelope.Records[0].Value[0]; got != 'o' {
		t.Fatalf("envelope retained caller mutation: %q", got)
	}
	filtered, err := emitter.Emit([]ExactlyOnceUpsertSinkRecord{{Sequence: 4, OutputID: "order-4", Key: []byte("4"), Value: []byte("shipped")}}, 5)
	if err != nil || filtered.Frontier != 5 {
		t.Fatalf("filtered envelope = %#v/%v", filtered, err)
	}
	if progress := emitter.Progress(); progress.Sequence != 5 || !progress.Progressed {
		t.Fatalf("progress = %#v", progress)
	}
}

func TestSinkProgressEmitterRejectsUncoupledProgress(t *testing.T) {
	emitter, err := NewSinkProgressEmitter("orders", 0)
	if err != nil {
		t.Fatal(err)
	}
	valid := []ExactlyOnceUpsertSinkRecord{{Sequence: 1, OutputID: "order-1", Key: []byte("1")}}
	tests := []struct {
		name     string
		records  []ExactlyOnceUpsertSinkRecord
		frontier uint64
		want     error
	}{
		{name: "empty", frontier: 1, want: ErrSinkProgressRecordsRequired},
		{name: "frontier behind output", records: valid, frontier: 0, want: ErrSinkProgressInvalid},
		{name: "zero frontier", records: valid, frontier: 0, want: ErrSinkProgressInvalid},
		{name: "duplicate sequence", records: []ExactlyOnceUpsertSinkRecord{valid[0], valid[0]}, frontier: 1, want: ErrSinkProgressInvalid},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := emitter.Emit(test.records, test.frontier); !errors.Is(err, test.want) {
				t.Fatalf("Emit() error = %v, want %v", err, test.want)
			}
		})
	}
	if _, err := emitter.Emit(valid, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := emitter.Emit([]ExactlyOnceUpsertSinkRecord{{Sequence: 2, OutputID: "order-2", Key: []byte("2")}}, 1); !errors.Is(err, ErrSinkProgressRegressed) {
		t.Fatalf("equal frontier error = %v", err)
	}
	if _, err := emitter.Emit([]ExactlyOnceUpsertSinkRecord{{Sequence: 1, OutputID: "order-1", Key: []byte("1")}}, 2); !errors.Is(err, ErrSinkProgressOverlap) {
		t.Fatalf("overlap error = %v", err)
	}
}

func TestSinkProgressEnvelopeBinaryRoundTripAndMalformed(t *testing.T) {
	envelope := SinkProgressEnvelope{
		Source:   "orders",
		Frontier: 42,
		Records:  []ExactlyOnceUpsertSinkRecord{{Sequence: 41, OutputID: "order-41", Key: []byte("41"), Value: []byte("closed"), Delete: true}},
	}
	encoded, err := envelope.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := UnmarshalSinkProgressEnvelope(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded, envelope) {
		t.Fatalf("decoded envelope = %#v, want %#v", decoded, envelope)
	}
	for _, malformed := range [][]byte{nil, encoded[:len(encoded)-1], append(append([]byte(nil), encoded...), 0)} {
		if _, err := UnmarshalSinkProgressEnvelope(malformed); !errors.Is(err, ErrSinkProgressInvalid) {
			t.Fatalf("malformed envelope error = %v", err)
		}
	}
	if bytes.Equal(encoded, nil) {
		t.Fatal("binary envelope unexpectedly empty")
	}
}
