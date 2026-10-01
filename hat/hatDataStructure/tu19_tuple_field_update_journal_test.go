package hatDataStructure

import (
	"bytes"
	"encoding/binary"
	"errors"
	"reflect"
	"testing"
)

func TestTUG19TupleFieldUpdateJournalRoundTripAndApply(t *testing.T) {
	var number [8]byte
	binary.BigEndian.PutUint64(number[:], uint64(10))
	cache, err := NewPackedTuple([][]byte{
		[]byte("active"), []byte("abc"), number[:], []byte("west"),
	})
	if err != nil {
		t.Fatalf("NewPackedTuple() error = %v", err)
	}
	record := TupleFieldUpdateJournalRecord{
		Sequence: 42,
		Key:      "customer-000042",
		Updates: []TupleFieldUpdate{
			{Index: 0, Kind: TupleFieldSet, Value: []byte("ready")},
			{Index: 1, Kind: TupleFieldSplice, Start: 1, Remove: 1, Insert: []byte("XYZ")},
			{Index: 2, Kind: TupleFieldAddInt64, Delta: 7},
			{Index: 3, Kind: TupleFieldSet, Value: []byte("region-eu-west")},
		},
	}
	payload, err := MarshalTupleFieldUpdateJournalRecord(record)
	if err != nil {
		t.Fatalf("MarshalTupleFieldUpdateJournalRecord() error = %v", err)
	}
	decoded, err := UnmarshalTupleFieldUpdateJournalRecord(payload)
	if err != nil {
		t.Fatalf("UnmarshalTupleFieldUpdateJournalRecord() error = %v", err)
	}
	if !reflect.DeepEqual(decoded, record) {
		t.Fatalf("decoded record = %#v, want %#v", decoded, record)
	}
	reencoded, err := MarshalTupleFieldUpdateJournalRecord(decoded)
	if err != nil {
		t.Fatalf("MarshalTupleFieldUpdateJournalRecord(decoded) error = %v", err)
	}
	if !bytes.Equal(reencoded, payload) {
		t.Fatalf("journal encoding is not deterministic: %x != %x", reencoded, payload)
	}

	updated, err := decoded.ApplyTo(cache)
	if err != nil {
		t.Fatalf("ApplyTo() error = %v", err)
	}
	var wantNumber [8]byte
	binary.BigEndian.PutUint64(wantNumber[:], uint64(17))
	wantFields := [][]byte{[]byte("ready"), []byte("aXYZc"), wantNumber[:], []byte("region-eu-west")}
	for index, want := range wantFields {
		got, err := updated.Field(index)
		if err != nil {
			t.Fatalf("Field(%d) error = %v", index, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("Field(%d) = %q, want %q", index, got, want)
		}
	}
	original, err := cache.Field(0)
	if err != nil || !bytes.Equal(original, []byte("active")) {
		t.Fatalf("source tuple changed: %q / %v", original, err)
	}
}

func TestTUG19TupleFieldUpdateJournalRejectsCorruptionAndLimits(t *testing.T) {
	record := TupleFieldUpdateJournalRecord{
		Sequence: 1,
		Key:      "key",
		Updates:  []TupleFieldUpdate{{Index: 0, Kind: TupleFieldSet, Value: []byte("value")}},
	}
	payload, err := MarshalTupleFieldUpdateJournalRecord(record)
	if err != nil {
		t.Fatalf("MarshalTupleFieldUpdateJournalRecord() error = %v", err)
	}
	corrupted := append([]byte(nil), payload...)
	corrupted[len(corrupted)-1]++
	if _, err := UnmarshalTupleFieldUpdateJournalRecord(corrupted); !errors.Is(err, ErrTupleFieldUpdateJournalChecksum) {
		t.Fatalf("checksum error = %v, want ErrTupleFieldUpdateJournalChecksum", err)
	}
	for name, malformed := range map[string][]byte{
		"short":    payload[:len(payload)-1],
		"trailing": append(append([]byte(nil), payload...), 0),
		"magic":    append([]byte("bad!"), payload[4:]...),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := UnmarshalTupleFieldUpdateJournalRecord(malformed); !errors.Is(err, ErrTupleFieldUpdateJournalWire) && !errors.Is(err, ErrTupleFieldUpdateJournalChecksum) {
				t.Fatalf("malformed error = %v", err)
			}
		})
	}
	if _, err := MarshalTupleFieldUpdateJournalRecord(TupleFieldUpdateJournalRecord{Key: "key", Updates: record.Updates}); !errors.Is(err, ErrTupleFieldUpdateJournalWire) {
		t.Fatalf("zero sequence error = %v, want ErrTupleFieldUpdateJournalWire", err)
	}
	if _, err := MarshalTupleFieldUpdateJournalRecord(TupleFieldUpdateJournalRecord{Sequence: 1, Key: "key"}); !errors.Is(err, ErrTupleFieldUpdateJournalWire) {
		t.Fatalf("empty updates error = %v, want ErrTupleFieldUpdateJournalWire", err)
	}
	tooMany := make([]TupleFieldUpdate, MaxTupleFieldUpdateJournalUpdates+1)
	for index := range tooMany {
		tooMany[index] = TupleFieldUpdate{Index: index, Kind: TupleFieldSet}
	}
	if _, err := MarshalTupleFieldUpdateJournalRecord(TupleFieldUpdateJournalRecord{Sequence: 1, Key: "key", Updates: tooMany}); !errors.Is(err, ErrTupleFieldUpdateJournalLimit) {
		t.Fatalf("too many updates error = %v, want ErrTupleFieldUpdateJournalLimit", err)
	}
	duplicate := record
	duplicate.Updates = append([]TupleFieldUpdate(nil), record.Updates...)
	duplicate.Updates = append(duplicate.Updates, TupleFieldUpdate{Index: duplicate.Updates[0].Index, Kind: TupleFieldSet})
	if _, err := MarshalTupleFieldUpdateJournalRecord(duplicate); !errors.Is(err, ErrTupleFieldUpdateJournalWire) {
		t.Fatalf("duplicate update error = %v, want ErrTupleFieldUpdateJournalWire", err)
	}
}

func TestTUG19TupleFieldUpdateJournalApplyPreservesValidation(t *testing.T) {
	cache, err := NewPackedTuple([][]byte{[]byte("not-an-int")})
	if err != nil {
		t.Fatalf("NewPackedTuple() error = %v", err)
	}
	record := TupleFieldUpdateJournalRecord{
		Sequence: 1,
		Key:      "key",
		Updates:  []TupleFieldUpdate{{Index: 0, Kind: TupleFieldAddInt64, Delta: 1}},
	}
	if _, err := record.ApplyTo(cache); !errors.Is(err, ErrTupleFieldUpdateType) {
		t.Fatalf("ApplyTo() error = %v, want ErrTupleFieldUpdateType", err)
	}
}
