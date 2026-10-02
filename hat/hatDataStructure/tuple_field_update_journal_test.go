package hatDataStructure

import (
	"bytes"
	"encoding/binary"
	"errors"
	"reflect"
	"testing"
)

func TestTupleFieldUpdateJournalRoundTripAndApply(t *testing.T) {
	data := make([]byte, 4+8+6)
	copy(data, "west")
	binary.BigEndian.PutUint64(data[4:12], uint64(4))
	copy(data[12:], "abcdef")
	cache, err := NewTupleFieldOffsetCache(data, []uint32{4, 8, 6})
	if err != nil {
		t.Fatalf("NewTupleFieldOffsetCache() error = %v", err)
	}
	updates := []TupleFieldUpdate{
		{Index: 0, Kind: TupleFieldSet, Value: []byte("east")},
		{Index: 1, Kind: TupleFieldAddInt64, Delta: 8},
		{Index: 2, Kind: TupleFieldSplice, Start: 2, Remove: 2, Insert: []byte("XYZ")},
	}
	encoded, err := MarshalTupleFieldUpdateJournal(42, updates)
	if err != nil {
		t.Fatalf("MarshalTupleFieldUpdateJournal() error = %v", err)
	}
	if len(encoded) >= 256 {
		t.Fatalf("journal bytes = %d, want compact record below 256 bytes", len(encoded))
	}
	decoded, err := UnmarshalTupleFieldUpdateJournal(encoded)
	if err != nil {
		t.Fatalf("UnmarshalTupleFieldUpdateJournal() error = %v", err)
	}
	if decoded.Sequence != 42 || !reflect.DeepEqual(decoded.Updates, updates) {
		t.Fatalf("decoded record = %#v, want sequence 42 and %#v", decoded, updates)
	}
	decoded.Updates[0].Value[0] = 'E'
	if updates[0].Value[0] != 'e' {
		t.Fatal("decoded update payload aliases caller input")
	}
	updated, err := decoded.Apply(cache)
	if err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	if got, _ := updated.Field(0); string(got) != "East" {
		t.Fatalf("updated field 0 = %q, want East", got)
	}
	field, _ := updated.Field(1)
	if got := int64(binary.BigEndian.Uint64(field)); got != 12 {
		t.Fatalf("updated field 1 = %d, want 12", got)
	}
	if got, _ := updated.Field(2); string(got) != "abXYZef" {
		t.Fatalf("updated field 2 = %q, want abXYZef", got)
	}
	encodedAgain, err := MarshalTupleFieldUpdateJournal(42, updates)
	if err != nil {
		t.Fatalf("MarshalTupleFieldUpdateJournal() second error = %v", err)
	}
	if !bytes.Equal(encoded, encodedAgain) {
		t.Fatal("journal encoding is not deterministic")
	}
}

func TestTupleFieldUpdateJournalRejectsCorruptionAndMalformedRecords(t *testing.T) {
	encoded, err := MarshalTupleFieldUpdateJournal(1, []TupleFieldUpdate{{Index: 0, Kind: TupleFieldSet, Value: []byte("x")}})
	if err != nil {
		t.Fatalf("MarshalTupleFieldUpdateJournal() error = %v", err)
	}
	corrupt := append([]byte(nil), encoded...)
	corrupt[len(corrupt)-1] ^= 0x01
	if _, err := UnmarshalTupleFieldUpdateJournal(corrupt); !errors.Is(err, ErrTupleFieldUpdateJournalChecksum) {
		t.Fatalf("corrupt record error = %v, want checksum error", err)
	}
	for _, malformed := range [][]byte{
		nil,
		[]byte("TFJ1"),
		append(append([]byte(nil), encoded...), 0),
	} {
		if _, err := UnmarshalTupleFieldUpdateJournal(malformed); err == nil {
			t.Fatalf("UnmarshalTupleFieldUpdateJournal(%x) error = nil", malformed)
		}
	}
	if _, err := MarshalTupleFieldUpdateJournal(1, []TupleFieldUpdate{
		{Index: -1, Kind: TupleFieldSet, Value: []byte("x")},
	}); !errors.Is(err, ErrTupleFieldUpdateJournalInvalid) {
		t.Fatalf("negative index error = %v, want invalid record", err)
	}
	if _, err := MarshalTupleFieldUpdateJournal(1, []TupleFieldUpdate{
		{Index: 0, Kind: TupleFieldSet, Value: []byte("x")},
		{Index: 0, Kind: TupleFieldSet, Value: []byte("y")},
	}); !errors.Is(err, ErrTupleFieldUpdateDuplicate) {
		t.Fatalf("duplicate index error = %v, want duplicate error", err)
	}
}
