package hatDataStructure

import (
	"bytes"
	"errors"
	"reflect"
	"testing"
)

func tupleFieldUpdateJournalTestFormat(t *testing.T) TupleFormat {
	t.Helper()
	format, err := NewTupleFormat(7, []TupleFieldSpec{
		{Name: "id", Type: TupleFieldInt64},
		{Name: "name", Type: TupleFieldString},
		{Name: "payload", Type: TupleFieldBytes},
	})
	if err != nil {
		t.Fatal(err)
	}
	return format
}

func tupleFieldUpdateJournalTestTuple(t *testing.T, format TupleFormat, id int64) VersionedTuple {
	t.Helper()
	tuple, err := NewVersionedTuple(format, []TupleFieldValue{
		TupleInt64(id),
		TupleString("alice"),
		TupleBytes([]byte{1, 2, 3}),
	})
	if err != nil {
		t.Fatal(err)
	}
	return tuple
}

func TestTupleFieldUpdateJournalRoundTripAndApply(t *testing.T) {
	format := tupleFieldUpdateJournalTestFormat(t)
	source := tupleFieldUpdateJournalTestTuple(t, format, 10)
	record := TupleFieldUpdateJournalRecord{
		Sequence:      7,
		SchemaVersion: format.Version(),
		Key:           []byte("account:42"),
		Updates: []TupleFieldUpdate{
			{Index: 0, Kind: TupleFieldAddInt64, Delta: 5},
			{Index: 1, Kind: TupleFieldSplice, Start: 5, Insert: []byte("-admin")},
			{Index: 2, Kind: TupleFieldSet, Value: []byte{9, 8, 7}},
		},
	}
	wire, err := MarshalTupleFieldUpdateJournal(record)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := UnmarshalTupleFieldUpdateJournal(wire)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded, record) {
		t.Fatalf("decoded record = %#v, want %#v", decoded, record)
	}

	applier, err := NewTupleFieldUpdateJournalApplier(6)
	if err != nil {
		t.Fatal(err)
	}
	updated, err := applier.Apply(source, format, decoded)
	if err != nil {
		t.Fatal(err)
	}
	values, err := format.Unpack(updated.Tuple())
	if err != nil {
		t.Fatal(err)
	}
	if values[0].Int64 != 15 || values[1].String != "alice-admin" || !bytes.Equal(values[2].Bytes, []byte{9, 8, 7}) {
		t.Fatalf("updated values = %#v, want id 15, alice-admin, 09 08 07", values)
	}
	if got := applier.LastSequence(); got != 7 {
		t.Fatalf("last sequence = %d, want 7", got)
	}

	if _, err := applier.Apply(updated, format, decoded); !errors.Is(err, ErrTupleFieldUpdateJournalSequence) {
		t.Fatalf("replay error = %v, want sequence error", err)
	}
	gap := decoded
	gap.Sequence = 9
	if _, err := applier.Apply(updated, format, gap); !errors.Is(err, ErrTupleFieldUpdateJournalSequenceGap) {
		t.Fatalf("sequence gap error = %v, want gap error", err)
	}
}

func TestTupleFieldUpdateJournalRejectsCorruptionAndInvalidRecords(t *testing.T) {
	format := tupleFieldUpdateJournalTestFormat(t)
	record := TupleFieldUpdateJournalRecord{
		Sequence:      1,
		SchemaVersion: format.Version(),
		Updates:       []TupleFieldUpdate{{Index: 0, Kind: TupleFieldAddInt64, Delta: 1}},
	}
	wire, err := MarshalTupleFieldUpdateJournal(record)
	if err != nil {
		t.Fatal(err)
	}
	corrupt := append([]byte(nil), wire...)
	corrupt[len(corrupt)-5] ^= 0x80
	if _, err := UnmarshalTupleFieldUpdateJournal(corrupt); !errors.Is(err, ErrTupleFieldUpdateJournalChecksum) {
		t.Fatalf("checksum error = %v, want checksum error", err)
	}
	if _, err := UnmarshalTupleFieldUpdateJournal(wire[:len(wire)-1]); err == nil || (!errors.Is(err, ErrTupleFieldUpdateJournalWire) && !errors.Is(err, ErrTupleFieldUpdateJournalChecksum)) {
		t.Fatalf("truncated error = %v, want wire or checksum error", err)
	}

	invalid := []TupleFieldUpdateJournalRecord{
		{SchemaVersion: format.Version(), Updates: record.Updates},
		{Sequence: 1, SchemaVersion: format.Version(), Updates: []TupleFieldUpdate{{Index: 0, Kind: TupleFieldSet, Value: make([]byte, MaxTupleFieldUpdateJournalValueBytes+1)}}},
		{Sequence: 1, SchemaVersion: format.Version(), Updates: []TupleFieldUpdate{{Index: 0, Kind: TupleFieldAddInt64}, {Index: 0, Kind: TupleFieldSet}}},
		{Sequence: 1, SchemaVersion: format.Version(), Updates: []TupleFieldUpdate{{Index: -1, Kind: TupleFieldSet}}},
	}
	for index, candidate := range invalid {
		if _, err := MarshalTupleFieldUpdateJournal(candidate); !errors.Is(err, ErrTupleFieldUpdateJournalInvalid) && !errors.Is(err, ErrTupleFieldUpdateJournalLimit) && !errors.Is(err, ErrTupleFieldUpdateDuplicate) && !errors.Is(err, ErrTupleFieldUpdateIndex) {
			t.Fatalf("invalid record %d error = %v", index, err)
		}
	}
	tooLarge := TupleFieldUpdateJournalRecord{
		Sequence:      1,
		SchemaVersion: format.Version(),
		Updates:       make([]TupleFieldUpdate, 5),
	}
	for index := range tooLarge.Updates {
		tooLarge.Updates[index] = TupleFieldUpdate{Index: index, Kind: TupleFieldSet, Value: make([]byte, MaxTupleFieldUpdateJournalValueBytes)}
	}
	if _, err := MarshalTupleFieldUpdateJournal(tooLarge); !errors.Is(err, ErrTupleFieldUpdateJournalLimit) {
		t.Fatalf("total-size error = %v, want journal limit", err)
	}
}

func TestTupleFieldUpdateJournalApplyIsAtomicAndVersionBound(t *testing.T) {
	format := tupleFieldUpdateJournalTestFormat(t)
	source := tupleFieldUpdateJournalTestTuple(t, format, 10)
	applier, err := NewTupleFieldUpdateJournalApplier(0)
	if err != nil {
		t.Fatal(err)
	}
	badType := TupleFieldUpdateJournalRecord{
		Sequence:      1,
		SchemaVersion: format.Version(),
		Updates:       []TupleFieldUpdate{{Index: 1, Kind: TupleFieldAddInt64, Delta: 1}},
	}
	if _, err := applier.Apply(source, format, badType); !errors.Is(err, ErrTupleFieldUpdateType) {
		t.Fatalf("bad type error = %v, want type error", err)
	}
	if got := applier.LastSequence(); got != 0 {
		t.Fatalf("last sequence after failed apply = %d, want 0", got)
	}

	wrongVersion := TupleFieldUpdateJournalRecord{
		Sequence:      1,
		SchemaVersion: format.Version() + 1,
		Updates:       []TupleFieldUpdate{{Index: 0, Kind: TupleFieldAddInt64, Delta: 1}},
	}
	if _, err := applier.Apply(source, format, wrongVersion); !errors.Is(err, ErrTupleFieldUpdateJournalVersion) {
		t.Fatalf("version error = %v, want version error", err)
	}
	if got := applier.LastSequence(); got != 0 {
		t.Fatalf("last sequence after wrong version = %d, want 0", got)
	}
}

func TestTupleFieldUpdateJournalRejectsApplierArguments(t *testing.T) {
	if _, err := NewTupleFieldUpdateJournalApplier(^uint64(0)); !errors.Is(err, ErrTupleFieldUpdateJournalInvalid) {
		t.Fatalf("invalid applier sequence error = %v, want invalid error", err)
	}
	if _, err := MarshalTupleFieldUpdateJournal(TupleFieldUpdateJournalRecord{Sequence: 1}); !errors.Is(err, ErrTupleFieldUpdateJournalInvalid) {
		t.Fatalf("missing schema version error = %v, want invalid error", err)
	}
}
