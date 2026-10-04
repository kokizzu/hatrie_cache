package hatDataStructure_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"reflect"
	"testing"

	"hatrie_cache/hat/hatDataStructure"
)

func TestTupleFieldUpdateJournalRoundTripAndReplay(t *testing.T) {
	integer := make([]byte, 8)
	binary.BigEndian.PutUint64(integer, 7)
	record := hatDataStructure.TupleFieldUpdateJournalRecord{
		Sequence: 42,
		Key:      "orders/42",
		Updates: []hatDataStructure.TupleFieldUpdate{
			{Index: 0, Kind: hatDataStructure.TupleFieldSet, Value: []byte("order-42")},
			{Index: 1, Kind: hatDataStructure.TupleFieldAddInt64, Delta: 3},
			{Index: 2, Kind: hatDataStructure.TupleFieldSplice, Start: 1, Remove: 2, Insert: []byte("XX")},
		},
	}
	encoded, err := hatDataStructure.MarshalTupleFieldUpdateJournalRecord(record)
	if err != nil {
		t.Fatalf("MarshalTupleFieldUpdateJournalRecord() error = %v", err)
	}
	decoded, err := hatDataStructure.UnmarshalTupleFieldUpdateJournalRecord(encoded)
	if err != nil {
		t.Fatalf("UnmarshalTupleFieldUpdateJournalRecord() error = %v", err)
	}
	if !reflect.DeepEqual(decoded, record) {
		t.Fatalf("decoded record = %#v, want %#v", decoded, record)
	}
	record.Updates[0].Value[0] = 'X'
	if decoded.Updates[0].Value[0] != 'o' {
		t.Fatal("decoded record aliases input update bytes")
	}

	initial, err := hatDataStructure.NewPackedTuple([][]byte{[]byte("order"), integer, []byte("tail")})
	if err != nil {
		t.Fatalf("NewPackedTuple() error = %v", err)
	}
	updated, err := decoded.Apply(initial)
	if err != nil {
		t.Fatalf("record.Apply() error = %v", err)
	}
	fields := make([][]byte, updated.FieldCount())
	for index := range fields {
		fields[index], err = updated.FieldInto(index, nil)
		if err != nil {
			t.Fatalf("FieldInto(%d) error = %v", index, err)
		}
	}
	wantInteger := make([]byte, 8)
	binary.BigEndian.PutUint64(wantInteger, 10)
	if !reflect.DeepEqual(fields, [][]byte{[]byte("order-42"), wantInteger, []byte("tXXl")}) {
		t.Fatalf("replayed fields = %#v, want order-42/10/tXXl", fields)
	}
}

func TestTupleFieldUpdateJournalStreamRoundTrip(t *testing.T) {
	records := []hatDataStructure.TupleFieldUpdateJournalRecord{
		{Sequence: 1, Key: "a", Updates: []hatDataStructure.TupleFieldUpdate{{Index: 0, Kind: hatDataStructure.TupleFieldSet, Value: []byte("one")}}},
		{Sequence: 2, Key: "b", Updates: []hatDataStructure.TupleFieldUpdate{{Index: 1, Kind: hatDataStructure.TupleFieldSplice, Start: 0, Remove: 0, Insert: []byte("two")}}},
	}
	var stream bytes.Buffer
	for _, record := range records {
		if err := hatDataStructure.WriteTupleFieldUpdateJournalRecord(&stream, record); err != nil {
			t.Fatalf("WriteTupleFieldUpdateJournalRecord() error = %v", err)
		}
	}
	for index, want := range records {
		got, err := hatDataStructure.ReadTupleFieldUpdateJournalRecord(&stream)
		if err != nil {
			t.Fatalf("ReadTupleFieldUpdateJournalRecord(%d) error = %v", index, err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("record %d = %#v, want %#v", index, got, want)
		}
	}
	if _, err := hatDataStructure.ReadTupleFieldUpdateJournalRecord(&stream); !errors.Is(err, io.EOF) {
		t.Fatalf("read after stream end error = %v, want EOF", err)
	}
}

func TestTupleFieldUpdateJournalRejectsCorruptionAndInvalidRecords(t *testing.T) {
	valid := hatDataStructure.TupleFieldUpdateJournalRecord{
		Sequence: 1,
		Key:      "orders/1",
		Updates:  []hatDataStructure.TupleFieldUpdate{{Index: 0, Kind: hatDataStructure.TupleFieldSet, Value: []byte("ok")}},
	}
	encoded, err := hatDataStructure.MarshalTupleFieldUpdateJournalRecord(valid)
	if err != nil {
		t.Fatalf("MarshalTupleFieldUpdateJournalRecord() error = %v", err)
	}
	corrupt := append([]byte(nil), encoded...)
	corrupt[len(corrupt)-1] ^= 0xff
	if _, err := hatDataStructure.UnmarshalTupleFieldUpdateJournalRecord(corrupt); !errors.Is(err, hatDataStructure.ErrTupleFieldUpdateJournalChecksum) {
		t.Fatalf("corrupt decode error = %v, want checksum error", err)
	}

	cases := []hatDataStructure.TupleFieldUpdateJournalRecord{
		{Sequence: 1, Key: "", Updates: valid.Updates},
		{Sequence: 1, Key: "orders/1", Updates: nil},
		{Sequence: 1, Key: "orders/1", Updates: []hatDataStructure.TupleFieldUpdate{{Index: 0, Kind: hatDataStructure.TupleFieldSet, Value: []byte("a")}, {Index: 0, Kind: hatDataStructure.TupleFieldSet, Value: []byte("b")}}},
		{Sequence: 1, Key: "orders/1", Updates: []hatDataStructure.TupleFieldUpdate{{Index: -1, Kind: hatDataStructure.TupleFieldSet, Value: []byte("a")}}},
	}
	for index, record := range cases {
		if _, err := hatDataStructure.MarshalTupleFieldUpdateJournalRecord(record); !errors.Is(err, hatDataStructure.ErrTupleFieldUpdateJournalInvalid) {
			t.Errorf("invalid record %d error = %v, want invalid record", index, err)
		}
	}
	if _, err := hatDataStructure.UnmarshalTupleFieldUpdateJournalRecord(encoded[:len(encoded)-1]); !errors.Is(err, hatDataStructure.ErrTupleFieldUpdateJournalTruncated) {
		t.Fatalf("truncated decode error = %v, want truncated error", err)
	}
}
func TestTupleFieldUpdateJournalApplyValidationPreservesCache(t *testing.T) {
	cache, err := hatDataStructure.NewPackedTuple([][]byte{[]byte("stable")})
	if err != nil {
		t.Fatal(err)
	}

	record := hatDataStructure.TupleFieldUpdateJournalRecord{
		Key: "orders/42",
		Updates: []hatDataStructure.TupleFieldUpdate{{
			Index: 0,
			Kind:  255,
		}},
	}
	got, err := record.Apply(cache)
	if err == nil {
		t.Fatal("expected invalid update error")
	}
	if got.FieldCount() != cache.FieldCount() || string(got.Bytes()) != string(cache.Bytes()) {
		t.Fatalf("invalid apply changed cache: got fields=%d bytes=%q, want fields=%d bytes=%q", got.FieldCount(), got.Bytes(), cache.FieldCount(), cache.Bytes())
	}
}
