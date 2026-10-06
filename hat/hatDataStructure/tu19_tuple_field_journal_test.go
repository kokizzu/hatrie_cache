package hatDataStructure

import (
	"bytes"
	"encoding/binary"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestTU19TupleFieldJournalRoundTripsAndReplaysAllOperations(t *testing.T) {
	cache, err := NewPackedTuple([][]byte{[]byte("before"), []byte("abcd"), make([]byte, 8)})
	if err != nil {
		t.Fatal(err)
	}
	record := TupleFieldUpdateJournalRecord{
		Sequence:      42,
		FormatVersion: 7,
		Updates: []TupleFieldUpdate{
			{Index: 0, Kind: TupleFieldSet, Value: []byte("after")},
			{Index: 1, Kind: TupleFieldSplice, Start: 1, Remove: 2, Insert: []byte("XYZ")},
			{Index: 2, Kind: TupleFieldAddInt64, Delta: 2},
		},
	}
	encoded, err := EncodeTupleFieldUpdateJournalRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	second, err := EncodeTupleFieldUpdateJournalRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(encoded, second) {
		t.Fatal("encoding is not deterministic")
	}
	decoded, err := DecodeTupleFieldUpdateJournalRecord(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded, record) {
		t.Fatalf("decoded record = %#v, want %#v", decoded, record)
	}
	updated, err := ApplyTupleFieldUpdateJournalRecord(cache, decoded, 7)
	if err != nil {
		t.Fatal(err)
	}
	fields := [][]byte{[]byte("after"), []byte("aXYZd"), make([]byte, 8)}
	binary.BigEndian.PutUint64(fields[2], uint64(2))
	for index, want := range fields {
		got, err := updated.Field(index)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("field %d = %q, want %q", index, got, want)
		}
	}
	record.Updates[0].Value[0] = 'X'
	if decoded.Updates[0].Value[0] != 'a' {
		t.Fatal("decoded payload aliases the input record")
	}
}

func TestTU19TupleFieldJournalRejectsCorruptionAndVersionMismatch(t *testing.T) {
	record := TupleFieldUpdateJournalRecord{
		Sequence:      1,
		FormatVersion: 3,
		Updates:       []TupleFieldUpdate{{Index: 0, Kind: TupleFieldSet, Value: []byte("x")}},
	}
	encoded, err := EncodeTupleFieldUpdateJournalRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	corrupt := append([]byte(nil), encoded...)
	corrupt[len(corrupt)-1] ^= 1
	if _, err := DecodeTupleFieldUpdateJournalRecord(corrupt); !errors.Is(err, ErrTupleFieldUpdateJournalCorrupt) {
		t.Fatalf("corrupt decode error = %v, want corruption", err)
	}
	cache, err := NewPackedTuple([][]byte{[]byte("old")})
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeTupleFieldUpdateJournalRecord(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ApplyTupleFieldUpdateJournalRecord(cache, decoded, 4); !errors.Is(err, ErrTupleFieldUpdateJournalVersion) {
		t.Fatalf("version error = %v, want version mismatch", err)
	}
}

func TestTU19TupleFieldJournalReplayIsAtomicAndBounded(t *testing.T) {
	cache, err := NewPackedTuple([][]byte{[]byte("short")})
	if err != nil {
		t.Fatal(err)
	}
	before := cache.Clone()
	record := TupleFieldUpdateJournalRecord{
		Sequence:      1,
		FormatVersion: 1,
		Updates:       []TupleFieldUpdate{{Index: 0, Kind: TupleFieldSplice, Start: 99, Remove: 1, Insert: []byte("x")}},
	}
	if _, err := ApplyTupleFieldUpdateJournalRecord(cache, record, 1); !errors.Is(err, ErrTupleFieldUpdateRange) {
		t.Fatalf("invalid replay error = %v, want range error", err)
	}
	if !bytes.Equal(cache.Bytes(), before.Bytes()) {
		t.Fatal("failed replay mutated the source cache")
	}
	invalid := []TupleFieldUpdateJournalRecord{
		{},
		{Sequence: 1, FormatVersion: 1, Updates: []TupleFieldUpdate{{Index: 0, Kind: TupleFieldSet, Value: []byte("a")}, {Index: 0, Kind: TupleFieldSet, Value: []byte("b")}}},
		{Sequence: 1, FormatVersion: 1, Updates: []TupleFieldUpdate{{Index: -1, Kind: TupleFieldSet}}},
		{Sequence: 1, FormatVersion: 1, Updates: []TupleFieldUpdate{{Index: 0, Kind: TupleFieldUpdateKind(99)}}},
		{Sequence: 1, FormatVersion: 1, Updates: []TupleFieldUpdate{{Index: 0, Kind: TupleFieldSet, Start: 1}}},
		{Sequence: 1, FormatVersion: 1, Updates: []TupleFieldUpdate{{Index: 0, Kind: TupleFieldSplice, Remove: -1}}},
		{Sequence: 1, FormatVersion: 1, Updates: []TupleFieldUpdate{{Index: 0, Kind: TupleFieldAddInt64, Value: []byte("x")}}},
	}
	for index, candidate := range invalid {
		if _, err := EncodeTupleFieldUpdateJournalRecord(candidate); !errors.Is(err, ErrTupleFieldUpdateJournalInvalid) {
			t.Fatalf("invalid record %d error = %v, want journal invalid", index, err)
		}
	}
	tooLarge := TupleFieldUpdateJournalRecord{
		Sequence:      1,
		FormatVersion: 1,
		Updates:       []TupleFieldUpdate{{Index: 0, Kind: TupleFieldSet, Value: []byte(strings.Repeat("x", MaxTupleFieldUpdateJournalPayloadBytes+1))}},
	}
	if _, err := EncodeTupleFieldUpdateJournalRecord(tooLarge); !errors.Is(err, ErrTupleFieldUpdateJournalLimit) {
		t.Fatalf("large record error = %v, want limit", err)
	}
}

func TestTU19TupleFieldJournalRejectsMalformedWire(t *testing.T) {
	for _, wire := range [][]byte{nil, []byte("HUF1"), []byte("HUF1\x01\x00")} {
		if _, err := DecodeTupleFieldUpdateJournalRecord(wire); !errors.Is(err, ErrTupleFieldUpdateJournalCorrupt) {
			t.Fatalf("wire %q error = %v, want corruption", wire, err)
		}
	}
	record := TupleFieldUpdateJournalRecord{Sequence: 1, FormatVersion: 1, Updates: []TupleFieldUpdate{{Index: 0, Kind: TupleFieldSet, Value: []byte("x")}}}
	encoded, err := EncodeTupleFieldUpdateJournalRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	withTrailing := append(append([]byte(nil), encoded...), 0)
	if _, err := DecodeTupleFieldUpdateJournalRecord(withTrailing); !errors.Is(err, ErrTupleFieldUpdateJournalCorrupt) {
		t.Fatalf("trailing wire error = %v, want corruption", err)
	}
}
