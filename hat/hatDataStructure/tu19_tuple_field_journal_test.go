package hatDataStructure

import (
	"bytes"
	"errors"
	"testing"
)

func TestTU19VersionedTupleApplyFieldUpdatesRetainsVersion(t *testing.T) {
	format, err := NewTupleFormat(17, []TupleFieldSpec{
		{Name: "count", Type: TupleFieldInt64},
		{Name: "name", Type: TupleFieldString},
		{Name: "payload", Type: TupleFieldBytes},
	})
	if err != nil {
		t.Fatalf("NewTupleFormat() error = %v", err)
	}
	original, err := format.PackVersioned([]TupleFieldValue{
		TupleInt64(10),
		TupleString("before"),
		TupleBytes([]byte("abcd")),
	})
	if err != nil {
		t.Fatalf("PackVersioned() error = %v", err)
	}
	want, err := format.PackVersioned([]TupleFieldValue{
		TupleInt64(15),
		TupleString("after"),
		TupleBytes([]byte("aXYd")),
	})
	if err != nil {
		t.Fatalf("PackVersioned(want) error = %v", err)
	}

	updated, err := original.ApplyFieldUpdates([]TupleFieldUpdate{
		{Index: 0, Kind: TupleFieldAddInt64, Delta: 5},
		{Index: 1, Kind: TupleFieldSet, Value: []byte("after")},
		{Index: 2, Kind: TupleFieldSplice, Start: 1, Remove: 2, Insert: []byte("XY")},
	})
	if err != nil {
		t.Fatalf("ApplyFieldUpdates() error = %v", err)
	}
	if updated.Version() != original.Version() {
		t.Fatalf("updated version = %d, want %d", updated.Version(), original.Version())
	}
	updatedBytes, err := MarshalVersionedTuple(updated)
	if err != nil {
		t.Fatalf("MarshalVersionedTuple(updated) error = %v", err)
	}
	wantBytes, err := MarshalVersionedTuple(want)
	if err != nil {
		t.Fatalf("MarshalVersionedTuple(want) error = %v", err)
	}
	if !bytes.Equal(updatedBytes, wantBytes) {
		t.Fatalf("updated tuple bytes = %x, want %x", updatedBytes, wantBytes)
	}
}

func TestTU19VersionedTupleApplyFieldUpdatesRejectsInvalidBatch(t *testing.T) {
	format, err := NewTupleFormat(19, []TupleFieldSpec{{Name: "count", Type: TupleFieldInt64}})
	if err != nil {
		t.Fatalf("NewTupleFormat() error = %v", err)
	}
	original, err := format.PackVersioned([]TupleFieldValue{TupleInt64(3)})
	if err != nil {
		t.Fatalf("PackVersioned() error = %v", err)
	}
	before, err := MarshalVersionedTuple(original)
	if err != nil {
		t.Fatalf("MarshalVersionedTuple(before) error = %v", err)
	}
	if _, err := original.ApplyFieldUpdates([]TupleFieldUpdate{{Index: 4, Kind: TupleFieldAddInt64, Delta: 1}}); !errors.Is(err, ErrTupleFieldUpdateIndex) {
		t.Fatalf("ApplyFieldUpdates() error = %v, want %v", err, ErrTupleFieldUpdateIndex)
	}
	after, err := MarshalVersionedTuple(original)
	if err != nil {
		t.Fatalf("MarshalVersionedTuple(after) error = %v", err)
	}
	if !bytes.Equal(after, before) {
		t.Fatalf("invalid update changed tuple: before=%x after=%x", before, after)
	}
}
