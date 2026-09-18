package hatDataStructure

import (
	"bytes"
	"errors"
	"testing"
)

func TestVersionedTupleEnforcesFormatVersion(t *testing.T) {
	formatV1 := mustTR020TupleFormat(t, 7)
	record, err := NewVersionedTuple(formatV1, []TupleFieldValue{
		TupleInt64(42),
		TupleString("orders"),
		TupleNull(),
	})
	if err != nil {
		t.Fatalf("NewVersionedTuple() error = %v", err)
	}
	if record.Version() != 7 {
		t.Fatalf("Version() = %d, want 7", record.Version())
	}
	if err := record.Validate(formatV1); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	formatV2 := mustTR020TupleFormat(t, 8)
	if err := record.Validate(formatV2); !errors.Is(err, ErrVersionedTupleVersionMismatch) {
		t.Fatalf("Validate() with wrong version error = %v, want %v", err, ErrVersionedTupleVersionMismatch)
	}

	legacy, err := formatV1.Pack([]TupleFieldValue{TupleInt64(42), TupleString("orders"), TupleNull()})
	if err != nil {
		t.Fatalf("Pack() error = %v", err)
	}
	wrapped, err := NewVersionedTupleFromCache(formatV1, legacy)
	if err != nil {
		t.Fatalf("NewVersionedTupleFromCache() error = %v", err)
	}
	if err := wrapped.Validate(formatV1); err != nil {
		t.Fatalf("wrapped Validate() error = %v", err)
	}
}

func TestVersionedTupleWireRoundTripPreservesNullAndEmptyFields(t *testing.T) {
	format := mustTR020TupleFormat(t, 11)
	want, err := NewVersionedTuple(format, []TupleFieldValue{
		TupleInt64(-9),
		TupleString(""),
		TupleBytes([]byte("payload")),
	})
	if err != nil {
		t.Fatalf("NewVersionedTuple() error = %v", err)
	}

	encoded, err := MarshalVersionedTuple(want)
	if err != nil {
		t.Fatalf("MarshalVersionedTuple() error = %v", err)
	}
	got, err := UnmarshalVersionedTuple(encoded)
	if err != nil {
		t.Fatalf("UnmarshalVersionedTuple() error = %v", err)
	}
	if err := got.Validate(format); err != nil {
		t.Fatalf("decoded Validate() error = %v", err)
	}
	if got.Version() != want.Version() {
		t.Fatalf("decoded version = %d, want %d", got.Version(), want.Version())
	}
	if !bytes.Equal(got.Tuple().Bytes(), want.Tuple().Bytes()) {
		t.Fatalf("decoded bytes = %q, want %q", got.Tuple().Bytes(), want.Tuple().Bytes())
	}
	for index, wantValid := range []bool{true, true, true} {
		valid, err := got.Tuple().FieldValid(index)
		if err != nil {
			t.Fatalf("FieldValid(%d) error = %v", index, err)
		}
		if valid != wantValid {
			t.Fatalf("FieldValid(%d) = %v, want %v", index, valid, wantValid)
		}
	}

	withNull, err := NewVersionedTuple(format, []TupleFieldValue{TupleInt64(1), TupleString(""), TupleNull()})
	if err != nil {
		t.Fatalf("NewVersionedTuple(null) error = %v", err)
	}
	encoded, err = MarshalVersionedTuple(withNull)
	if err != nil {
		t.Fatalf("MarshalVersionedTuple(null) error = %v", err)
	}
	decodedNull, err := UnmarshalVersionedTuple(encoded)
	if err != nil {
		t.Fatalf("UnmarshalVersionedTuple(null) error = %v", err)
	}
	valid, err := decodedNull.Tuple().FieldValid(2)
	if err != nil {
		t.Fatalf("FieldValid(null) error = %v", err)
	}
	if valid {
		t.Fatal("decoded NULL field is marked valid")
	}
}

func TestVersionedTupleRejectsMalformedWire(t *testing.T) {
	tests := []struct {
		name string
		data []byte
		want error
	}{
		{name: "short", data: []byte("HTV"), want: ErrVersionedTupleWire},
		{name: "magic", data: []byte("BAD!\x01\x01\x00"), want: ErrVersionedTupleWire},
		{name: "zero version", data: []byte("HTV1\x01\x00\x00"), want: ErrVersionedTupleInvalid},
		{name: "truncated field", data: []byte("HTV1\x01\x01\x01\x03a"), want: ErrVersionedTupleWire},
		{name: "trailing bytes", data: []byte("HTV1\x01\x01\x00x"), want: ErrVersionedTupleWire},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := UnmarshalVersionedTuple(test.data)
			if !errors.Is(err, test.want) {
				t.Fatalf("UnmarshalVersionedTuple() error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestVersionedTupleUpdatesPreserveVersionAndValidate(t *testing.T) {
	format := mustTR020TupleFormat(t, 13)
	record, err := NewVersionedTuple(format, []TupleFieldValue{TupleInt64(1), TupleString("before"), TupleNull()})
	if err != nil {
		t.Fatalf("NewVersionedTuple() error = %v", err)
	}
	updated, err := record.ApplyUpdates(format, []TupleFieldUpdate{{Index: 1, Kind: TupleFieldSet, Value: []byte("after")}})
	if err != nil {
		t.Fatalf("ApplyUpdates() error = %v", err)
	}
	if updated.Version() != record.Version() {
		t.Fatalf("updated version = %d, want %d", updated.Version(), record.Version())
	}
	if err := updated.Validate(format); err != nil {
		t.Fatalf("updated Validate() error = %v", err)
	}
	field, err := updated.Tuple().Field(1)
	if err != nil {
		t.Fatalf("updated Field(1) error = %v", err)
	}
	if string(field) != "after" {
		t.Fatalf("updated field = %q, want after", field)
	}
}

func mustTR020TupleFormat(t *testing.T, version uint64) TupleFormat {
	t.Helper()
	format, err := NewTupleFormat(version, []TupleFieldSpec{
		{Name: "id", Type: TupleFieldInt64},
		{Name: "name", Type: TupleFieldString},
		{Name: "payload", Type: TupleFieldBytes, Nullable: true},
	})
	if err != nil {
		t.Fatalf("NewTupleFormat() error = %v", err)
	}
	return format
}
