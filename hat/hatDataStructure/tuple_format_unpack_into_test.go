package hatDataStructure

import (
	"reflect"
	"testing"
	"time"
)

func TestTupleFormatUnpackIntoMatchesUnpackAndReusesDestination(t *testing.T) {
	format, err := NewTupleFormat(1, []TupleFieldSpec{
		{Name: "name", Type: TupleFieldString},
		{Name: "payload", Type: TupleFieldBytes},
		{Name: "count", Type: TupleFieldInt64},
		{Name: "optional", Type: TupleFieldString, Nullable: true},
		{Name: "at", Type: TupleFieldTimestamp},
	})
	if err != nil {
		t.Fatal(err)
	}
	first, err := format.Pack([]TupleFieldValue{
		TupleString("alpha"),
		TupleBytes([]byte{1, 2, 3}),
		TupleInt64(7),
		TupleNull(),
		TupleTimestamp(time.Unix(123, 456).UTC()),
	})
	if err != nil {
		t.Fatal(err)
	}

	destination := make([]TupleFieldValue, 0, format.FieldCount())
	backing := &destination[:cap(destination)][0]
	got, err := format.UnpackInto(first, destination)
	if err != nil {
		t.Fatal(err)
	}
	want, err := format.Unpack(first)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("UnpackInto() = %#v, want %#v", got, want)
	}
	if &got[:cap(got)][0] != backing {
		t.Fatal("UnpackInto() did not reuse the destination backing array")
	}

	second, err := format.Pack([]TupleFieldValue{
		TupleString("beta"),
		TupleBytes([]byte{9}),
		TupleInt64(8),
		TupleString("present"),
		TupleTimestamp(time.Unix(456, 789).UTC()),
	})
	if err != nil {
		t.Fatal(err)
	}
	got, err = format.UnpackInto(second, got)
	if err != nil {
		t.Fatal(err)
	}
	want, err = format.Unpack(second)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("UnpackInto() after reuse = %#v, want %#v", got, want)
	}

	if empty, err := format.UnpackInto(first, got[:0]); err != nil || len(empty) != format.FieldCount() {
		t.Fatalf("UnpackInto() with zero-length reusable destination = len %d, err %v", len(empty), err)
	}

	if values, err := format.Unpack(TupleFieldOffsetCache{}); err == nil || values != nil {
		t.Fatalf("Unpack(invalid) = %#v/%v, want nil and error", values, err)
	}
	if values, err := format.UnpackInto(TupleFieldOffsetCache{}, got); err == nil || len(values) != 0 {
		t.Fatalf("UnpackInto(invalid) = len %d/%v, want empty slice and error", len(values), err)
	}
}
