package hatDataStructure

import (
	"reflect"
	"testing"
)

func TestTupleFormatFieldsIntoMatchesFieldsAndReusesDestination(t *testing.T) {
	defaultValue := TupleBytes([]byte("default"))
	format, err := NewTupleFormat(1, []TupleFieldSpec{
		{Name: "id", Type: TupleFieldInt64},
		{Name: "payload", Type: TupleFieldBytes, Default: &defaultValue},
		{Name: "optional", Type: TupleFieldString, Nullable: true},
	})
	if err != nil {
		t.Fatal(err)
	}

	destination := make([]TupleFieldSpec, 0, format.FieldCount())
	backing := &destination[:cap(destination)][0]
	got := format.FieldsInto(destination)
	want := format.Fields()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("FieldsInto() = %#v, want %#v", got, want)
	}
	if &got[:cap(got)][0] != backing {
		t.Fatal("FieldsInto() did not reuse the destination backing array")
	}

	got[1].Default.Bytes[0] = 'x'
	if format.Fields()[1].Default.Bytes[0] != 'd' {
		t.Fatal("FieldsInto() exposed the format's default value storage")
	}

	got = format.FieldsInto(append(got, TupleFieldSpec{Name: "stale"}))
	if len(got) != format.FieldCount() || got[2].Default != nil {
		t.Fatalf("FieldsInto() did not clear stale slots: %#v", got)
	}
}
