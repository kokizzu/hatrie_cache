package hatDataStructure_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"hatrie_cache/hat/hatDataStructure"
)

func TestTupleFormatPacksDefaultsAndGeneratedFields(t *testing.T) {
	format, err := hatDataStructure.NewTupleFormat(1, []hatDataStructure.TupleFieldSpec{
		{Name: "id", Type: hatDataStructure.TupleFieldInt64},
		{Name: "region", Type: hatDataStructure.TupleFieldString, Default: tupleValuePtr(hatDataStructure.TupleString("sg"))},
		{Name: "slug", Type: hatDataStructure.TupleFieldString, Generated: func(values []hatDataStructure.TupleFieldValue) (hatDataStructure.TupleFieldValue, error) {
			return hatDataStructure.TupleString(fmt.Sprintf("%s-%d", values[1].String, values[0].Int64)), nil
		}},
	})
	if err != nil {
		t.Fatalf("NewTupleFormat() error = %v", err)
	}
	packed, err := format.Pack([]hatDataStructure.TupleFieldValue{hatDataStructure.TupleInt64(7)})
	if err != nil {
		t.Fatalf("Pack() error = %v", err)
	}
	values, err := format.Unpack(packed)
	if err != nil {
		t.Fatalf("Unpack() error = %v", err)
	}
	if len(values) != 3 || values[0].Int64 != 7 || values[1].String != "sg" || values[2].String != "sg-7" {
		t.Fatalf("unpacked values = %#v", values)
	}
}

func TestTupleFormatHandlesNullabilityAndRejectsTypeShapeErrors(t *testing.T) {
	format, err := hatDataStructure.NewTupleFormat(2, []hatDataStructure.TupleFieldSpec{
		{Name: "required", Type: hatDataStructure.TupleFieldInt64},
		{Name: "optional", Type: hatDataStructure.TupleFieldString, Nullable: true},
	})
	if err != nil {
		t.Fatalf("NewTupleFormat() error = %v", err)
	}
	packed, err := format.Pack([]hatDataStructure.TupleFieldValue{hatDataStructure.TupleInt64(1)})
	if err != nil {
		t.Fatalf("Pack() with omitted nullable field error = %v", err)
	}
	values, err := format.Unpack(packed)
	if err != nil || len(values) != 2 || values[1].Valid {
		t.Fatalf("nullable unpack = %#v/%v", values, err)
	}

	for _, values := range [][]hatDataStructure.TupleFieldValue{
		{hatDataStructure.TupleString("wrong")},
		{},
		{hatDataStructure.TupleInt64(1), hatDataStructure.TupleString("ok"), hatDataStructure.TupleBool(true)},
	} {
		if _, err := format.Pack(values); err == nil {
			t.Fatalf("Pack(%#v) error = nil, want shape/type rejection", values)
		}
	}
}

func TestTupleFormatValidatesExistingPackedBytesWithoutCopying(t *testing.T) {
	format, err := hatDataStructure.NewTupleFormat(3, []hatDataStructure.TupleFieldSpec{
		{Name: "count", Type: hatDataStructure.TupleFieldInt64},
		{Name: "when", Type: hatDataStructure.TupleFieldTimestamp},
		{Name: "payload", Type: hatDataStructure.TupleFieldBytes, Nullable: true},
	})
	if err != nil {
		t.Fatalf("NewTupleFormat() error = %v", err)
	}
	packed, err := format.Pack([]hatDataStructure.TupleFieldValue{
		hatDataStructure.TupleInt64(4),
		hatDataStructure.TupleTimestamp(time.Unix(10, 20)),
		hatDataStructure.TupleBytes([]byte("payload")),
	})
	if err != nil {
		t.Fatalf("Pack() error = %v", err)
	}
	if err := format.Validate(packed); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	data := packed.Bytes()
	if len(data) == 0 {
		t.Fatal("packed tuple is empty")
	}
	data[0] ^= 0xff
	values, err := format.Unpack(packed)
	if err != nil || values[0].Int64 == 4 {
		t.Fatalf("corrupted tuple unpack = %#v/%v, want changed valid int", values, err)
	}
	malformed, err := hatDataStructure.NewPackedTuple([][]byte{{1}, make([]byte, 8), nil})
	if err != nil {
		t.Fatalf("NewPackedTuple(malformed) error = %v", err)
	}
	if err := format.Validate(malformed); err == nil {
		t.Fatal("Validate() error = nil for malformed fixed-width tuple")
	}
}

func TestTupleFormatCopiesDefaultsAndDoesNotMutateInput(t *testing.T) {
	defaultBytes := []byte("default")
	format, err := hatDataStructure.NewTupleFormat(1, []hatDataStructure.TupleFieldSpec{
		{Name: "payload", Type: hatDataStructure.TupleFieldBytes, Default: tupleValuePtr(hatDataStructure.TupleBytes(defaultBytes))},
	})
	if err != nil {
		t.Fatalf("NewTupleFormat() error = %v", err)
	}
	defaultBytes[0] = 'x'
	packed, err := format.Pack(nil)
	if err != nil {
		t.Fatalf("Pack() error = %v", err)
	}
	values, err := format.Unpack(packed)
	if err != nil || string(values[0].Bytes) != "default" {
		t.Fatalf("copied default = %#v/%v", values, err)
	}
	values[0].Bytes[0] = 'x'
	packedAgain, err := format.Pack(nil)
	if err != nil {
		t.Fatalf("second Pack() error = %v", err)
	}
	valuesAgain, err := format.Unpack(packedAgain)
	if err != nil || string(valuesAgain[0].Bytes) != "default" {
		t.Fatalf("format default changed through unpack = %#v/%v", valuesAgain, err)
	}
}

func TestTupleFormatRoundTripsEmptyValuesAndTemporalTypes(t *testing.T) {
	format, err := hatDataStructure.NewTupleFormat(4, []hatDataStructure.TupleFieldSpec{
		{Name: "empty_string", Type: hatDataStructure.TupleFieldString, Nullable: true},
		{Name: "empty_bytes", Type: hatDataStructure.TupleFieldBytes, Nullable: true},
		{Name: "date", Type: hatDataStructure.TupleFieldDate},
		{Name: "timestamp", Type: hatDataStructure.TupleFieldTimestamp},
	})
	if err != nil {
		t.Fatalf("NewTupleFormat() error = %v", err)
	}
	date := time.Date(2024, time.March, 8, 15, 4, 3, 0, time.FixedZone("test", 3600))
	timestamp := time.Unix(1709904243, 123).UTC()
	packed, err := format.Pack([]hatDataStructure.TupleFieldValue{
		hatDataStructure.TupleString(""),
		hatDataStructure.TupleBytes([]byte{}),
		hatDataStructure.TupleDate(date),
		hatDataStructure.TupleTimestamp(timestamp),
	})
	if err != nil {
		t.Fatalf("Pack() error = %v", err)
	}
	values, err := format.Unpack(packed)
	if err != nil {
		t.Fatalf("Unpack() error = %v", err)
	}
	if !values[0].Valid || !values[1].Valid || values[0].String != "" || len(values[1].Bytes) != 0 {
		t.Fatalf("empty values = %#v, want valid empty values", values)
	}
	wantDate := time.Date(2024, time.March, 8, 0, 0, 0, 0, time.UTC)
	if !values[2].Time.Equal(wantDate) || !values[3].Time.Equal(timestamp) {
		t.Fatalf("temporal values = %#v, want %s and %s", values, wantDate, timestamp)
	}
}

func BenchmarkTupleFormatPack(b *testing.B) {
	format, err := hatDataStructure.NewTupleFormat(1, []hatDataStructure.TupleFieldSpec{
		{Name: "id", Type: hatDataStructure.TupleFieldInt64},
		{Name: "name", Type: hatDataStructure.TupleFieldString},
		{Name: "active", Type: hatDataStructure.TupleFieldBool},
		{Name: "payload", Type: hatDataStructure.TupleFieldBytes},
	})
	if err != nil {
		b.Fatal(err)
	}
	values := []hatDataStructure.TupleFieldValue{
		hatDataStructure.TupleInt64(7),
		hatDataStructure.TupleString(strings.Repeat("name", 4)),
		hatDataStructure.TupleBool(true),
		hatDataStructure.TupleBytes([]byte(strings.Repeat("x", 32))),
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := format.Pack(values); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkNewPackedTupleBaseline(b *testing.B) {
	fields := [][]byte{
		make([]byte, 8),
		[]byte(strings.Repeat("name", 4)),
		{1},
		[]byte(strings.Repeat("x", 32)),
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := hatDataStructure.NewPackedTuple(fields); err != nil {
			b.Fatal(err)
		}
	}
}

func tupleValuePtr(value hatDataStructure.TupleFieldValue) *hatDataStructure.TupleFieldValue {
	return &value
}
