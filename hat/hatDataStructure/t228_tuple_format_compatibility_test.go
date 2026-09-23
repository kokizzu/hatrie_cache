package hatDataStructure_test

import (
	"errors"
	"fmt"
	"testing"

	"hatrie_cache/hat/hatDataStructure"
)

func TestT228CompatibleReaderFillsMissingTrailingFields(t *testing.T) {
	source := mustT228Format(t, 1, []hatDataStructure.TupleFieldSpec{
		{Name: "id", Type: hatDataStructure.TupleFieldUint64},
		{Name: "name", Type: hatDataStructure.TupleFieldString},
	})
	target := mustT228Format(t, 2, []hatDataStructure.TupleFieldSpec{
		{Name: "id", Type: hatDataStructure.TupleFieldUint64},
		{Name: "name", Type: hatDataStructure.TupleFieldString},
		{Name: "region", Type: hatDataStructure.TupleFieldString, Default: t228ValuePtr(hatDataStructure.TupleString("sg"))},
		{Name: "slug", Type: hatDataStructure.TupleFieldString, Generated: func(values []hatDataStructure.TupleFieldValue) (hatDataStructure.TupleFieldValue, error) {
			return hatDataStructure.TupleString(fmt.Sprintf("%s-%s", values[1].String, values[2].String)), nil
		}},
		{Name: "active", Type: hatDataStructure.TupleFieldBool, Nullable: true},
	})
	reader, err := target.ReaderFor(source)
	if err != nil {
		t.Fatalf("ReaderFor() error = %v", err)
	}
	tuple, err := source.Pack([]hatDataStructure.TupleFieldValue{
		hatDataStructure.TupleUint64(42),
		hatDataStructure.TupleString("orders"),
	})
	if err != nil {
		t.Fatalf("source.Pack() error = %v", err)
	}
	values, err := reader.Unpack(tuple)
	if err != nil {
		t.Fatalf("reader.Unpack() error = %v", err)
	}
	if len(values) != 5 || values[0].Uint64 != 42 || values[1].String != "orders" || values[2].String != "sg" || values[3].String != "orders-sg" {
		t.Fatalf("compatible values = %#v", values)
	}
	if values[4].Valid {
		t.Fatalf("missing nullable field = %#v, want NULL", values[4])
	}
}

func TestT228CompatibleReaderIgnoresTrailingSourceFields(t *testing.T) {
	source := mustT228Format(t, 2, []hatDataStructure.TupleFieldSpec{
		{Name: "id", Type: hatDataStructure.TupleFieldUint64},
		{Name: "name", Type: hatDataStructure.TupleFieldString},
		{Name: "region", Type: hatDataStructure.TupleFieldString},
	})
	target := mustT228Format(t, 1, []hatDataStructure.TupleFieldSpec{
		{Name: "id", Type: hatDataStructure.TupleFieldUint64},
		{Name: "name", Type: hatDataStructure.TupleFieldString},
	})
	reader, err := hatDataStructure.NewTupleFormatReader(source, target)
	if err != nil {
		t.Fatalf("NewTupleFormatReader() error = %v", err)
	}
	versioned, err := source.PackVersioned([]hatDataStructure.TupleFieldValue{
		hatDataStructure.TupleUint64(7),
		hatDataStructure.TupleString("orders"),
		hatDataStructure.TupleString("sg"),
	})
	if err != nil {
		t.Fatalf("PackVersioned() error = %v", err)
	}
	values, err := reader.UnpackVersioned(versioned)
	if err != nil {
		t.Fatalf("reader.UnpackVersioned() error = %v", err)
	}
	if len(values) != 2 || values[0].Uint64 != 7 || values[1].String != "orders" {
		t.Fatalf("forward-compatible values = %#v", values)
	}
}

func TestT228CompatibleReaderRejectsUnsafeSchemaChanges(t *testing.T) {
	tests := []struct {
		name   string
		source []hatDataStructure.TupleFieldSpec
		target []hatDataStructure.TupleFieldSpec
	}{
		{
			name: "type change",
			source: []hatDataStructure.TupleFieldSpec{
				{Name: "id", Type: hatDataStructure.TupleFieldUint64},
			},
			target: []hatDataStructure.TupleFieldSpec{
				{Name: "id", Type: hatDataStructure.TupleFieldString},
			},
		},
		{
			name: "rename",
			source: []hatDataStructure.TupleFieldSpec{
				{Name: "id", Type: hatDataStructure.TupleFieldUint64},
			},
			target: []hatDataStructure.TupleFieldSpec{
				{Name: "key", Type: hatDataStructure.TupleFieldUint64},
			},
		},
		{
			name: "reorder",
			source: []hatDataStructure.TupleFieldSpec{
				{Name: "id", Type: hatDataStructure.TupleFieldUint64},
				{Name: "name", Type: hatDataStructure.TupleFieldString},
			},
			target: []hatDataStructure.TupleFieldSpec{
				{Name: "name", Type: hatDataStructure.TupleFieldString},
				{Name: "id", Type: hatDataStructure.TupleFieldUint64},
			},
		},
		{
			name: "nullable to required",
			source: []hatDataStructure.TupleFieldSpec{
				{Name: "name", Type: hatDataStructure.TupleFieldString, Nullable: true},
			},
			target: []hatDataStructure.TupleFieldSpec{
				{Name: "name", Type: hatDataStructure.TupleFieldString},
			},
		},
		{
			name: "missing required tail",
			source: []hatDataStructure.TupleFieldSpec{
				{Name: "id", Type: hatDataStructure.TupleFieldUint64},
			},
			target: []hatDataStructure.TupleFieldSpec{
				{Name: "id", Type: hatDataStructure.TupleFieldUint64},
				{Name: "name", Type: hatDataStructure.TupleFieldString},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			source := mustT228Format(t, 1, test.source)
			target := mustT228Format(t, 2, test.target)
			_, err := hatDataStructure.NewTupleFormatReader(source, target)
			if !errors.Is(err, hatDataStructure.ErrTupleFormatIncompatibleReader) {
				t.Fatalf("NewTupleFormatReader() error = %v, want %v", err, hatDataStructure.ErrTupleFormatIncompatibleReader)
			}
		})
	}
}

func TestT228CompatibleReaderRejectsWrongTupleCountAndVersion(t *testing.T) {
	source := mustT228Format(t, 1, []hatDataStructure.TupleFieldSpec{
		{Name: "id", Type: hatDataStructure.TupleFieldUint64},
		{Name: "name", Type: hatDataStructure.TupleFieldString},
	})
	target := mustT228Format(t, 2, source.Fields())
	reader, err := hatDataStructure.NewTupleFormatReader(source, target)
	if err != nil {
		t.Fatalf("NewTupleFormatReader() error = %v", err)
	}
	shortTuple, err := hatDataStructure.NewPackedTuple([][]byte{make([]byte, 8)})
	if err != nil {
		t.Fatalf("NewPackedTuple() error = %v", err)
	}
	if _, err := reader.Unpack(shortTuple); !errors.Is(err, hatDataStructure.ErrTupleFormatReaderTupleCount) {
		t.Fatalf("wrong tuple count error = %v, want %v", err, hatDataStructure.ErrTupleFormatReaderTupleCount)
	}
	wrongVersionFormat := mustT228Format(t, 3, source.Fields())
	wrongVersionTuple, err := wrongVersionFormat.PackVersioned([]hatDataStructure.TupleFieldValue{
		hatDataStructure.TupleUint64(42),
		hatDataStructure.TupleString("orders"),
	})
	if err != nil {
		t.Fatalf("wrongVersionFormat.PackVersioned() error = %v", err)
	}
	if _, err := reader.UnpackVersioned(wrongVersionTuple); !errors.Is(err, hatDataStructure.ErrVersionedTupleVersionMismatch) {
		t.Fatalf("wrong version error = %v, want %v", err, hatDataStructure.ErrVersionedTupleVersionMismatch)
	}
}

func TestT228CompatibleReaderPreservesSourceValidation(t *testing.T) {
	source := mustT228Format(t, 1, []hatDataStructure.TupleFieldSpec{
		{Name: "id", Type: hatDataStructure.TupleFieldUint64},
	})
	target := mustT228Format(t, 2, source.Fields())
	reader, err := target.ReaderFor(source)
	if err != nil {
		t.Fatalf("ReaderFor() error = %v", err)
	}
	malformed, err := hatDataStructure.NewPackedTuple([][]byte{{1}})
	if err != nil {
		t.Fatalf("NewPackedTuple() error = %v", err)
	}
	if _, err := reader.Unpack(malformed); err == nil {
		t.Fatal("reader.Unpack() error = nil for malformed source field")
	}
}

func BenchmarkT228TupleFormatReaderUnpack(b *testing.B) {
	source := mustT228BenchmarkFormat(b, 1, []hatDataStructure.TupleFieldSpec{
		{Name: "id", Type: hatDataStructure.TupleFieldUint64},
		{Name: "name", Type: hatDataStructure.TupleFieldString},
	})
	target := mustT228BenchmarkFormat(b, 2, []hatDataStructure.TupleFieldSpec{
		{Name: "id", Type: hatDataStructure.TupleFieldUint64},
		{Name: "name", Type: hatDataStructure.TupleFieldString},
		{Name: "region", Type: hatDataStructure.TupleFieldString, Default: t228ValuePtr(hatDataStructure.TupleString("sg"))},
		{Name: "active", Type: hatDataStructure.TupleFieldBool, Nullable: true},
	})
	reader, err := target.ReaderFor(source)
	if err != nil {
		b.Fatal(err)
	}
	tuple, err := source.Pack([]hatDataStructure.TupleFieldValue{
		hatDataStructure.TupleUint64(42),
		hatDataStructure.TupleString("orders"),
	})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		values, err := reader.Unpack(tuple)
		if err != nil {
			b.Fatal(err)
		}
		t228ExternalTupleReaderBenchmarkSink = values
	}
}

func BenchmarkT228TupleFormatReaderExactUnpack(b *testing.B) {
	format := mustT228BenchmarkFormat(b, 1, []hatDataStructure.TupleFieldSpec{
		{Name: "id", Type: hatDataStructure.TupleFieldUint64},
		{Name: "name", Type: hatDataStructure.TupleFieldString},
		{Name: "region", Type: hatDataStructure.TupleFieldString},
		{Name: "active", Type: hatDataStructure.TupleFieldBool},
	})
	reader, err := format.ReaderFor(format)
	if err != nil {
		b.Fatal(err)
	}
	tuple, err := format.Pack([]hatDataStructure.TupleFieldValue{
		hatDataStructure.TupleUint64(42),
		hatDataStructure.TupleString("orders"),
		hatDataStructure.TupleString("sg"),
		hatDataStructure.TupleBool(true),
	})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		values, err := reader.Unpack(tuple)
		if err != nil {
			b.Fatal(err)
		}
		t228ExternalTupleReaderBenchmarkSink = values
	}
}

var t228ExternalTupleReaderBenchmarkSink []hatDataStructure.TupleFieldValue

func mustT228Format(t *testing.T, version uint64, fields []hatDataStructure.TupleFieldSpec) hatDataStructure.TupleFormat {
	t.Helper()
	format, err := hatDataStructure.NewTupleFormat(version, fields)
	if err != nil {
		t.Fatalf("NewTupleFormat() error = %v", err)
	}
	return format
}

func mustT228BenchmarkFormat(b testing.TB, version uint64, fields []hatDataStructure.TupleFieldSpec) hatDataStructure.TupleFormat {
	b.Helper()
	format, err := hatDataStructure.NewTupleFormat(version, fields)
	if err != nil {
		b.Fatalf("NewTupleFormat() error = %v", err)
	}
	return format
}

func t228ValuePtr(value hatDataStructure.TupleFieldValue) *hatDataStructure.TupleFieldValue {
	return &value
}
