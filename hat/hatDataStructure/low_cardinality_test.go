package hatDataStructure

import (
	"errors"
	"reflect"
	"testing"
)

func TestLowCardinalityStringColumnBuildsSortedCodesAndPreservesNulls(t *testing.T) {
	builder := NewLowCardinalityStringBuilder(LowCardinalityStringBuilderOptions{InitialCapacity: 5})
	if _, err := builder.Append("zulu"); err != nil {
		t.Fatalf("Append(zulu) error = %v", err)
	}
	if err := builder.AppendNull(); err != nil {
		t.Fatalf("AppendNull() error = %v", err)
	}
	if _, err := builder.Append("alpha"); err != nil {
		t.Fatalf("Append(alpha) error = %v", err)
	}
	if _, err := builder.Append("alpha"); err != nil {
		t.Fatalf("Append(alpha duplicate) error = %v", err)
	}

	column, err := builder.Build()
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	if got, want := column.Len(), 4; got != want {
		t.Fatalf("Len() = %d, want %d", got, want)
	}
	if got, want := column.Cardinality(), 2; got != want {
		t.Fatalf("Cardinality() = %d, want %d", got, want)
	}
	if got, want := column.DictionaryValues(), []string{"alpha", "zulu"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("DictionaryValues() = %#v, want %#v", got, want)
	}
	for row, want := range []struct {
		value string
		valid bool
		code  uint32
	}{{"zulu", true, 1}, {"", false, 0}, {"alpha", true, 0}, {"alpha", true, 0}} {
		value, valid := column.ValueAt(row)
		if value != want.value || valid != want.valid {
			t.Fatalf("ValueAt(%d) = %q/%t, want %q/%t", row, value, valid, want.value, want.valid)
		}
		code, ok := column.CodeAt(row)
		if ok != want.valid || (ok && code != want.code) {
			t.Fatalf("CodeAt(%d) = %d/%t, want %d/%t", row, code, ok, want.code, want.valid)
		}
	}
	if code, ok := column.LookupCode("alpha"); !ok || code != 0 {
		t.Fatalf("LookupCode(alpha) = %d/%t, want 0/true", code, ok)
	}
	if _, ok := column.LookupCode("missing"); ok {
		t.Fatal("LookupCode(missing) unexpectedly found a value")
	}
	if got, want := column.CountCodes(), []uint64{2, 1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("CountCodes() = %#v, want %#v", got, want)
	}
	leftCode, leftValid := column.CodeAt(2)
	rightCode, rightValid := column.CodeAt(0)
	if !leftValid || !rightValid || leftCode >= rightCode {
		t.Fatalf("sorted codes do not preserve order: left=%d/%t right=%d/%t", leftCode, leftValid, rightCode, rightValid)
	}
	if _, err := builder.Append("after-build"); !errors.Is(err, ErrLowCardinalityStringBuilderBuilt) {
		t.Fatalf("Append() after Build() error = %v, want ErrLowCardinalityStringBuilderBuilt", err)
	}
}

func TestLowCardinalityStringBuilderEnforcesDistinctLimit(t *testing.T) {
	builder := NewLowCardinalityStringBuilder(LowCardinalityStringBuilderOptions{MaxDistinctValues: 2})
	for _, value := range []string{"a", "b"} {
		if _, err := builder.Append(value); err != nil {
			t.Fatalf("Append(%q) error = %v", value, err)
		}
	}
	if _, err := builder.Append("c"); !errors.Is(err, ErrLowCardinalityStringDistinctLimit) {
		t.Fatalf("Append(c) error = %v, want ErrLowCardinalityStringDistinctLimit", err)
	}
}

func TestLowCardinalityStringBuilderUsesConservativeDefaultLimit(t *testing.T) {
	builder := NewLowCardinalityStringBuilder(LowCardinalityStringBuilderOptions{})
	for value := 0; value < DefaultLowCardinalityStringMaxDistinctValues; value++ {
		if _, err := builder.Append(string([]byte{byte(value >> 8), byte(value)})); err != nil {
			t.Fatalf("Append(%d) error = %v", value, err)
		}
	}
	if _, err := builder.Append("overflow"); !errors.Is(err, ErrLowCardinalityStringDistinctLimit) {
		t.Fatalf("Append(overflow) error = %v, want ErrLowCardinalityStringDistinctLimit", err)
	}
}

func TestLowCardinalityStringColumnUsesSmallestCodeWidth(t *testing.T) {
	for _, test := range []struct {
		distinct int
		width    int
	}{
		{distinct: 1, width: 1},
		{distinct: 256, width: 1},
		{distinct: 257, width: 2},
	} {
		builder := NewLowCardinalityStringBuilder(LowCardinalityStringBuilderOptions{InitialCapacity: test.distinct, InitialRowCapacity: test.distinct})
		for value := 0; value < test.distinct; value++ {
			if _, err := builder.Append(string([]byte{byte(value >> 8), byte(value)})); err != nil {
				t.Fatalf("Append(%d) error = %v", value, err)
			}
		}
		column, err := builder.Build()
		if err != nil {
			t.Fatalf("Build(%d) error = %v", test.distinct, err)
		}
		if got := column.CodeWidth(); got != test.width {
			t.Fatalf("CodeWidth(%d) = %d, want %d", test.distinct, got, test.width)
		}
	}
}

func TestLowCardinalityStringColumnBinaryRoundTripAndRejectsCorruption(t *testing.T) {
	builder := NewLowCardinalityStringBuilder(LowCardinalityStringBuilderOptions{})
	for _, value := range []string{"beta", "alpha", "beta"} {
		if _, err := builder.Append(value); err != nil {
			t.Fatalf("Append(%q) error = %v", value, err)
		}
	}
	if err := builder.AppendNull(); err != nil {
		t.Fatalf("AppendNull() error = %v", err)
	}
	original, err := builder.Build()
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	encoded, err := original.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary() error = %v", err)
	}
	decoded, err := UnmarshalLowCardinalityStringColumn(encoded)
	if err != nil {
		t.Fatalf("UnmarshalLowCardinalityStringColumn() error = %v", err)
	}
	if got, want := decoded.DictionaryValues(), original.DictionaryValues(); !reflect.DeepEqual(got, want) {
		t.Fatalf("decoded dictionary = %#v, want %#v", got, want)
	}
	for row := 0; row < original.Len(); row++ {
		gotValue, gotValid := decoded.ValueAt(row)
		wantValue, wantValid := original.ValueAt(row)
		if gotValue != wantValue || gotValid != wantValid {
			t.Fatalf("decoded row %d = %q/%t, want %q/%t", row, gotValue, gotValid, wantValue, wantValid)
		}
	}
	corruptions := [][]byte{nil, append([]byte{}, encoded[:len(encoded)-1]...), append(append([]byte{}, encoded...), 0)}
	for index, corrupted := range corruptions {
		if _, err := UnmarshalLowCardinalityStringColumn(corrupted); err == nil {
			t.Fatalf("corruption %d unexpectedly decoded", index)
		}
	}
	for offset := range encoded {
		corrupted := append([]byte{}, encoded...)
		corrupted[offset] ^= 0xff
		func() {
			defer func() {
				if recovered := recover(); recovered != nil {
					t.Fatalf("byte mutation at offset %d panicked: %v", offset, recovered)
				}
			}()
			_, _ = UnmarshalLowCardinalityStringColumn(corrupted)
		}()
	}
}
