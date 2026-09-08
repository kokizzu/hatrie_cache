package hatSql

import (
	"reflect"
	"testing"
)

func TestColumnarDictionaryAdmissionMaximumUniqueMatchesSizeBound(t *testing.T) {
	for _, test := range []struct {
		rows, want int
	}{
		{4, 3},
		{5, 3},
		{6, 4},
		{7, 5},
		{8, 6},
		{9, 6},
	} {
		if got := columnarDictionaryMaximumUnique(test.rows); got != test.want {
			t.Fatalf("columnarDictionaryMaximumUnique(%d) = %d, want %d", test.rows, got, test.want)
		}
	}
}

func TestEncodeRepeatedStringsDefersCodesButPreservesFirstSeenOrder(t *testing.T) {
	values := []interface{}{"alpha", "beta", "alpha", "gamma", "beta", "alpha"}
	batch := ColumnarBatch{
		Columns: map[string][]interface{}{"state": values},
		Rows:    len(values),
	}
	batch.EncodeRepeatedStrings()
	dictionary, encoded := batch.Dictionaries["state"]
	if !encoded {
		t.Fatal("state was not dictionary encoded")
	}
	if !reflect.DeepEqual(dictionary.Values, []string{"alpha", "beta", "gamma"}) {
		t.Fatalf("dictionary values = %#v", dictionary.Values)
	}
	if !reflect.DeepEqual(dictionary.Codes, []uint32{0, 1, 0, 2, 1, 0}) {
		t.Fatalf("dictionary codes = %#v", dictionary.Codes)
	}
	for index, want := range values {
		if got, ok := batch.Value("state", index); !ok || got != want {
			t.Fatalf("Value(state, %d) = %#v/%t, want %#v/true", index, got, ok, want)
		}
	}
}

func TestEncodeRepeatedStringsRejectsPastUniqueBoundWithoutChangingPlainColumn(t *testing.T) {
	values := []interface{}{"a", "b", "c", "d", "e", "f", "g", "a"}
	batch := ColumnarBatch{
		Columns: map[string][]interface{}{"state": values},
		Rows:    len(values),
	}
	batch.EncodeRepeatedStrings()
	if _, encoded := batch.Dictionaries["state"]; encoded {
		t.Fatal("high-cardinality state was dictionary encoded")
	}
	if got := batch.Columns["state"]; !reflect.DeepEqual(got, values) {
		t.Fatalf("plain column = %#v, want %#v", got, values)
	}
}

func TestEncodeRepeatedStringsPreservesCodesAfterPrefixBuffer(t *testing.T) {
	values := []interface{}{
		"alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel",
		"alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel",
	}
	batch := ColumnarBatch{
		Columns: map[string][]interface{}{"state": values},
		Rows:    len(values),
	}

	batch.EncodeRepeatedStrings()

	dictionary, encoded := batch.Dictionaries["state"]
	if !encoded {
		t.Fatal("state was not dictionary encoded")
	}
	wantCodes := []uint32{0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7}
	if !reflect.DeepEqual(dictionary.Codes, wantCodes) {
		t.Fatalf("dictionary codes = %#v, want %#v", dictionary.Codes, wantCodes)
	}
	for index, want := range values {
		if got, ok := batch.Value("state", index); !ok || got != want {
			t.Fatalf("Value(state, %d) = %#v/%t, want %#v/true", index, got, ok, want)
		}
	}
}

func TestEncodeRepeatedStringsRejectsMixedColumnAfterDuplicate(t *testing.T) {
	values := []interface{}{"alpha", "bravo", "alpha", int64(3), "delta", "echo"}
	batch := ColumnarBatch{
		Columns: map[string][]interface{}{"state": values},
		Rows:    len(values),
	}

	batch.EncodeRepeatedStrings()

	if _, encoded := batch.Dictionaries["state"]; encoded {
		t.Fatal("mixed state column was dictionary encoded")
	}
	if got := batch.Columns["state"]; !reflect.DeepEqual(got, values) {
		t.Fatalf("plain column = %#v, want %#v", got, values)
	}
}

func TestEncodeRepeatedStringsRejectsHighCardinalityAfterDuplicate(t *testing.T) {
	values := []interface{}{"a", "a", "b", "c", "d", "e", "f", "g"}
	batch := ColumnarBatch{
		Columns: map[string][]interface{}{"state": values},
		Rows:    len(values),
	}

	batch.EncodeRepeatedStrings()

	if _, encoded := batch.Dictionaries["state"]; encoded {
		t.Fatal("high-cardinality state was dictionary encoded")
	}
	if got := batch.Columns["state"]; !reflect.DeepEqual(got, values) {
		t.Fatalf("plain column = %#v, want %#v", got, values)
	}
}
