package hatSql

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

type dictionaryValuesQueryResolver struct {
	batch ColumnarBatch
}

func (resolver dictionaryValuesQueryResolver) ResolveSQLSource(string, string) ([]Row, error) {
	return nil, errors.New("row source must not be resolved for packed dictionary values")
}

func (resolver dictionaryValuesQueryResolver) ResolveSQLColumnarSource(string, string, []string) (ColumnarBatch, bool, error) {
	return resolver.batch, true, nil
}

func TestColumnarBatchPackDictionaryValuesRoundTrip(t *testing.T) {
	batch := ColumnarBatch{Dictionaries: map[string]DictionaryColumn{
		"team": {Values: []string{"ops", "", "data"}, Codes: []uint32{0, 2, 1, 0}},
	}}

	batch.PackDictionaryValues()
	batch.PackDictionaryCodes()

	dictionary := batch.Dictionaries["team"]
	if dictionary.Values != nil {
		t.Fatalf("legacy dictionary values retained: %#v", dictionary.Values)
	}
	for code, want := range []string{"ops", "", "data"} {
		got, ok := dictionary.ValueAt(uint32(code))
		if !ok || got != want {
			t.Fatalf("ValueAt(%d) = %q, %v; want %q, true", code, got, ok, want)
		}
	}
	if got := dictionary.ValueCount(); got != 3 {
		t.Fatalf("ValueCount = %d, want 3", got)
	}
	if got, ok := batch.Value("team", 2); !ok || !reflect.DeepEqual(got, "") {
		t.Fatalf("batch Value(team, 2) = %#v, %v; want empty string, true", got, ok)
	}
}

func TestColumnarBatchPackDictionaryValuesPreservesSmallLegacyDictionary(t *testing.T) {
	batch := ColumnarBatch{Dictionaries: map[string]DictionaryColumn{
		"team": {Values: []string{"only"}},
	}}

	batch.PackDictionaryValues()

	if !reflect.DeepEqual(batch.Dictionaries["team"].Values, []string{"only"}) {
		t.Fatalf("small dictionary values changed: %#v", batch.Dictionaries["team"].Values)
	}
}

func TestPackedDictionaryValuesSQLAndVerticalMerge(t *testing.T) {
	first := ColumnarBatch{
		Dictionaries: map[string]DictionaryColumn{
			"team": {Values: []string{"ops", "core", "data"}, Codes: []uint32{0, 1, 2, 1}},
		},
		Rows: 4,
	}
	first.PackDictionaryValues()
	result, err := ExecuteSQLQueryParameters(context.Background(), "SELECT DISTINCT team FROM CACHE('items') WHERE team IN ('core', 'ops') ORDER BY team", dictionaryValuesQueryResolver{batch: first}, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if want := []SQLRow{{"team": "core"}, {"team": "ops"}}; !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("filtered rows = %#v, want %#v", result.Rows, want)
	}

	second := ColumnarBatch{
		Dictionaries: map[string]DictionaryColumn{
			"team": {Values: []string{"ops", "core", "data"}, Codes: []uint32{2, 0}},
		},
		Rows: 2,
	}
	second.PackDictionaryValues()
	merged, err := MergeColumnarParts([]ColumnarMergePart{ColumnarBatchPart{Batch: first}, ColumnarBatchPart{Batch: second}}, []string{"team"})
	if err != nil {
		t.Fatal(err)
	}
	wantValues := []interface{}{"ops", "core", "data", "core", "data", "ops"}
	for row, want := range wantValues {
		got, ok := merged.Value("team", row)
		if !ok || !reflect.DeepEqual(got, want) {
			t.Fatalf("merged Value(team, %d) = %#v, %v; want %#v, true", row, got, ok, want)
		}
	}
}

func TestDictionaryValuesValidRejectsMalformedPackedOffsets(t *testing.T) {
	for name, dictionary := range map[string]DictionaryColumn{
		"missing first offset": {PackedValueData: "ok", ValueOffsets: []uint32{1, 2}},
		"descending offset":    {PackedValueData: "ok", ValueOffsets: []uint32{0, 2, 1}},
		"past data":            {PackedValueData: "ok", ValueOffsets: []uint32{0, 1, 3}},
		"missing terminal":     {PackedValueData: "ok", ValueOffsets: []uint32{0}},
	} {
		t.Run(name, func(t *testing.T) {
			if dictionary.ValuesValid() {
				t.Fatal("malformed packed offsets accepted")
			}
			batch := ColumnarBatch{Dictionaries: map[string]DictionaryColumn{"team": dictionary}, Rows: 1}
			if got := batch.FieldRows("team"); got != 0 {
				t.Fatalf("FieldRows(team) = %d, want 0", got)
			}
		})
	}
}

var dictionaryValuesPackedBenchmarkSink int

func newDictionaryValuesPackedBenchmarkBatch(packed bool) ColumnarBatch {
	values := []string{"alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel", "india", "juliet", "kilo", "lima", "mike", "november", "oscar", "papa"}
	codes := make([]uint32, 4096)
	for row := range codes {
		codes[row] = uint32(row & 15)
	}
	batch := ColumnarBatch{Dictionaries: map[string]DictionaryColumn{"team": {Values: values, Codes: codes}}, Rows: len(codes)}
	if packed {
		batch.PackDictionaryValues()
	}
	return batch
}

func dictionaryValuesPackedBenchmarkLayoutBytes(batch ColumnarBatch) int {
	dictionary := batch.Dictionaries["team"]
	bytes := len(dictionary.Codes) * 4
	return bytes + dictionaryValuesPackedBenchmarkDictionaryBytes(dictionary)
}

func dictionaryValuesPackedBenchmarkDictionaryBytes(dictionary DictionaryColumn) int {
	bytes := 0
	if dictionary.Values != nil {
		for _, value := range dictionary.Values {
			bytes += 16 + len(value)
		}
	} else {
		bytes += 16 + len(dictionary.PackedValueData) + len(dictionary.ValueOffsets)*4
	}
	return bytes
}

func BenchmarkColumnarDictionaryValuesPackedLookup(b *testing.B) {
	for _, test := range []struct {
		name   string
		packed bool
	}{
		{name: "legacy", packed: false},
		{name: "packed", packed: true},
	} {
		b.Run(test.name, func(b *testing.B) {
			batch := newDictionaryValuesPackedBenchmarkBatch(test.packed)
			b.ResetTimer()
			b.ReportMetric(float64(dictionaryValuesPackedBenchmarkLayoutBytes(batch)), "layout-bytes/op")
			b.ReportMetric(float64(dictionaryValuesPackedBenchmarkDictionaryBytes(batch.Dictionaries["team"])), "dictionary-bytes/op")
			checksum := 0
			for iteration := 0; iteration < b.N; iteration++ {
				for row := 0; row < batch.Rows; row++ {
					value, ok := batch.Value("team", row)
					if ok {
						checksum += len(value.(string))
					}
				}
			}
			dictionaryValuesPackedBenchmarkSink = checksum
		})
	}
}
