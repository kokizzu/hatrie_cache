package hatSql

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestMergeColumnarPartsLoadsOnlyRequestedColumns(t *testing.T) {
	parts := []*recordingColumnarPart{
		{batch: ColumnarBatch{
			Columns: map[string][]interface{}{
				"id":      {int64(1), int64(2)},
				"region":  {"ap", "eu"},
				"payload": {strings.Repeat("x", 1024), strings.Repeat("y", 1024)},
			},
			Rows: 2,
		}},
		{batch: ColumnarBatch{
			Columns: map[string][]interface{}{
				"id":      {int64(3), int64(4)},
				"region":  {"ap", "us"},
				"payload": {strings.Repeat("z", 1024), strings.Repeat("w", 1024)},
			},
			Rows: 2,
		}},
	}
	mergeParts := []ColumnarMergePart{parts[0], parts[1]}

	merged, err := MergeColumnarParts(mergeParts, []string{"id", "region"})
	if err != nil {
		t.Fatal(err)
	}
	if merged.Rows != 4 {
		t.Fatalf("merged rows = %d, want 4", merged.Rows)
	}
	for row, want := range []interface{}{int64(1), int64(2), int64(3), int64(4)} {
		got, ok := merged.Value("id", row)
		if !ok || got != want {
			t.Fatalf("id row %d = %#v/%v, want %#v/true", row, got, ok, want)
		}
	}
	for row, want := range []interface{}{"ap", "eu", "ap", "us"} {
		got, ok := merged.Value("region", row)
		if !ok || got != want {
			t.Fatalf("region row %d = %#v/%v, want %#v/true", row, got, ok, want)
		}
	}
	for index, part := range parts {
		if want := []string{"id", "region"}; !reflect.DeepEqual(part.loaded, want) {
			t.Errorf("part %d loaded columns = %#v, want %#v", index, part.loaded, want)
		}
	}
}

func TestMergeColumnarPartsAcceptsDictionaryColumns(t *testing.T) {
	parts := []ColumnarMergePart{
		ColumnarBatchPart{Batch: ColumnarBatch{
			Dictionaries: map[string]DictionaryColumn{
				"region": {Values: []string{"ap", "eu"}, Codes: []uint32{0, 1}},
			},
			Rows: 2,
		}},
	}
	merged, err := MergeColumnarParts(parts, []string{"region"})
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := merged.Value("region", 1); !ok || got != "eu" {
		t.Fatalf("dictionary value = %#v/%v, want eu/true", got, ok)
	}
}

func TestMergeColumnarPartsRejectsInvalidParts(t *testing.T) {
	part := &recordingColumnarPart{batch: ColumnarBatch{Rows: 1}}
	for name, parts := range map[string][]ColumnarMergePart{
		"empty parts":   {},
		"nil part":      {nil},
		"negative rows": {&recordingColumnarPart{batch: ColumnarBatch{Rows: -1}}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := MergeColumnarParts(parts, []string{"id"}); !errors.Is(err, ErrColumnarMergeInvalid) {
				t.Fatalf("error = %v, want %v", err, ErrColumnarMergeInvalid)
			}
		})
	}
	if _, err := MergeColumnarParts([]ColumnarMergePart{part}, nil); !errors.Is(err, ErrColumnarMergeInvalid) {
		t.Fatalf("empty fields error = %v, want %v", err, ErrColumnarMergeInvalid)
	}
	if _, err := MergeColumnarParts([]ColumnarMergePart{part}, []string{"id", "id"}); !errors.Is(err, ErrColumnarMergeInvalid) {
		t.Fatalf("duplicate fields error = %v, want %v", err, ErrColumnarMergeInvalid)
	}
}

func TestMergeColumnarPartsRejectsMissingOrShortColumns(t *testing.T) {
	missing := &recordingColumnarPart{batch: ColumnarBatch{Rows: 1}}
	if _, err := MergeColumnarParts([]ColumnarMergePart{missing}, []string{"id"}); !errors.Is(err, ErrColumnarMergeFieldMissing) {
		t.Fatalf("missing column error = %v, want %v", err, ErrColumnarMergeFieldMissing)
	}
	short := &recordingColumnarPart{batch: ColumnarBatch{Columns: map[string][]interface{}{"id": {int64(1)}}, Rows: 2}}
	if _, err := MergeColumnarParts([]ColumnarMergePart{short}, []string{"id"}); !errors.Is(err, ErrColumnarMergeFieldMismatch) {
		t.Fatalf("short column error = %v, want %v", err, ErrColumnarMergeFieldMismatch)
	}
}

func BenchmarkMergeColumnarParts(b *testing.B) {
	parts := make([]ColumnarMergePart, 4)
	for partIndex := range parts {
		rows := make([]interface{}, 256)
		regions := make([]interface{}, 256)
		payload := make([]interface{}, 256)
		for row := range rows {
			rows[row] = int64(partIndex*len(rows) + row)
			regions[row] = []string{"ap", "eu", "us", "sa"}[row%4]
			payload[row] = strings.Repeat("payload-", 16)
		}
		parts[partIndex] = ColumnarBatchPart{Batch: ColumnarBatch{Columns: map[string][]interface{}{
			"id": rows, "region": regions, "payload": payload,
		}, Rows: len(rows)}}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := MergeColumnarParts(parts, []string{"id", "region"}); err != nil {
			b.Fatal(err)
		}
	}
}

type recordingColumnarPart struct {
	batch  ColumnarBatch
	loaded []string
}

func (part *recordingColumnarPart) RowCount() int {
	return part.batch.Rows
}

func (part *recordingColumnarPart) LoadColumn(field string) ([]interface{}, bool, error) {
	part.loaded = append(part.loaded, field)
	if part.batch.FieldRows(field) == 0 {
		return nil, false, nil
	}
	values := make([]interface{}, part.batch.Rows)
	for row := range values {
		value, ok := part.batch.Value(field, row)
		if !ok {
			return nil, true, nil
		}
		values[row] = value
	}
	return values, true, nil
}
