package hatSql

import (
	"strconv"
	"testing"
)

func BenchmarkEncodeRepeatedStringsHighCardinality(b *testing.B) {
	values := make([]interface{}, 4096)
	for index := range values {
		values[index] = "value-" + strconv.Itoa(index)
	}
	batch := ColumnarBatch{
		Columns: map[string][]interface{}{"value": values},
		Rows:    len(values),
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		batch.EncodeRepeatedStrings()
	}
}

func BenchmarkEncodeRepeatedStringsLowCardinality(b *testing.B) {
	values := make([]interface{}, 4096)
	for index := range values {
		values[index] = []string{"queued", "running", "done", "failed"}[index%4]
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		batch := ColumnarBatch{
			Columns: map[string][]interface{}{"state": values},
			Rows:    len(values),
		}
		batch.EncodeRepeatedStrings()
	}
}

func BenchmarkEncodeRepeatedStringsHighCardinalityBaseline(b *testing.B) {
	values := make([]interface{}, 4096)
	for index := range values {
		values[index] = "value-" + strconv.Itoa(index)
	}
	batch := ColumnarBatch{
		Columns: map[string][]interface{}{"value": values},
		Rows:    len(values),
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		encodeRepeatedStringsBaseline(&batch)
	}
}

func BenchmarkEncodeRepeatedStringsLowCardinalityBaseline(b *testing.B) {
	values := make([]interface{}, 4096)
	for index := range values {
		values[index] = []string{"queued", "running", "done", "failed"}[index%4]
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		batch := ColumnarBatch{
			Columns: map[string][]interface{}{"state": values},
			Rows:    len(values),
		}
		encodeRepeatedStringsBaseline(&batch)
	}
}

func encodeRepeatedStringsBaseline(batch *ColumnarBatch) {
	if batch == nil || batch.Rows < 4 || batch.Columns == nil {
		return
	}
	if batch.Dictionaries == nil {
		batch.Dictionaries = make(map[string]DictionaryColumn)
	}
	for field, values := range batch.Columns {
		if len(values) != batch.Rows {
			continue
		}
		positions := make(map[string]uint32)
		strings := make([]string, 0)
		codes := make([]uint32, len(values))
		totalStringBytes := 0
		uniqueStringBytes := 0
		allStrings := true
		for index, value := range values {
			text, ok := value.(string)
			if !ok {
				allStrings = false
				break
			}
			code, found := positions[text]
			if !found {
				code = uint32(len(strings))
				positions[text] = code
				strings = append(strings, text)
				uniqueStringBytes += len(text)
			}
			totalStringBytes += len(text)
			codes[index] = code
		}
		if !allStrings || !columnarDictionaryLayoutSmaller(len(values), len(strings), totalStringBytes, uniqueStringBytes) {
			continue
		}
		batch.Dictionaries[field] = DictionaryColumn{Values: strings, Codes: codes}
		delete(batch.Columns, field)
	}
}
