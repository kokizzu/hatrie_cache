package hatSql

import (
	"encoding/binary"
	"strconv"
	"testing"
)

var tr019ColumnarBatchValueSink interface{}

func BenchmarkTR019ColumnarBatchValue(b *testing.B) {
	for _, test := range []struct {
		name    string
		prepare bool
	}{
		{name: "uncached"},
		{name: "cached", prepare: true},
	} {
		b.Run(test.name, func(b *testing.B) {
			batch := tr019MixedColumnarBatch()
			fields := []string{"team", "score", "active", "ratio", "name"}
			if test.prepare {
				batch.PrepareFieldOffsets()
			}
			b.ReportAllocs()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				row := index % batch.Rows
				field := fields[index%len(fields)]
				value, valid := batch.Value(field, row)
				if !valid {
					b.Fatalf("Value(%q, %d) returned invalid value", field, row)
				}
				tr019ColumnarBatchValueSink = value
			}
		})
	}
}

func BenchmarkTR019ColumnarBatchPrepareFieldOffsets(b *testing.B) {
	batch := tr019MixedColumnarBatch()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		batch.fieldOffsets = nil
		batch.PrepareFieldOffsets()
	}
}

func tr019MixedColumnarBatch() ColumnarBatch {
	const rows = 128
	codes := make([]uint32, rows)
	for index := range codes {
		codes[index] = uint32(index % 4)
	}
	bits := make([]byte, (rows+7)/8)
	for index := 0; index < rows; index += 2 {
		bits[index>>3] |= 1 << (index & 7)
	}
	numericData := make([]byte, rows*8)
	for index := 0; index < rows; index++ {
		binary.LittleEndian.PutUint64(numericData[index*8:], uint64(index*10))
	}
	packed := make([]interface{}, rows)
	for index := range packed {
		packed[index] = int64(index)
	}
	packedValidity := make([]byte, (rows+7)/8)
	for index := range packedValidity {
		packedValidity[index] = 0xff
	}
	packedRanks := make([]uint32, columnarPackedRankCount(rows))
	for index := range packedRanks {
		packedRanks[index] = uint32(index * 8)
	}
	plain := make([]interface{}, rows)
	for index := range plain {
		plain[index] = "name-" + strconv.Itoa(index%8)
	}
	return ColumnarBatch{
		Columns: map[string][]interface{}{
			"name": plain,
		},
		Dictionaries: map[string]DictionaryColumn{
			"team": {Values: []string{"red", "green", "blue", "yellow"}, Codes: codes},
		},
		PackedColumns: map[string]ColumnarPackedColumn{
			"score": {Values: packed, Validity: packedValidity, Ranks: packedRanks, Rows: rows},
		},
		BoolColumns: map[string]ColumnarBoolColumn{
			"active": {Bits: bits, Rows: rows},
		},
		NumericColumns: map[string]ColumnarNumericColumn{
			"ratio": {Kind: ColumnarNumericInt64, Data: numericData, Rows: rows},
		},
		Rows: rows,
	}
}
