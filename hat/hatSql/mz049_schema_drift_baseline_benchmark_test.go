package hatSql

import (
	"bytes"
	"encoding/json"
	"testing"
)

var mz049BaselineSink Row

func BenchmarkMZ049SchemaDriftBaseline(b *testing.B) {
	rows := mz049BenchmarkRows()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		for _, row := range rows {
			mz049BaselineSink = row
		}
	}
}

var mz049BaselineStreamSink Row

func BenchmarkMZ049SchemaDriftJSONStreamBaseline(b *testing.B) {
	data := mz049BenchmarkNDJSONData()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		err := StreamJSONEachRow(bytes.NewReader(data), ExternalImportOptions{}, func(row Row) error {
			mz049BaselineStreamSink = row
			return nil
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}

func mz049BenchmarkRows() []Row {
	rows := make([]Row, 256)
	for index := range rows {
		rows[index] = Row{
			"active":   index%2 == 0,
			"id":       int64(index),
			"metadata": map[string]interface{}{"region": "sg", "tier": index % 3},
			"name":     "customer",
			"score":    float64(index) + 0.5,
		}
	}
	return rows
}

func mz049BenchmarkData() []byte {
	data, err := json.Marshal(mz049BenchmarkRows())
	if err != nil {
		panic(err)
	}
	return data
}

func mz049BenchmarkNDJSONData() []byte {
	var data bytes.Buffer
	for _, row := range mz049BenchmarkRows() {
		encoded, err := json.Marshal(row)
		if err != nil {
			panic(err)
		}
		data.Write(encoded)
		data.WriteByte('\n')
	}
	return data.Bytes()
}
