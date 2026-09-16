package hatSql

import (
	"strconv"
	"testing"
)

func chU21BenchmarkCSVData() []byte {
	data := make([]byte, 0, 400_000)
	data = append(data, "id,name\n"...)
	for index := 0; index < 20_000; index++ {
		data = strconv.AppendInt(data, int64(index), 10)
		data = append(data, ",name-"...)
		data = strconv.AppendInt(data, int64(index%1000), 10)
		data = append(data, '\n')
	}
	return data
}

func chU21BenchmarkJSONEachRowData() []byte {
	data := make([]byte, 0, 500_000)
	for index := 0; index < 20_000; index++ {
		data = append(data, "{\"id\":"...)
		data = strconv.AppendInt(data, int64(index), 10)
		data = append(data, ",\"name\":\"name-"...)
		data = strconv.AppendInt(data, int64(index%1000), 10)
		data = append(data, "\"}\n"...)
	}
	return data
}

func BenchmarkExternalTablesImportCSVWholeBuffer(b *testing.B) {
	data := chU21BenchmarkCSVData()
	tables := NewExternalTables()
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := tables.ImportCSV("people", data); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkExternalTablesImportNDJSONWholeBuffer(b *testing.B) {
	data := chU21BenchmarkJSONEachRowData()
	tables := NewExternalTables()
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := tables.ImportNDJSON("people", data); err != nil {
			b.Fatal(err)
		}
	}
}
