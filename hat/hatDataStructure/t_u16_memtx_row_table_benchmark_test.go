package hatDataStructure_test

import (
	"fmt"
	"testing"

	hatDataStructure "hatrie_cache/hat/hatDataStructure"
)

const (
	tU16BenchmarkRows    = 10_000
	tU16BenchmarkColumns = 4
)

var (
	tU16BenchmarkKeys   = tU16BuildKeys()
	tU16BenchmarkUpdate = []any{int64(7), 11, "updated", true}
	tU16BenchmarkSink   int64
)

func tU16BuildKeys() []string {
	keys := make([]string, tU16BenchmarkRows)
	for index := range keys {
		keys[index] = fmt.Sprintf("key-%05d", index)
	}
	return keys
}

func tU16BuildValues(index int) []any {
	return []any{int64(index), index % 17, "payload", index%2 == 0}
}

func tU16BuildMap() map[string][]any {
	rows := make(map[string][]any, tU16BenchmarkRows)
	for index, key := range tU16BenchmarkKeys {
		rows[key] = tU16BuildValues(index)
	}
	return rows
}

func tU16BuildTable() *hatDataStructure.MemtxRowTable {
	table, err := hatDataStructure.NewMemtxRowTableWithCapacity(tU16BenchmarkColumns, tU16BenchmarkRows)
	if err != nil {
		panic(err)
	}
	for index, key := range tU16BenchmarkKeys {
		if err := table.Upsert(key, tU16BuildValues(index)); err != nil {
			panic(err)
		}
	}
	return table
}

func BenchmarkTU16MapBuild(b *testing.B) {
	for index := 0; index < b.N; index++ {
		rows := tU16BuildMap()
		tU16BenchmarkSink += int64(len(rows))
	}
}

func BenchmarkTU16MemtxBuild(b *testing.B) {
	for index := 0; index < b.N; index++ {
		table := tU16BuildTable()
		tU16BenchmarkSink += int64(table.Len())
	}
}

func BenchmarkTU16MapGet(b *testing.B) {
	rows := tU16BuildMap()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		row := rows[tU16BenchmarkKeys[index%tU16BenchmarkRows]]
		tU16BenchmarkSink += row[0].(int64)
	}
}

func BenchmarkTU16MemtxGetInto(b *testing.B) {
	table := tU16BuildTable()
	buffer := make([]any, tU16BenchmarkColumns)
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		row, ok := table.GetInto(buffer, tU16BenchmarkKeys[index%tU16BenchmarkRows])
		if !ok {
			b.Fatal("GetInto() did not find benchmark key")
		}
		tU16BenchmarkSink += row[0].(int64)
	}
}

func BenchmarkTU16MapScan(b *testing.B) {
	rows := tU16BuildMap()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		var count int64
		for _, row := range rows {
			count += row[0].(int64)
		}
		tU16BenchmarkSink += count
	}
}

func BenchmarkTU16MemtxScanBorrowed(b *testing.B) {
	table := tU16BuildTable()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		var count int64
		table.VisitBorrowed(func(_ string, row []any) bool {
			count += row[0].(int64)
			return true
		})
		tU16BenchmarkSink += count
	}
}

func BenchmarkTU16MapUpdate(b *testing.B) {
	rows := tU16BuildMap()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		row := rows[tU16BenchmarkKeys[index%tU16BenchmarkRows]]
		copy(row, tU16BenchmarkUpdate)
		tU16BenchmarkSink += row[0].(int64)
	}
}

func BenchmarkTU16MemtxUpdate(b *testing.B) {
	table := tU16BuildTable()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := table.Upsert(tU16BenchmarkKeys[index%tU16BenchmarkRows], tU16BenchmarkUpdate); err != nil {
			b.Fatal(err)
		}
		tU16BenchmarkSink += int64(index)
	}
}
