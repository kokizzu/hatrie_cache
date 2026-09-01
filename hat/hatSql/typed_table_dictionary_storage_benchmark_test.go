package hatSql

import "testing"

const typedTableDictionaryBenchmarkRows = 10_000

func BenchmarkTypedTableDictionaryStringStorage(b *testing.B) {
	values := []string{"alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel"}
	b.Run("plain_string_headers", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			storage := typedTableColumnStorage{kind: TypedTableString}
			for index := 0; index < typedTableDictionaryBenchmarkRows; index++ {
				storage.append(TypedString(values[index%len(values)]))
			}
			if len(storage.strings) != typedTableDictionaryBenchmarkRows {
				b.Fatal("plain string storage lost rows")
			}
		}
	})
	b.Run("dictionary_codes", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			storage := typedTableColumnStorage{kind: TypedTableString, dictionary: true, dictionaryPositions: make(map[string]uint32)}
			for index := 0; index < typedTableDictionaryBenchmarkRows; index++ {
				storage.append(TypedString(values[index%len(values)]))
			}
			if len(storage.dictionaryCodes) != typedTableDictionaryBenchmarkRows || len(storage.dictionaryValues) != len(values) {
				b.Fatal("dictionary string storage lost rows")
			}
		}
	})
}
