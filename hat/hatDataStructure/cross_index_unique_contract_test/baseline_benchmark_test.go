package cross_index_unique_contract_test

import (
	"testing"

	"hatrie_cache/hat/hatDataStructure"
)

type baselineRow struct {
	Email string
	Code  string
}

type baselineIndexes struct {
	emails *hatDataStructure.HashIndex[baselineRow, string]
	codes  *hatDataStructure.HashIndex[baselineRow, string]
}

func newBaselineIndexes(capacity int) *baselineIndexes {
	emails, err := hatDataStructure.NewHashIndex(
		func(row baselineRow) string { return row.Email },
		hatDataStructure.HashIndexOptions{Unique: true, Capacity: capacity},
	)
	if err != nil {
		panic(err)
	}
	codes, err := hatDataStructure.NewHashIndex(
		func(row baselineRow) string { return row.Code },
		hatDataStructure.HashIndexOptions{Unique: true, Capacity: capacity},
	)
	if err != nil {
		panic(err)
	}
	return &baselineIndexes{emails: emails, codes: codes}
}

func (indexes *baselineIndexes) upsert(id uint64, row baselineRow) error {
	if err := indexes.emails.Upsert(id, row); err != nil {
		return err
	}
	if err := indexes.codes.Upsert(id, row); err != nil {
		indexes.emails.Delete(id)
		return err
	}
	return nil
}

func tu22BenchmarkRows(size int) []baselineRow {
	rows := make([]baselineRow, size)
	for i := range rows {
		rows[i] = baselineRow{
			Email: "user-" + string(rune('a'+i%26)) + "-" + uintString(i) + "@example.test",
			Code:  "code-" + uintString(i),
		}
	}
	return rows
}

func uintString(value int) string {
	if value == 0 {
		return "0"
	}
	var digits [20]byte
	position := len(digits)
	for value > 0 {
		position--
		digits[position] = byte('0' + value%10)
		value /= 10
	}
	return string(digits[position:])
}

func BenchmarkTU22BaselineUpsert(b *testing.B) {
	const size = 10_000
	rows := tu22BenchmarkRows(size)
	indexes := newBaselineIndexes(size)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		row := rows[i%size]
		if err := indexes.upsert(uint64(i%size+1), row); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTU22BaselineBuild10000(b *testing.B) {
	const size = 10_000
	rows := tu22BenchmarkRows(size)
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		indexes := newBaselineIndexes(size)
		for i, row := range rows {
			if err := indexes.upsert(uint64(i+1), row); err != nil {
				b.Fatal(err)
			}
		}
	}
}
