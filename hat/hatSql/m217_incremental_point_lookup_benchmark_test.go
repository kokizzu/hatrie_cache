package hatSql

import (
	"strconv"
	"testing"
)

func BenchmarkM217FullScanPointLookup(b *testing.B) {
	rows := m217BenchmarkRows(10_000)
	b.ReportAllocs()
	b.ResetTimer()
	var result []Row
	for iteration := 0; iteration < b.N; iteration++ {
		result = m217ScanPointLookup(rows, "team-42")
	}
	b.StopTimer()
	b.ReportMetric(float64(len(rows)), "source_rows")
	b.ReportMetric(float64(len(result)), "result_rows")
}

func BenchmarkM217MaintainedPointLookup(b *testing.B) {
	rows := m217BenchmarkRows(10_000)
	index, err := NewIncrementalPointLookup(IncrementalPointLookupDefinition{
		IndexKey: func(row Row) (string, error) {
			return row["team"].(string), nil
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	updates := make([]DifferentialRow, len(rows))
	for index, row := range rows {
		updates[index] = DifferentialRow{Key: row["id"].(string), Time: 1, Diff: 1, Row: row}
	}
	if err := index.Apply(updates); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	var result []DifferentialRow
	for iteration := 0; iteration < b.N; iteration++ {
		result = index.Lookup("team-42")
	}
	b.StopTimer()
	b.ReportMetric(float64(index.Len()), "retained_entries")
	b.ReportMetric(float64(len(result)), "result_rows")
}

func BenchmarkM217MaintainedPointLookupBuild(b *testing.B) {
	rows := m217BenchmarkRows(10_000)
	updates := make([]DifferentialRow, len(rows))
	for index, row := range rows {
		updates[index] = DifferentialRow{Key: row["id"].(string), Time: 1, Diff: 1, Row: row}
	}
	b.ReportAllocs()
	for iteration := 0; iteration < b.N; iteration++ {
		index, err := NewIncrementalPointLookup(IncrementalPointLookupDefinition{
			IndexKey: func(row Row) (string, error) {
				return row["team"].(string), nil
			},
		})
		if err != nil {
			b.Fatal(err)
		}
		if err := index.Apply(updates); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(len(rows)), "retained_entries")
}

func m217BenchmarkRows(count int) []Row {
	rows := make([]Row, count)
	for index := range rows {
		rows[index] = Row{
			"id":    "row-" + strconv.Itoa(index),
			"team":  "team-" + strconv.Itoa(index%64),
			"value": int64(index),
		}
	}
	return rows
}

func m217ScanPointLookup(rows []Row, indexKey string) []Row {
	result := make([]Row, 0, len(rows)/64)
	for _, row := range rows {
		if row["team"] == indexKey {
			result = append(result, cloneDifferentialRow(row))
		}
	}
	return result
}
