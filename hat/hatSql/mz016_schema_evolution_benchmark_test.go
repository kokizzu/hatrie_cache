package hatSql_test

import (
	"testing"

	"hatrie_cache/hat/hatSql"
)

var mz016SchemaEvolutionBenchmarkSink hatSql.Row

func BenchmarkMZ016SchemaEvolutionPlanReuse(b *testing.B) {
	current, expected, row := mz016SchemaEvolutionBenchmarkFixture()
	plan, err := hatSql.NewSQLSchemaEvolutionPlan(current, expected, hatSql.SQLSchemaEvolutionOptions{AllowAddedColumns: true})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		adapted, err := plan.AdaptRow(row)
		if err != nil {
			b.Fatal(err)
		}
		mz016SchemaEvolutionBenchmarkSink = adapted
	}
}

func BenchmarkMZ016SchemaEvolutionRebuildPerRow(b *testing.B) {
	current, expected, row := mz016SchemaEvolutionBenchmarkFixture()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		plan, err := hatSql.NewSQLSchemaEvolutionPlan(current, expected, hatSql.SQLSchemaEvolutionOptions{AllowAddedColumns: true})
		if err != nil {
			b.Fatal(err)
		}
		adapted, err := plan.AdaptRow(row)
		if err != nil {
			b.Fatal(err)
		}
		mz016SchemaEvolutionBenchmarkSink = adapted
	}
}

func mz016SchemaEvolutionBenchmarkFixture() ([]hatSql.SQLRowBinaryColumn, []hatSql.SQLRowBinaryColumn, hatSql.Row) {
	current := []hatSql.SQLRowBinaryColumn{
		{Name: "id", Type: hatSql.SQLRowBinaryInt64},
		{Name: "name", Type: hatSql.SQLRowBinaryString, Nullable: true},
		{Name: "region", Type: hatSql.SQLRowBinaryString, Nullable: true},
		{Name: "score", Type: hatSql.SQLRowBinaryFloat64, Nullable: true},
		{Name: "source", Type: hatSql.SQLRowBinaryString, Nullable: true},
	}
	expected := []hatSql.SQLRowBinaryColumn{
		{Name: "id", Type: hatSql.SQLRowBinaryInt64},
		{Name: "name", Type: hatSql.SQLRowBinaryString, Nullable: true},
		{Name: "score", Type: hatSql.SQLRowBinaryFloat64, Nullable: true},
	}
	row := hatSql.Row{
		"id":     int64(17),
		"name":   "Ada",
		"region": "apac",
		"score":  float64(42),
		"source": "import",
	}
	return current, expected, row
}
