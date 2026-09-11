package hatCache

import (
	"context"
	"testing"
)

func TestSQLPlannerStatisticsAnalyzeAndInvalidate(t *testing.T) {
	trie := CreateHatTrie()
	defer trie.Destroy()
	trie.UpsertString("people", `[
		{"age":21,"state":"open"},
		{"age":21,"state":"open"},
		{"age":42,"state":"closed"},
		{"age":null,"state":"closed"}
	]`)

	if _, available, err := trie.SQLWhatIfSourceStatistics("CACHE", "people", []string{"age"}); err != nil || available {
		t.Fatalf("SQLWhatIfSourceStatistics before ANALYZE = available %v, error %v; want unavailable", available, err)
	}

	statistics, err := trie.AnalyzeSQLSource("CACHE", "people", "age", "state")
	if err != nil {
		t.Fatalf("AnalyzeSQLSource() error = %v", err)
	}
	if statistics.Source != "CACHE(people)" || statistics.Rows != 4 || statistics.Bytes == 0 {
		t.Fatalf("AnalyzeSQLSource() = %#v, want source, rows, and bytes", statistics)
	}
	age := statistics.Fields["age"]
	if age.Rows != 3 || age.NullRows != 1 || age.DistinctValues != 2 || age.Minimum != float64(21) || age.Maximum != float64(42) {
		t.Fatalf("age statistics = %#v, want exact counts and numeric bounds", age)
	}
	if len(age.FrequencyHistogram) != 2 || age.FrequencyHistogram[0] != (SQLWhatIfFrequencyBucket{RowsPerValue: 1, DistinctValues: 1}) || age.FrequencyHistogram[1] != (SQLWhatIfFrequencyBucket{RowsPerValue: 2, DistinctValues: 1}) {
		t.Fatalf("age frequency histogram = %#v, want one singleton and one duplicate bucket", age.FrequencyHistogram)
	}

	cached, available, err := trie.SQLWhatIfSourceStatistics("CACHE", "people", []string{"age"})
	if err != nil || !available {
		t.Fatalf("SQLWhatIfSourceStatistics after ANALYZE = available %v, error %v; want available", available, err)
	}
	if cached.Rows != statistics.Rows || cached.Fields["age"].DistinctValues != 2 {
		t.Fatalf("cached statistics = %#v, want analyzed statistics", cached)
	}
	if _, available, err := trie.SQLWhatIfSourceStatistics("CACHE", "people", []string{"missing"}); err != nil || available {
		t.Fatalf("SQLWhatIfSourceStatistics for unanalyzed field = available %v, error %v; want unavailable", available, err)
	}
	whatIf, err := ExplainSQLWhatIf(context.Background(), SQLWhatIfRequest{
		Query: `FROM CACHE('people') AS p WHERE p.age = 21 SELECT p.id`,
		Index: SQLWhatIfIndex{Kind: SQLWhatIfIndexEquality, Fields: []string{"age"}},
	}, trie)
	if err != nil {
		t.Fatalf("ExplainSQLWhatIf() error = %v", err)
	}
	if whatIf.SourceRows != 4 || whatIf.HypotheticalRowsRead != 2 || whatIf.RowsSkipped != 2 {
		t.Fatalf("ExplainSQLWhatIf() = %#v, want analyzed cardinality estimate", whatIf)
	}

	trie.UpsertString("people", `[{"age":7,"state":"open"}]`)
	if _, available, err := trie.SQLWhatIfSourceStatistics("CACHE", "people", []string{"age"}); err != nil || available {
		t.Fatalf("SQLWhatIfSourceStatistics after source mutation = available %v, error %v; want unavailable", available, err)
	}
	updated, err := trie.AnalyzeSQLSource("CACHE", "people", "age")
	if err != nil {
		t.Fatalf("AnalyzeSQLSource() after mutation error = %v", err)
	}
	if updated.Rows != 1 || updated.Fields["age"].DistinctValues != 1 || updated.Fields["age"].Minimum != float64(7) || updated.Fields["age"].Maximum != float64(7) {
		t.Fatalf("updated statistics = %#v, want refreshed source statistics", updated)
	}

	trie.UpsertString("mixed", `[{"value":1},{"value":"not numeric"}]`)
	mixed, err := trie.AnalyzeSQLSource("CACHE", "mixed", "value")
	if err != nil {
		t.Fatalf("AnalyzeSQLSource() mixed values error = %v", err)
	}
	if mixed.Fields["value"].Minimum != nil || mixed.Fields["value"].Maximum != nil {
		t.Fatalf("mixed numeric statistics = %#v, want no unsafe numeric bounds", mixed.Fields["value"])
	}
}
