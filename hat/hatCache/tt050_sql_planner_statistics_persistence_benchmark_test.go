package hatCache

import (
	"os"
	"path/filepath"
	"testing"
)

func BenchmarkTT050SaveSQLPlannerStatistics(b *testing.B) {
	trie := benchmarkSQLPlannerStatisticsTrie(b)
	defer trie.Destroy()
	if _, err := trie.AnalyzeSQLSource("CACHE", "people", "age", "state"); err != nil {
		b.Fatal(err)
	}
	path := filepath.Join(b.TempDir(), "planner-stats.hps")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := trie.SaveSQLPlannerStatistics(path); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	info, err := os.Stat(path)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportMetric(float64(info.Size()), "file-bytes")
}

func BenchmarkTT050LoadSQLPlannerStatistics(b *testing.B) {
	source := benchmarkSQLPlannerStatisticsTrie(b)
	defer source.Destroy()
	if _, err := source.AnalyzeSQLSource("CACHE", "people", "age", "state"); err != nil {
		b.Fatal(err)
	}
	path := filepath.Join(b.TempDir(), "planner-stats.hps")
	if err := source.SaveSQLPlannerStatistics(path); err != nil {
		b.Fatal(err)
	}
	trie := benchmarkSQLPlannerStatisticsTrie(b)
	defer trie.Destroy()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		report, err := trie.LoadSQLPlannerStatistics(path)
		if err != nil || report.Loaded != 1 || report.Skipped != 0 {
			b.Fatalf("LoadSQLPlannerStatistics() = %#v, error %v", report, err)
		}
	}
}
