package hatriecache

import (
	"os"
	"strings"
	"testing"
)

func TestBenchmarkSerializationScriptIncludesDocumentedStructuredJournalBenches(t *testing.T) {
	data, err := os.ReadFile("scripts/benchmark-serialization.sh")
	if err != nil {
		t.Fatalf("ReadFile(benchmark-serialization.sh) error = %v", err)
	}
	script := string(data)
	for _, token := range []string{
		"CommandJournal(Encode|Decode)(Structured)?(JSON|Binary)",
		"SnapshotFormat(JSON|Binary|GzipJSON|GzipBestJSON|GzipBinary|GzipBestBinary|Structured",
		"LevelDB(Save(Materialized|MaterializedJSON|StructuredMaterialized",
	} {
		if !strings.Contains(script, token) {
			t.Fatalf("benchmark-serialization.sh default benchmark regex does not include %q", token)
		}
	}
}

func TestNativeAhtableAllocatorBenchmarkIsReproducible(t *testing.T) {
	data, err := os.ReadFile("scripts/benchmark-native-ahtable-allocator.sh")
	if err != nil {
		t.Fatalf("ReadFile(benchmark-native-ahtable-allocator.sh) error = %v", err)
	}
	for _, token := range []string{
		"NATIVE_AHTABLE_KEYS",
		"NATIVE_AHTABLE_SLOTS",
		"bench_ahtable_allocator.c",
		"native-ahtable-allocator.txt",
		"-O3",
	} {
		if !strings.Contains(string(data), token) {
			t.Fatalf("native ahtable allocator benchmark script missing token %q", token)
		}
	}

	data, err = os.ReadFile("Makefile")
	if err != nil {
		t.Fatalf("ReadFile(Makefile) error = %v", err)
	}
	for _, token := range []string{"bench-native-ahtable-allocator:", "./scripts/benchmark-native-ahtable-allocator.sh"} {
		if !strings.Contains(string(data), token) {
			t.Fatalf("Makefile missing native ahtable allocator benchmark token %q", token)
		}
	}
}

func TestStartupPersistenceBenchmarkIsReproducible(t *testing.T) {
	data, err := os.ReadFile("scripts/benchmark-startup-persistence.sh")
	if err != nil {
		t.Fatalf("ReadFile(benchmark-startup-persistence.sh) error = %v", err)
	}
	for _, token := range []string{"STARTUP_PERSISTENCE_BENCH", "BenchmarkStartupPersistence10k", "startup-persistence.txt", "-benchmem"} {
		if !strings.Contains(string(data), token) {
			t.Fatalf("startup persistence benchmark script missing token %q", token)
		}
	}
	data, err = os.ReadFile("Makefile")
	if err != nil {
		t.Fatalf("ReadFile(Makefile) error = %v", err)
	}
	for _, token := range []string{"bench-startup-persistence:", "./scripts/benchmark-startup-persistence.sh"} {
		if !strings.Contains(string(data), token) {
			t.Fatalf("Makefile missing startup persistence benchmark token %q", token)
		}
	}
}

func TestHatTrieTransportFeatureBenchmarkScriptReportsRSS(t *testing.T) {
	data, err := os.ReadFile("scripts/benchmark-hatrie-transport-features.sh")
	if err != nil {
		t.Fatalf("ReadFile(benchmark-hatrie-transport-features.sh) error = %v", err)
	}
	script := string(data)
	for _, token := range []string{
		"HATRIE_TRANSPORT_BENCH",
		"BenchmarkCommandTransportFeature",
		"go test -c",
		"-test.benchmem",
		"/usr/bin/time",
		"Max resident set size",
	} {
		if !strings.Contains(script, token) {
			t.Fatalf("HAT-trie transport benchmark script missing token %q", token)
		}
	}
}

func TestBenchmarkSmokeScriptSupportsRegressionThresholds(t *testing.T) {
	data, err := os.ReadFile("scripts/benchmark-smoke.sh")
	if err != nil {
		t.Fatalf("ReadFile(benchmark-smoke.sh) error = %v", err)
	}
	script := string(data)
	for _, token := range []string{
		"BENCH_SMOKE_CHECK_THRESHOLDS",
		"BENCH_SMOKE_MAX_COMMAND_NS_OP",
		"BENCH_SMOKE_MAX_TRANSPORT_NS_OP",
		"BENCH_SMOKE_MAX_SERIALIZATION_NS_OP",
		"BENCH_SMOKE_MAX_B_OP",
		"BENCH_SMOKE_MAX_ALLOCS_OP",
		"BENCH_SMOKE_ARTIFACT_DIR",
		"BENCH_SMOKE_BASELINE_JSON",
		"BENCH_SMOKE_MAX_REGRESSION_PCT",
		"BENCH_SMOKE_COMPARE_MEMORY",
		"ns/op",
		"B/op",
		"allocs/op",
		"benchmark-smoke.json",
		"benchmark-smoke.md",
		"go run ./cmd/hatrie-benchcmp",
		"Benchmark smoke regression guard failed",
	} {
		if !strings.Contains(script, token) {
			t.Fatalf("benchmark-smoke.sh missing threshold token %q", token)
		}
	}

	data, err = os.ReadFile("Makefile")
	if err != nil {
		t.Fatalf("ReadFile(Makefile) error = %v", err)
	}
	makefile := string(data)
	for _, token := range []string{
		"BENCH_SMOKE_CHECK_THRESHOLDS ?= 0",
		"BENCH_SMOKE_MAX_COMMAND_NS_OP ?=",
		"BENCH_SMOKE_MAX_TRANSPORT_NS_OP ?=",
		"BENCH_SMOKE_MAX_SERIALIZATION_NS_OP ?=",
		"BENCH_SMOKE_MAX_B_OP ?=",
		"BENCH_SMOKE_MAX_ALLOCS_OP ?=",
		"BENCH_SMOKE_ARTIFACT_DIR ?=",
		"BENCH_SMOKE_BASELINE_JSON ?=",
		"BENCH_SMOKE_MAX_REGRESSION_PCT ?= 20",
		"BENCH_SMOKE_COMPARE_MEMORY ?= 0",
	} {
		if !strings.Contains(makefile, token) {
			t.Fatalf("Makefile missing benchmark smoke threshold token %q", token)
		}
	}
}

func TestCommandFeatureComparisonScriptJoinsBackendArtifacts(t *testing.T) {
	data, err := os.ReadFile("command_feature_benchmark_test.go")
	if err != nil {
		t.Fatalf("ReadFile(command_feature_benchmark_test.go) error = %v", err)
	}
	source := string(data)
	for _, token := range []string{
		"MixedReadHeavy100",
		"MixedWriteHeavy100",
		"benchmarkCommandMixedProfileOps",
	} {
		if !strings.Contains(source, token) {
			t.Fatalf("command_feature_benchmark_test.go missing mixed workload token %q", token)
		}
	}

	data, err = os.ReadFile("scripts/benchmark-command-comparison.sh")
	if err != nil {
		t.Fatalf("ReadFile(benchmark-command-comparison.sh) error = %v", err)
	}
	script := string(data)
	for _, token := range []string{
		"BENCHMARK_ARTIFACT_DIR",
		"hatrie-command-features.tsv",
		"redis-command-features.tsv",
		"tarantool-command-features.tsv",
		"command-feature-comparison.md",
		"Redis/HAT speedup",
		"Tarantool/HAT speedup",
		"seconds_per_10k",
		"Pipelined string write",
		"PipelineBatch16",
		"Mixed read-heavy profile",
		"Mixed write-heavy profile",
		"MixedReadHeavy100",
		"MixedWriteHeavy100",
	} {
		if !strings.Contains(script, token) {
			t.Fatalf("benchmark-command-comparison.sh missing token %q", token)
		}
	}

	data, err = os.ReadFile("Makefile")
	if err != nil {
		t.Fatalf("ReadFile(Makefile) error = %v", err)
	}
	makefile := string(data)
	for _, token := range []string{
		"BENCHMARK_ARTIFACT_DIR ?=",
		"bench-command-comparison",
		"./scripts/benchmark-command-comparison.sh",
	} {
		if !strings.Contains(makefile, token) {
			t.Fatalf("Makefile missing benchmark comparison token %q", token)
		}
	}

	for _, path := range []string{
		"scripts/benchmark-hatrie-command-features.sh",
		"scripts/benchmark-redis-command-features.sh",
		"scripts/tarantool-command-features.lua",
	} {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("ReadFile(%s) error = %v", path, err)
		}
		source := string(data)
		for _, token := range []string{"Mixed read-heavy profile", "Mixed write-heavy profile"} {
			if !strings.Contains(source, token) {
				t.Fatalf("%s missing mixed workload token %q", path, token)
			}
		}
	}
}

func TestBigWinsBenchmarkHarnessCoversArchitecturalBottlenecks(t *testing.T) {
	scriptData, err := os.ReadFile("scripts/benchmark-big-wins.sh")
	if err != nil {
		t.Fatalf("ReadFile(benchmark-big-wins.sh) error = %v", err)
	}
	script := string(scriptData)
	for _, token := range []string{
		"BIG_WINS_BENCH",
		"BenchmarkBigWins",
		"-test.benchmem",
		"/usr/bin/time",
		"Max resident set size",
	} {
		if !strings.Contains(script, token) {
			t.Fatalf("benchmark-big-wins.sh missing token %q", token)
		}
	}

	makeData, err := os.ReadFile("Makefile")
	if err != nil {
		t.Fatalf("ReadFile(Makefile) error = %v", err)
	}
	for _, token := range []string{
		"BIG_WINS_BENCH ?= ^BenchmarkBigWins$",
		"bench-big-wins:",
		"./scripts/benchmark-big-wins.sh",
	} {
		if !strings.Contains(string(makeData), token) {
			t.Fatalf("Makefile missing big-wins benchmark token %q", token)
		}
	}

	sourceData, err := os.ReadFile("big_wins_benchmark_test.go")
	if err != nil {
		t.Fatalf("ReadFile(big_wins_benchmark_test.go) error = %v", err)
	}
	for _, token := range []string{
		"ConcurrentRead",
		"PerKeyMemory",
		"DurableWrite",
		"Snapshot",
		"AntiEntropy",
		"UnaryCommand",
	} {
		if !strings.Contains(string(sourceData), token) {
			t.Fatalf("big_wins_benchmark_test.go missing benchmark token %q", token)
		}
	}
}
