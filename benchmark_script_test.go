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

func TestBenchmarkCISmokeScriptSupportsRegressionThresholds(t *testing.T) {
	data, err := os.ReadFile("scripts/benchmark-ci-smoke.sh")
	if err != nil {
		t.Fatalf("ReadFile(benchmark-ci-smoke.sh) error = %v", err)
	}
	script := string(data)
	for _, token := range []string{
		"BENCH_CI_SMOKE_CHECK_THRESHOLDS",
		"BENCH_CI_SMOKE_MAX_COMMAND_NS_OP",
		"BENCH_CI_SMOKE_MAX_TRANSPORT_NS_OP",
		"BENCH_CI_SMOKE_MAX_SERIALIZATION_NS_OP",
		"BENCH_CI_SMOKE_MAX_B_OP",
		"BENCH_CI_SMOKE_MAX_ALLOCS_OP",
		"ns/op",
		"B/op",
		"allocs/op",
		"Benchmark CI smoke regression guard failed",
	} {
		if !strings.Contains(script, token) {
			t.Fatalf("benchmark-ci-smoke.sh missing threshold token %q", token)
		}
	}

	data, err = os.ReadFile("Makefile")
	if err != nil {
		t.Fatalf("ReadFile(Makefile) error = %v", err)
	}
	makefile := string(data)
	for _, token := range []string{
		"BENCH_CI_SMOKE_CHECK_THRESHOLDS ?= 0",
		"BENCH_CI_SMOKE_MAX_COMMAND_NS_OP ?=",
		"BENCH_CI_SMOKE_MAX_TRANSPORT_NS_OP ?=",
		"BENCH_CI_SMOKE_MAX_SERIALIZATION_NS_OP ?=",
		"BENCH_CI_SMOKE_MAX_B_OP ?=",
		"BENCH_CI_SMOKE_MAX_ALLOCS_OP ?=",
	} {
		if !strings.Contains(makefile, token) {
			t.Fatalf("Makefile missing benchmark CI smoke threshold token %q", token)
		}
	}
}
