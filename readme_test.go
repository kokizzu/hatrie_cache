package hatriecache

import (
	"bytes"
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"
)

func TestREADMEListsCompactStructureCommands(t *testing.T) {
	data, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatalf("ReadFile(README.md) error = %v", err)
	}
	readme := string(data)
	for _, token := range []string{
		"CREATEBF",
		"CREATECF",
		"CREATEXF",
		"ADDXF",
		"BUILDXF",
		"HASXF",
		"INFOXF",
		"CREATERB",
		"CREATESB",
		"ADDSB",
		"REMSB",
		"HASSB",
		"COUNTSB",
		"GETSB",
		"INFOSB",
		"CREATERT",
		"PUTRT",
		"GETRT",
		"DELRT",
		"HASRT",
		"PREFIXRT",
		"INFORT",
		"CREATECMS",
		"CREATEHLL",
		"CREATETOPK",
		"CREATEQ",
		"ADDQ",
		"ESTQ",
		"CREATERS",
		"ADDRS",
		"GETRS",
		"INFORS",
		"CREATEFW",
		"ADDFW",
		"GETFW",
		"SUMFW",
		"RANGEFW",
		"INFOFW",
	} {
		if !strings.Contains(readme, token) {
			t.Fatalf("README.md does not document %s", token)
		}
	}
}

func TestBenchmarkMarkdownTracksExecuteCommand(t *testing.T) {
	commandGroups := executeCommandCases(t)
	data, err := os.ReadFile("BENCHMARK.md")
	if err != nil {
		t.Fatalf("ReadFile(BENCHMARK.md) error = %v", err)
	}
	doc := string(data)
	if want := fmt.Sprintf("%d canonical command groups", len(commandGroups)); !strings.Contains(doc, want) {
		t.Fatalf("BENCHMARK.md does not document command group count %q", want)
	}
	for _, group := range commandGroups {
		canonical := group[0]
		if !strings.Contains(doc, "`"+canonical+"`") {
			t.Fatalf("BENCHMARK.md does not document canonical command %s", canonical)
		}
	}
	for _, token := range []string{
		"https://redis.io/docs/latest/commands/",
		"https://redis.io/docs/latest/develop/data-types/",
		"https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_space/",
		"https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_index/",
		"make command-support",
		"make bench-command-features BENCHTIME=100x",
		"make bench-hatrie-command-features",
		"make bench-hatrie-transport-features",
		"make bench-big-wins BIG_WINS_KEYS=100000 BIG_WINS_OPS=100000 BENCHTIME=1x COUNT=3",
		"BenchmarkCommandTransportFeature/HTTPProtobuf/StringSet",
		"BenchmarkCommandTransportFeature/GRPC/StringGet",
		"BenchmarkCommandTransportFeature/(GRPC|GRPCStream)",
		"PipelinedStreamCommand",
		"18.94x",
		"make bench-redis-command-features REDIS_START_DOCKER=1",
		"make bench-tarantool-command-features TARANTOOL_REQUESTS=1000000",
		"make bench-command-comparison BENCHMARK_ARTIFACT_DIR=build/benchmarks",
		"`command-feature-comparison.md`",
		"BenchmarkCommandFeature/StringSet",
		"BenchmarkCommandFeature/FenwickTreeRange",
		"Redis 7.0.4",
		"Redis seconds / 10k",
		"Tarantool 2.6.0",
		"Tarantool/HAT speedup",
		"Benchmark Results",
		"Memory Summary",
		"HAT-trie vs Tarantool",
		"HAT-trie vs Redis",
		"Raw Tarantool Result",
		"Tarantool 2.6.0-0-g47aa4e01e benchmark: requests=1000000 keyspace=10000",
		"`SET`",
	} {
		if !strings.Contains(doc, token) {
			t.Fatalf("BENCHMARK.md missing comparison/source token %q", token)
		}
	}
}

func TestGeneratedCommandBenchmarkComparisonDoesNotOwnManualSections(t *testing.T) {
	data, err := os.ReadFile("BENCHMARK.md")
	if err != nil {
		t.Fatalf("ReadFile(BENCHMARK.md) error = %v", err)
	}
	doc := string(data)
	start := strings.Index(doc, "<!-- BEGIN GENERATED COMMAND BENCHMARK COMPARISON -->")
	end := strings.Index(doc, "<!-- END GENERATED COMMAND BENCHMARK COMPARISON -->")
	if start < 0 || end <= start {
		t.Fatal("BENCHMARK.md has invalid generated command comparison markers")
	}
	generated := doc[start:end]
	for _, heading := range []string{"## HAT-trie vs Tarantool", "## HAT-trie vs Redis"} {
		if !strings.Contains(generated, heading) {
			t.Fatalf("generated command comparison missing %q", heading)
		}
	}
	for _, heading := range []string{"## Replication Batching Benchmark", "## Journal Delta-First Recovery Benchmark"} {
		if strings.Contains(generated, heading) {
			t.Fatalf("generated command comparison incorrectly owns manual section %q", heading)
		}
	}
}

func TestBenchmarkMarkdownSummarizesMeasuredImprovements(t *testing.T) {
	data, err := os.ReadFile("BENCHMARK.md")
	if err != nil {
		t.Fatalf("ReadFile(BENCHMARK.md) error = %v", err)
	}
	doc := string(data)
	start := strings.Index(doc, "## Measured Improvement Summary")
	if start < 0 {
		t.Fatal("BENCHMARK.md missing measured improvement summary")
	}
	end := strings.Index(doc[start:], "\n### Incremental Anti-Entropy")
	if end < 0 {
		t.Fatal("BENCHMARK.md measured improvement summary has no detail-section boundary")
	}
	summary := doc[start : start+end]
	for _, token := range []string{
		"## Measured Improvement Summary",
		"[HTTP protobuf command wire](README.md#serialization-tradeoffs)",
		"[Binary journal encode](README.md#serialization-tradeoffs)",
		"[Binary journal decode](README.md#serialization-tradeoffs)",
		"[Structured binary journal](README.md#serialization-tradeoffs)",
		"[Structured gzip-best snapshot](README.md#serialization-tradeoffs)",
		"[Binary LevelDB scalar records](README.md#serialization-tradeoffs)",
		"[Binary LevelDB structured records](README.md#serialization-tradeoffs)",
		"[Live string-slot replacement](#live-string-slot-replacement)",
		"[Single-pass default spill-directory initialization](#single-pass-default-spill-directory-initialization)",
		"[Replication request batching](#replication-batching-benchmark)",
		"[Replication routing and encoding](#replication-batching-benchmark)",
		"[Replication page traversal](#replication-page-traversal)",
		"[gRPC replication transport](#replication-transport)",
		"[Bounded gzip writer cache](#replication-compression-tradeoff)",
		"[Four-target replication fanout](#replication-target-fanout)",
		"[Journal delta durability](#journal-delta-first-recovery-benchmark)",
		"[Retained journal catch-up](#journal-delta-first-recovery-benchmark)",
		"[Two-value small-set read](#collection-allocation-follow-up)",
		"[Priority queue push+pop](#collection-allocation-follow-up)",
		"[Direct priority-queue command reads](#compact-priority-queue-items)",
		"[Direct generic priority-queue GET](#compact-priority-queue-items)",
		"[Typed priority-queue pop extraction](#compact-priority-queue-items)",
		"[Allocation-free duplicate packed-map writes](#packed-small-map-storage)",
		"[Map field encoding outside cache lock](#packed-small-map-storage)",
		"[Direct promoted-set JSON](#packed-small-string-set-storage)",
		"[Direct promoted-slice JSON](#packed-small-slice-storage)",
		"[Radix prefix scan](#collection-allocation-follow-up)",
		"[Allocation-free duplicate radix updates](#idempotent-plain-string-radix-updates)",
		"[Order-independent radix bulk insertion](#order-independent-radix-bulk-insertion)",
		"[Borrowed command pair fields](#borrowed-command-pair-fields)",
		"[Flat scalar structured validation](#flat-scalar-structured-validation)",
		"[Flat scalar sequence validation](#flat-scalar-sequence-validation)",
		"[Single-fallback slice payload validation](#flat-scalar-sequence-validation)",
		"[Trailing-fallback whole-sequence validation](#flat-scalar-sequence-validation)",
		"[Direct Count-Min Sketch row loops](#direct-count-min-sketch-row-loops)",
		"[Prepared-result Fenwick updates](#prepared-result-fenwick-updates)",
		"[Prevalidated quantile insertions](#prevalidated-quantile-insertions)",
		"[Compact XOR-filter headers](#compact-xor-filter-headers)",
		"[Linked XOR-filter build queue](#linked-xor-filter-build-queue)",
		"[Order-independent XOR-filter build](#order-independent-xor-filter-build)",
		"[Compact XOR-filter build hash index](#compact-xor-filter-build-hash-index)",
		"[Inline sparse-bitset containers](#inline-sparse-bitset-containers)",
		"[Compact sparse-bitset headers](#compact-sparse-bitset-headers)",
		"[Compact Roaring-container headers](#compact-roaring-container-headers)",
		"[Reservoir sample add](#collection-allocation-follow-up)",
		"[Direct generic reservoir GET](#reservoir-sample-read-materialization)",
		"[Direct generic Top-K GET](#multi-item-top-k-read-materialization)",
		"[Generic Top-K encoding outside read lock](#multi-item-top-k-read-materialization)",
		"[Allocation-free inline Top-K duplicates](#lazy-small-top-k-indexes)",
		"[Bounded generic Top-K scalar updates](#bounded-short-generic-top-k-dispatch)",
		"[Per-key telemetry](#per-key-telemetry-modes)",
		"[Atomic cache-wide telemetry](#atomic-cache-wide-telemetry)",
		"[Concurrent scalar reads](#concurrent-scalar-read-fast-path)",
		"[Durable journal group commit](#durable-journal-group-commit)",
		"[Segmented WAL compaction](#segmented-wal-compaction)",
		"[Point-in-time snapshot capture](#point-in-time-snapshot-capture)",
		"[Compact streaming snapshot capture](#compact-streaming-snapshot-capture)",
		"[Equal-state anti-entropy](#incremental-anti-entropy)",
		"[Sequential gRPC stream](#persistent-grpc-command-stream)",
		"[Pipelined gRPC stream](#persistent-grpc-command-stream)",
		"[Live gRPC micro-batching](#pipelined-live-grpc-replication)",
		"[Bounded lazy outbox restore](#binary-grouped-replication-outbox)",
		"[Election-record status leader lookup](#election-record-status-leader-lookup)",
		"[Normalized replication target precomputation](#normalized-replication-target-precomputation)",
		"[Map-free replication routing snapshots](#map-free-replication-routing-snapshots)",
		"[Direct replication route membership](#direct-replication-route-membership)",
		"[Normalized replication route owners](#direct-replication-route-membership)",
		"[Production native C optimization](#production-native-c-optimization)",
		"73.8% lower",
		"2.38x faster",
		"2.42x faster",
		"11.99x faster",
		"3.71x shorter",
		"49,971x smaller wire",
		"18.94x faster",
		"3.44x fewer batches",
		"118.0x lower heap",
		"1.94x lower heap",
		"8,929x fewer allocs",
	} {
		if !strings.Contains(summary, token) {
			t.Fatalf("BENCHMARK.md missing final architecture summary token %q", token)
		}
	}
}

func TestREADMEDocumentsFlatScalarSequenceValidation(t *testing.T) {
	data, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatalf("ReadFile(README.md) error = %v", err)
	}
	readme := string(data)
	for _, token := range []string{
		"[sequence validation](BENCHMARK.md#flat-scalar-sequence-validation)",
		"allocation-free validation",
		"single nested payload",
		"single trailing nested whole-sequence value",
	} {
		if !strings.Contains(readme, token) {
			t.Fatalf("README.md missing sequence validation token %q", token)
		}
	}
}

func TestBenchmarkMarkdownIndexesRejectedOptimizations(t *testing.T) {
	data, err := os.ReadFile("BENCHMARK.md")
	if err != nil {
		t.Fatalf("ReadFile(BENCHMARK.md) error = %v", err)
	}
	doc := string(data)
	start := strings.Index(doc, "## Rejected Optimization Index")
	if start < 0 {
		t.Fatal("BENCHMARK.md missing rejected optimization index")
	}
	end := strings.Index(doc[start:], "\n<a id=\"delta-only-startup-persistence\"")
	if end < 0 {
		t.Fatal("BENCHMARK.md rejected optimization index has no detail-section boundary")
	}
	index := doc[start : start+end]
	for _, token := range []string{
		"Online generational compaction",
		"Packed-string compaction",
		"Direct Unix telemetry clock",
		"Exact scalar command dispatch",
		"Cgo call annotations",
		"Known-valid-key GET helper",
		"Idempotent string assignment",
		"Temporary packed-map materialization",
		"Single-object storage-header group",
		"Raw owned-directory removal",
		"Boxed packed-set reads",
		"Sentinel-encoded packed-slice length",
		"SetStorage-level promoted JSON dispatch",
		"Priority-queue interface marker",
		"Priority-queue structured fallback scan",
		"Radix-node tag compaction",
		"Fully linked XOR peel order",
		"Direct staged-map XOR build",
		"Marker-only plain XOR staging",
		"Inline sparse bitsets with generic search",
		"Inline Roaring-container values",
		"Local slice view over fixed Roaring bitmap",
		"Compact Count-Min Sketch header",
		"Backward quantile-summary compaction",
		"40-byte Roaring field order",
		"HyperLogLog side allocation",
		"String-keyed Merkle pending set",
		"Merkle table occupancy sentinel",
		"Top-K one-item rewrite",
		"Generic Top-K slice sorter",
		"Generic Top-K structured fallback scan",
		"Dedicated `GETTOPK` lock-release snapshot",
		"Reservoir escaped-value exact sizing",
		"Reservoir sort outside cache lock",
		"Reservoir scalar/batch preparation layouts",
		"Scalar-only out-of-line wrapper",
		"Bloom split-first preparation",
		"Inline Cuckoo scalar wrapper body",
		"Canonical JSON classifier layouts",
		"Mutation response encoding outside cache lock",
		"Shared-lock generic collection GET",
		"Top-K helper lookup",
		"Wider generic Top-K string dispatch",
		"Naive repeated-read scalar routing",
		"Two-command native scalar routing",
		"64 KiB WAL staging",
		"Known-position expiration removal",
		"Proven-absent expiration insertion",
		"Single-pass expiration time comparison",
		"Single-pass expiration update direction",
		"Expiration decision-time capture",
		"Expiration heap hole-sifting",
		"Uppercase EXISTS fast path",
		"Pointer Count-Min increment parser",
		"All-pointer priority parser",
		"Power-of-two ahtable slot mask",
		"Direct native tryget traversal",
		"Post-O3 command normalization",
		"Replication constructor flag",
		"Mixed-page compact descriptors",
		"Ten-page replication aggregation",
		"Copying replication arena",
		"Direct native packed scan",
		"Single-pass legacy repair",
		"Exact protobuf batch coalescing",
		"Carried compact payload estimates",
		"Specialized compact payload estimator",
		"Unchecked normalized replication owners",
		"all candidate code was removed",
	} {
		if !strings.Contains(index, token) {
			t.Fatalf("BENCHMARK.md rejected optimization index missing token %q", token)
		}
	}
}

func TestDocsDescribeExpirationDecisionTimeCaptureRollback(t *testing.T) {
	for _, path := range []string{"README.md", "BENCHMARK.md"} {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("ReadFile(%s) error = %v", path, err)
		}
		doc := string(data)
		for _, token := range []string{"Expiration decision-time capture rollback", "1.047x"} {
			if !strings.Contains(doc, token) {
				t.Fatalf("%s missing expiration decision-time rollback token %q", path, token)
			}
		}
	}
}

func TestDocsDescribeExpirationHeapHoleSiftingRollback(t *testing.T) {
	for _, path := range []string{"README.md", "BENCHMARK.md"} {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("ReadFile(%s) error = %v", path, err)
		}
		doc := string(data)
		for _, token := range []string{"Expiration heap hole-sifting rollback", "1.18x", "1.026x"} {
			if !strings.Contains(doc, token) {
				t.Fatalf("%s missing expiration heap hole-sifting rollback token %q", path, token)
			}
		}
	}
}

func TestDocsDescribeDirectNativeTrygetTraversalRollback(t *testing.T) {
	for _, path := range []string{"README.md", "BENCHMARK.md"} {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("ReadFile(%s) error = %v", path, err)
		}
		doc := string(data)
		for _, token := range []string{"Direct native tryget traversal rollback", "1.032x", "1.003x"} {
			if !strings.Contains(doc, token) {
				t.Fatalf("%s missing direct native tryget rollback token %q", path, token)
			}
		}
	}
}

func TestDocsDescribeProductionNativeCOptimization(t *testing.T) {
	for _, path := range []string{"README.md", "BENCHMARK.md"} {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("ReadFile(%s) error = %v", path, err)
		}
		doc := string(data)
		for _, token := range []string{"Production native C optimization", "-O3", "1.44x", "1.87x", "1.74x", "1.34x", "5,856"} {
			if !strings.Contains(doc, token) {
				t.Fatalf("%s missing production native optimization token %q", path, token)
			}
		}
	}
}

func TestDocsDescribePostO3CommandNormalizationRollback(t *testing.T) {
	for _, path := range []string{"README.md", "BENCHMARK.md"} {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("ReadFile(%s) error = %v", path, err)
		}
		doc := string(data)
		for _, token := range []string{"Post-O3 command normalization rollback", "1.17x", "1.10x", "1.08x"} {
			if !strings.Contains(doc, token) {
				t.Fatalf("%s missing post-O3 command normalization rollback token %q", path, token)
			}
		}
	}
}

func TestDocsDescribeNativeSlotMaskRollback(t *testing.T) {
	for _, path := range []string{"README.md", "BENCHMARK.md"} {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("ReadFile(%s) error = %v", path, err)
		}
		doc := string(data)
		for _, token := range []string{"Native slot-mask rollback", "1.037x"} {
			if !strings.Contains(doc, token) {
				t.Fatalf("%s missing native slot-mask rollback token %q", path, token)
			}
		}
	}
}

func TestDocsDescribePointerExactCommandRequestDispatch(t *testing.T) {
	for _, path := range []string{"README.md", "BENCHMARK.md"} {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("ReadFile(%s) error = %v", path, err)
		}
		doc := string(data)
		for _, token := range []string{"Pointer exact-command request dispatch", "168-byte"} {
			if !strings.Contains(doc, token) {
				t.Fatalf("%s missing pointer request dispatch token %q", path, token)
			}
		}
	}
}

func TestDocsDescribeNarrowPriorityRequestParsing(t *testing.T) {
	for _, path := range []string{"README.md", "BENCHMARK.md"} {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("ReadFile(%s) error = %v", path, err)
		}
		doc := string(data)
		for _, token := range []string{"Narrow priority request parsing", "14.75 to 13.41 ns"} {
			if !strings.Contains(doc, token) {
				t.Fatalf("%s missing priority request parsing token %q", path, token)
			}
		}
	}
}

func TestDocsDescribeCarriedExpirationUpdateIndex(t *testing.T) {
	for _, path := range []string{"README.md", "BENCHMARK.md"} {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("ReadFile(%s) error = %v", path, err)
		}
		doc := string(data)
		for _, token := range []string{"Carried expiration update index", "225.0 to 199.8 ns"} {
			if !strings.Contains(doc, token) {
				t.Fatalf("%s missing carried expiration index token %q", path, token)
			}
		}
	}
}

func TestDocsDescribeGenericBloomScalarAdditions(t *testing.T) {
	readmeData, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatalf("ReadFile(README.md) error = %v", err)
	}
	for _, token := range []string{
		"[Bloom scalar-add measurements](BENCHMARK.md#generic-bloom-filter-scalar-additions)",
		"make bench-bloom-scalar BENCHTIME=1s COUNT=7",
	} {
		if !strings.Contains(string(readmeData), token) {
			t.Fatalf("README.md missing Bloom scalar-add token %q", token)
		}
	}

	benchmarkData, err := os.ReadFile("BENCHMARK.md")
	if err != nil {
		t.Fatalf("ReadFile(BENCHMARK.md) error = %v", err)
	}
	for _, token := range []string{
		"Generic Bloom-Filter Scalar Additions",
		"1.39x faster",
		"Bloom split-first preparation",
		"10,947 to 11,109 ns",
	} {
		if !strings.Contains(string(benchmarkData), token) {
			t.Fatalf("BENCHMARK.md missing Bloom scalar-add token %q", token)
		}
	}
}

func TestDocsDescribeGenericCuckooScalarOperations(t *testing.T) {
	readmeData, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatalf("ReadFile(README.md) error = %v", err)
	}
	for _, token := range []string{
		"[Cuckoo scalar-add measurements](BENCHMARK.md#generic-cuckoo-filter-scalar-additions)",
		"[Cuckoo scalar-delete measurements](BENCHMARK.md#generic-cuckoo-filter-scalar-deletions)",
		"make bench-cuckoo-scalar BENCHTIME=1s COUNT=7",
	} {
		if !strings.Contains(string(readmeData), token) {
			t.Fatalf("README.md missing Cuckoo scalar-add token %q", token)
		}
	}

	benchmarkData, err := os.ReadFile("BENCHMARK.md")
	if err != nil {
		t.Fatalf("ReadFile(BENCHMARK.md) error = %v", err)
	}
	for _, token := range []string{
		"Generic Cuckoo-Filter Scalar Additions",
		"1.58x faster",
		"Inline Cuckoo scalar wrapper body",
		"9,834 versus 9,724 ns",
		"Generic Cuckoo-Filter Scalar Deletions",
		"280.3 ns; 32 B; 2 allocs",
		"11,669 ns; 5,248 B; 129 allocs",
		"Cuckoo scalar-delete dispatch layouts",
		"Cuckoo variadic scalar-add preparation",
		"364.9/1,639/11,539 ns",
		"408.0/1,768 ns",
	} {
		if !strings.Contains(string(benchmarkData), token) {
			t.Fatalf("BENCHMARK.md missing Cuckoo scalar-add token %q", token)
		}
	}
}

func TestDocsDescribeCanonicalJSONCommandFastPaths(t *testing.T) {
	readmeData, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatalf("ReadFile(README.md) error = %v", err)
	}
	for _, token := range []string{
		"[canonical JSON command measurements](BENCHMARK.md#canonical-json-command-fast-paths)",
		"1.02x-1.14x faster",
	} {
		if !strings.Contains(string(readmeData), token) {
			t.Fatalf("README.md missing canonical JSON command token %q", token)
		}
	}

	benchmarkData, err := os.ReadFile("BENCHMARK.md")
	if err != nil {
		t.Fatalf("ReadFile(BENCHMARK.md) error = %v", err)
	}
	for _, token := range []string{
		"Canonical JSON Command Fast Paths",
		"make bench-command-json-string BENCHTIME=1s COUNT=7",
		"99.24 ns; 0 B; 0 allocs",
		"Canonical JSON classifier layouts",
		"16.81/22.42/83.78 ns",
	} {
		if !strings.Contains(string(benchmarkData), token) {
			t.Fatalf("BENCHMARK.md missing canonical JSON command token %q", token)
		}
	}
}

func TestDocsDescribeAllocationFreeCanonicalStringLookups(t *testing.T) {
	readmeData, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatalf("ReadFile(README.md) error = %v", err)
	}
	for _, token := range []string{
		"[public canonical-string lookup measurements](BENCHMARK.md#allocation-free-public-canonical-string-lookups)",
		"1.91x-2.33x faster",
		"make bench-canonical-string-lookups BENCHTIME=1s COUNT=7",
	} {
		if !strings.Contains(string(readmeData), token) {
			t.Fatalf("README.md missing public canonical-string lookup token %q", token)
		}
	}

	benchmarkData, err := os.ReadFile("BENCHMARK.md")
	if err != nil {
		t.Fatalf("ReadFile(BENCHMARK.md) error = %v", err)
	}
	for _, token := range []string{
		"Allocation-Free Public Canonical-String Lookups",
		"121.7 ns; 16 B; 1 alloc",
		"63.73 ns; 0 B; 0 allocs",
		"Public XOR canonical-string lookup",
		"Public Top-K canonical-string lookup",
		"^BenchmarkPublicCanonicalStringLookupFallbackAlternating$",
	} {
		if !strings.Contains(string(benchmarkData), token) {
			t.Fatalf("BENCHMARK.md missing public canonical-string lookup token %q", token)
		}
	}
}

func TestDocsDescribeOptionalCommandBenchmarkFixtureAllocations(t *testing.T) {
	readmeData, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatalf("ReadFile(README.md) error = %v", err)
	}
	for _, token := range []string{
		"[optional command fixture audit](BENCHMARK.md#optional-command-benchmark-fixture-allocations)",
		"allocation-free for a reused request",
	} {
		if !strings.Contains(string(readmeData), token) {
			t.Fatalf("README.md missing optional command fixture token %q", token)
		}
	}

	benchmarkData, err := os.ReadFile("BENCHMARK.md")
	if err != nil {
		t.Fatalf("ReadFile(BENCHMARK.md) error = %v", err)
	}
	for _, token := range []string{
		"Optional Command Benchmark Fixture Allocations",
		"390.0 ns; 8 B; 1 alloc",
		"358.1 ns; 0 B; 0 allocs",
		"Optional command integer representation",
	} {
		if !strings.Contains(string(benchmarkData), token) {
			t.Fatalf("BENCHMARK.md missing optional command fixture token %q", token)
		}
	}
}

func TestDocsDescribeTimedCommandBenchmarkHelperCorrection(t *testing.T) {
	readmeData, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatalf("ReadFile(README.md) error = %v", err)
	}
	for _, token := range []string{
		"[timed command benchmark helper audit](BENCHMARK.md#timed-command-benchmark-helper-overhead)",
		"benchmark-harness correction, not a production speedup",
	} {
		if !strings.Contains(string(readmeData), token) {
			t.Fatalf("README.md missing timed command benchmark helper token %q", token)
		}
	}

	benchmarkData, err := os.ReadFile("BENCHMARK.md")
	if err != nil {
		t.Fatalf("ReadFile(BENCHMARK.md) error = %v", err)
	}
	for _, token := range []string{
		"Timed Command Benchmark Helper Overhead",
		"314.4 ns | 172.1 ns | 1.83x",
		"300.7 ns | 158.7 ns | 1.90x",
		"only inside benchmark failure branches",
	} {
		if !strings.Contains(string(benchmarkData), token) {
			t.Fatalf("BENCHMARK.md missing timed command benchmark helper token %q", token)
		}
	}
}

func TestDocsDescribeGenericHyperLogLogScalarAdditions(t *testing.T) {
	readmeData, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatalf("ReadFile(README.md) error = %v", err)
	}
	for _, token := range []string{
		"[HyperLogLog scalar-add measurements](BENCHMARK.md#generic-hyperloglog-scalar-additions)",
		"make bench-hll-scalar BENCHTIME=1s COUNT=7",
	} {
		if !strings.Contains(string(readmeData), token) {
			t.Fatalf("README.md missing HyperLogLog scalar-add token %q", token)
		}
	}

	benchmarkData, err := os.ReadFile("BENCHMARK.md")
	if err != nil {
		t.Fatalf("ReadFile(BENCHMARK.md) error = %v", err)
	}
	for _, token := range []string{
		"Generic HyperLogLog Scalar Additions",
		"1.52x faster",
		"94.58 to 55.36 ns",
		"9,671 ns before and 9,614 ns after",
	} {
		if !strings.Contains(string(benchmarkData), token) {
			t.Fatalf("BENCHMARK.md missing HyperLogLog scalar-add token %q", token)
		}
	}
}

func TestDocsDescribeGenericCountMinScalarAdditions(t *testing.T) {
	readmeData, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatalf("ReadFile(README.md) error = %v", err)
	}
	for _, token := range []string{
		"[Count-Min scalar-add measurements](BENCHMARK.md#generic-count-min-sketch-scalar-additions)",
		"make bench-cms-scalar BENCHTIME=1s COUNT=7",
	} {
		if !strings.Contains(string(readmeData), token) {
			t.Fatalf("README.md missing Count-Min scalar-add token %q", token)
		}
	}

	benchmarkData, err := os.ReadFile("BENCHMARK.md")
	if err != nil {
		t.Fatalf("ReadFile(BENCHMARK.md) error = %v", err)
	}
	for _, token := range []string{
		"Generic Count-Min Sketch Scalar Additions",
		"1.45x faster",
		"120.2 to 71.47 ns",
		"10,946 versus 11,016 ns",
		"Count-Min clustered scalar helper",
	} {
		if !strings.Contains(string(benchmarkData), token) {
			t.Fatalf("BENCHMARK.md missing Count-Min scalar-add token %q", token)
		}
	}
}

func TestDocsDescribeDirectGenericScalarSetKeys(t *testing.T) {
	readmeData, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatalf("ReadFile(README.md) error = %v", err)
	}
	for _, token := range []string{
		"[generic scalar set measurements](BENCHMARK.md#direct-generic-scalar-set-keys)",
		"make bench-set-scalar-generic BENCHTIME=1s COUNT=7",
	} {
		if !strings.Contains(string(readmeData), token) {
			t.Fatalf("README.md missing generic scalar set token %q", token)
		}
	}

	benchmarkData, err := os.ReadFile("BENCHMARK.md")
	if err != nil {
		t.Fatalf("ReadFile(BENCHMARK.md) error = %v", err)
	}
	for _, token := range []string{
		"Direct Generic Scalar Set Keys",
		"288.2 to 239.1 ns",
		"167.5 to 131.3 ns",
		"121.2 versus 121.9 ns",
	} {
		if !strings.Contains(string(benchmarkData), token) {
			t.Fatalf("BENCHMARK.md missing generic scalar set token %q", token)
		}
	}
}

func TestDocsDescribeDirectScalarPriorityQueuePushes(t *testing.T) {
	readmeData, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatalf("ReadFile(README.md) error = %v", err)
	}
	for _, token := range []string{
		"[scalar push measurements](BENCHMARK.md#direct-scalar-priority-queue-pushes)",
		"make bench-priority-queue-scalar BENCHTIME=1s COUNT=7",
	} {
		if !strings.Contains(string(readmeData), token) {
			t.Fatalf("README.md missing scalar priority-queue token %q", token)
		}
	}

	benchmarkData, err := os.ReadFile("BENCHMARK.md")
	if err != nil {
		t.Fatalf("ReadFile(BENCHMARK.md) error = %v", err)
	}
	for _, token := range []string{
		"Direct Scalar Priority-Queue Pushes",
		"156.6 ns; 0 B; 0 allocs | 146.7 ns",
		"600.1 ns; 64 B; 3 allocs | 585.6 ns",
		"23,267 ns; 12,416 B; 4 allocs | 22,897 ns",
		"Priority-queue scalar dispatch placements",
	} {
		if !strings.Contains(string(benchmarkData), token) {
			t.Fatalf("BENCHMARK.md missing scalar priority-queue token %q", token)
		}
	}
}

func TestDocsDescribeBoundedShortGenericTopKDispatch(t *testing.T) {
	readmeData, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatalf("ReadFile(README.md) error = %v", err)
	}
	for _, token := range []string{
		"[bounded Top-K dispatch measurements](BENCHMARK.md#bounded-short-generic-top-k-dispatch)",
		"make bench-topk-scalar BENCHTIME=1s COUNT=7",
	} {
		if !strings.Contains(string(readmeData), token) {
			t.Fatalf("README.md missing bounded Top-K dispatch token %q", token)
		}
	}

	benchmarkData, err := os.ReadFile("BENCHMARK.md")
	if err != nil {
		t.Fatalf("ReadFile(BENCHMARK.md) error = %v", err)
	}
	for _, token := range []string{
		"BenchmarkTopKGenericScalarDispatch",
		"9.59x faster",
		"1.60x faster",
		"1.44x",
		"four-byte limit was benchmark-selected",
	} {
		if !strings.Contains(string(benchmarkData), token) {
			t.Fatalf("BENCHMARK.md missing bounded Top-K dispatch token %q", token)
		}
	}
}

func TestDocsDescribeLazyEmptyMerkleTableBacking(t *testing.T) {
	readmeData, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatalf("ReadFile(README.md) error = %v", err)
	}
	for _, token := range []string{
		"[lazy empty Merkle table](BENCHMARK.md#lazy-empty-merkle-table-backing)",
		"allocates its open-addressing table on the first indexed key",
		"[stateless empty Merkle root](BENCHMARK.md#stateless-empty-merkle-root)",
		"retains no Merkle index",
	} {
		if !strings.Contains(string(readmeData), token) {
			t.Fatalf("README.md missing lazy Merkle token %q", token)
		}
	}

	benchmarkData, err := os.ReadFile("BENCHMARK.md")
	if err != nil {
		t.Fatalf("ReadFile(BENCHMARK.md) error = %v", err)
	}
	for _, token := range []string{
		"BenchmarkReplicationMerkleEmptyIndexAllocation",
		"BenchmarkReplicationMerkleEmptySnapshot",
		"2.08x faster",
		"19.56x faster",
		"10.58x lower heap",
		"4x fewer allocations",
		"16,384",
	} {
		if !strings.Contains(string(benchmarkData), token) {
			t.Fatalf("BENCHMARK.md missing lazy Merkle token %q", token)
		}
	}
}

func TestDocsDescribeGroupedStorageHeaders(t *testing.T) {
	readmeData, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatalf("ReadFile(README.md) error = %v", err)
	}
	for _, token := range []string{
		"[grouped storage headers](BENCHMARK.md#grouped-storage-headers)",
		"25 to 8 allocations",
	} {
		if !strings.Contains(string(readmeData), token) {
			t.Fatalf("README.md missing grouped storage token %q", token)
		}
	}

	benchmarkData, err := os.ReadFile("BENCHMARK.md")
	if err != nil {
		t.Fatalf("ReadFile(BENCHMARK.md) error = %v", err)
	}
	for _, token := range []string{
		"BenchmarkHatTrieConstruction",
		"3.13x fewer allocations",
		"3,360 B",
	} {
		if !strings.Contains(string(benchmarkData), token) {
			t.Fatalf("BENCHMARK.md missing grouped storage token %q", token)
		}
	}
}

func TestDocsDescribeSinglePassDefaultSpillDirectoryInitialization(t *testing.T) {
	readmeData, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatalf("ReadFile(README.md) error = %v", err)
	}
	for _, token := range []string{
		"[single-pass initialization](BENCHMARK.md#single-pass-default-spill-directory-initialization)",
		"removes two allocations plus 240 B",
		"make bench-default-construction BENCHTIME=10000x COUNT=7",
	} {
		if !strings.Contains(string(readmeData), token) {
			t.Fatalf("README.md missing default spill-directory token %q", token)
		}
	}

	benchmarkData, err := os.ReadFile("BENCHMARK.md")
	if err != nil {
		t.Fatalf("ReadFile(BENCHMARK.md) error = %v", err)
	}
	for _, token := range []string{
		"Single-Pass Default Spill-Directory Initialization",
		"69,483 ns",
		"66,639 ns",
		"3,391 B",
		"3,151 B",
		"18,882 B and 220 allocations",
	} {
		if !strings.Contains(string(benchmarkData), token) {
			t.Fatalf("BENCHMARK.md missing default spill-directory token %q", token)
		}
	}
}

func TestDocsDescribeDeferredOptionalMaps(t *testing.T) {
	readmeData, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatalf("ReadFile(README.md) error = %v", err)
	}
	for _, token := range []string{
		"[deferred optional maps](BENCHMARK.md#deferred-optional-maps)",
		"8 to 6 allocations",
	} {
		if !strings.Contains(string(readmeData), token) {
			t.Fatalf("README.md missing deferred optional-map token %q", token)
		}
	}

	benchmarkData, err := os.ReadFile("BENCHMARK.md")
	if err != nil {
		t.Fatalf("ReadFile(BENCHMARK.md) error = %v", err)
	}
	for _, token := range []string{
		"BenchmarkHatTrieOptionalMapLifecycle",
		"96 B lower",
		"1.33x fewer allocations",
		"CPU-neutral within 0.2%",
	} {
		if !strings.Contains(string(benchmarkData), token) {
			t.Fatalf("BENCHMARK.md missing deferred optional-map token %q", token)
		}
	}
}

func TestDocsDescribeLazyRateLimiterShardMaps(t *testing.T) {
	readmeData, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatalf("ReadFile(README.md) error = %v", err)
	}
	if token := "[lazy rate-limiter shard maps](BENCHMARK.md#lazy-rate-limiter-shard-maps)"; !strings.Contains(string(readmeData), token) {
		t.Fatalf("README.md missing lazy rate-limiter token %q", token)
	}

	benchmarkData, err := os.ReadFile("BENCHMARK.md")
	if err != nil {
		t.Fatalf("ReadFile(BENCHMARK.md) error = %v", err)
	}
	for _, token := range []string{
		"BenchmarkRateLimiterFirstClientLifecycle",
		"BenchmarkRateLimiterAllowSameClientAlternating",
		"6.47x faster",
		"2.87x lower heap",
		"22x fewer allocations",
		"steady-state admission",
	} {
		if !strings.Contains(string(benchmarkData), token) {
			t.Fatalf("BENCHMARK.md missing lazy rate-limiter token %q", token)
		}
	}
}

func TestDocsDescribeLazyReplicationGRPCSessionMaps(t *testing.T) {
	readmeData, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatalf("ReadFile(README.md) error = %v", err)
	}
	if token := "[lazy gRPC session maps](BENCHMARK.md#lazy-grpc-session-maps)"; !strings.Contains(string(readmeData), token) {
		t.Fatalf("README.md missing lazy gRPC session token %q", token)
	}

	benchmarkData, err := os.ReadFile("BENCHMARK.md")
	if err != nil {
		t.Fatalf("ReadFile(BENCHMARK.md) error = %v", err)
	}
	for _, token := range []string{
		"BenchmarkReplicationGRPCSessionLifecycle",
		"2.98x-3.04x faster",
		"3.25x lower heap",
		"4x fewer allocations",
		"live sessions never allocate sticky fallback",
	} {
		if !strings.Contains(string(benchmarkData), token) {
			t.Fatalf("BENCHMARK.md missing lazy gRPC session token %q", token)
		}
	}
}

func TestDocsDescribeDirectSingleTargetGRPCSyncDispatch(t *testing.T) {
	readmeData, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatalf("ReadFile(README.md) error = %v", err)
	}
	if token := "[direct gRPC dispatch](BENCHMARK.md#direct-single-target-grpc-sync-dispatch)"; !strings.Contains(string(readmeData), token) {
		t.Fatalf("README.md missing direct gRPC dispatch token %q", token)
	}

	benchmarkData, err := os.ReadFile("BENCHMARK.md")
	if err != nil {
		t.Fatalf("ReadFile(BENCHMARK.md) error = %v", err)
	}
	for _, token := range []string{
		"BenchmarkReplicationGRPCSingleTaskGroupPlanning",
		"1.33x faster",
		"2.10x lower heap",
		"Four distinct targets",
		"CPU neutral within 0.3%",
	} {
		if !strings.Contains(string(benchmarkData), token) {
			t.Fatalf("BENCHMARK.md missing direct gRPC dispatch token %q", token)
		}
	}
}

func TestDocsDescribeNormalizedTopologyStoreRouting(t *testing.T) {
	readmeData, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatalf("ReadFile(README.md) error = %v", err)
	}
	if token := "[normalized topology routing](BENCHMARK.md#normalized-topology-store-routing)"; !strings.Contains(string(readmeData), token) {
		t.Fatalf("README.md missing normalized topology routing token %q", token)
	}

	benchmarkData, err := os.ReadFile("BENCHMARK.md")
	if err != nil {
		t.Fatalf("ReadFile(BENCHMARK.md) error = %v", err)
	}
	for _, token := range []string{
		"BenchmarkTopologyStoreRouteAlternating",
		"4,096-key sweep",
		"1.80x faster",
		"3.54x faster",
		"9.17x lower heap",
		"4.50x fewer allocations",
		"Public `ClusterTopology.RouteForKey` remains unchanged",
	} {
		if !strings.Contains(string(benchmarkData), token) {
			t.Fatalf("BENCHMARK.md missing normalized topology routing token %q", token)
		}
	}
}

func TestDocsDescribeDirectElectionKeyRouting(t *testing.T) {
	readmeData, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatalf("ReadFile(README.md) error = %v", err)
	}
	if token := "[direct election routing](BENCHMARK.md#direct-election-key-routing)"; !strings.Contains(string(readmeData), token) {
		t.Fatalf("README.md missing direct election routing token %q", token)
	}

	benchmarkData, err := os.ReadFile("BENCHMARK.md")
	if err != nil {
		t.Fatalf("ReadFile(BENCHMARK.md) error = %v", err)
	}
	for _, token := range []string{
		"BenchmarkElectionStoreLeaderForKeyAlternating",
		"4,096 keys per topology",
		"5.08x faster",
		"13.19x lower heap",
		"6x fewer allocations",
		"no topology lock is held while waiting",
	} {
		if !strings.Contains(string(benchmarkData), token) {
			t.Fatalf("BENCHMARK.md missing direct election routing token %q", token)
		}
	}
}

func TestDocsDescribeAllocationFreeElectionNodeUpdates(t *testing.T) {
	readmeData, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatalf("ReadFile(README.md) error = %v", err)
	}
	if token := "[allocation-free node validation](BENCHMARK.md#allocation-free-election-node-updates)"; !strings.Contains(string(readmeData), token) {
		t.Fatalf("README.md missing allocation-free election node-update token %q", token)
	}

	benchmarkData, err := os.ReadFile("BENCHMARK.md")
	if err != nil {
		t.Fatalf("ReadFile(BENCHMARK.md) error = %v", err)
	}
	for _, token := range []string{
		"BenchmarkElectionStoreNodeUpdateAlternating",
		"9.15x faster",
		"standalone 33-36 ns final medians",
		"0 B; 0 allocs",
		"Node-ID trimming, unknown-node errors",
	} {
		if !strings.Contains(string(benchmarkData), token) {
			t.Fatalf("BENCHMARK.md missing allocation-free election node-update token %q", token)
		}
	}
}

func TestDocsDescribeNormalizedElectionStatusGeneration(t *testing.T) {
	readmeData, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatalf("ReadFile(README.md) error = %v", err)
	}
	if token := "[normalized generation output](BENCHMARK.md#normalized-election-status-generation)"; !strings.Contains(string(readmeData), token) {
		t.Fatalf("README.md missing normalized election-status token %q", token)
	}

	benchmarkData, err := os.ReadFile("BENCHMARK.md")
	if err != nil {
		t.Fatalf("ReadFile(BENCHMARK.md) error = %v", err)
	}
	for _, token := range []string{
		"BenchmarkElectionStoreStatusAlternating",
		"2.36x faster",
		"2.54x lower heap",
		"3.25x fewer allocations",
		"every response-owned slice remained present",
	} {
		if !strings.Contains(string(benchmarkData), token) {
			t.Fatalf("BENCHMARK.md missing normalized election-status token %q", token)
		}
	}
}

func TestDocsDescribeElectionRecordStatusLeaderLookup(t *testing.T) {
	readmeData, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatalf("ReadFile(README.md) error = %v", err)
	}
	if token := "[election-record leader lookup](BENCHMARK.md#election-record-status-leader-lookup)"; !strings.Contains(string(readmeData), token) {
		t.Fatalf("README.md missing election-record status token %q", token)
	}

	benchmarkData, err := os.ReadFile("BENCHMARK.md")
	if err != nil {
		t.Fatalf("ReadFile(BENCHMARK.md) error = %v", err)
	}
	for _, token := range []string{
		"BenchmarkElectionStoreStatusActiveMapAlternating",
		"64-node shared-primary",
		"1.58x faster",
		"1.32x faster",
		"3,544 fewer heap bytes",
		"old=184 new=184 actual=184",
		"Maintenance generations retain",
	} {
		if !strings.Contains(string(benchmarkData), token) {
			t.Fatalf("BENCHMARK.md missing election-record status token %q", token)
		}
	}
}

func TestDocsDescribeCachedReplicationRoutingFingerprint(t *testing.T) {
	readmeData, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatalf("ReadFile(README.md) error = %v", err)
	}
	if token := "[cached topology fingerprint](BENCHMARK.md#cached-replication-routing-fingerprint)"; !strings.Contains(string(readmeData), token) {
		t.Fatalf("README.md missing cached topology fingerprint token %q", token)
	}

	benchmarkData, err := os.ReadFile("BENCHMARK.md")
	if err != nil {
		t.Fatalf("ReadFile(BENCHMARK.md) error = %v", err)
	}
	for _, token := range []string{
		"BenchmarkReplicationRoutingSnapshotFingerprintAlternating",
		"2.00x faster",
		"2.04x faster",
		"3.71x fewer allocations",
		"3.79x fewer allocations",
		"Topology installation still pays exactly one fingerprint",
	} {
		if !strings.Contains(string(benchmarkData), token) {
			t.Fatalf("BENCHMARK.md missing cached topology fingerprint token %q", token)
		}
	}
}

func TestDocsDescribeNormalizedReplicationTargetPrecomputation(t *testing.T) {
	readmeData, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatalf("ReadFile(README.md) error = %v", err)
	}
	if token := "[normalized target precomputation](BENCHMARK.md#normalized-replication-target-precomputation)"; !strings.Contains(string(readmeData), token) {
		t.Fatalf("README.md missing normalized replication-target token %q", token)
	}

	benchmarkData, err := os.ReadFile("BENCHMARK.md")
	if err != nil {
		t.Fatalf("ReadFile(BENCHMARK.md) error = %v", err)
	}
	for _, token := range []string{
		"BenchmarkPrecomputedReplicationTargetsDedupAlternating",
		"TestPrecomputedReplicationTargetsMatchDeduplicatingControl",
		"1.15x faster",
		"1.10x faster",
		"50 fewer heap bytes",
		"one fewer allocation",
		"duplicate primary/replica owners",
	} {
		if !strings.Contains(string(benchmarkData), token) {
			t.Fatalf("BENCHMARK.md missing normalized replication-target token %q", token)
		}
	}
}

func TestDocsDescribeMapFreeReplicationRoutingSnapshots(t *testing.T) {
	readmeData, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatalf("ReadFile(README.md) error = %v", err)
	}
	if token := "[map-free](BENCHMARK.md#map-free-replication-routing-snapshots)"; !strings.Contains(string(readmeData), token) {
		t.Fatalf("README.md missing map-free replication routing token %q", token)
	}

	benchmarkData, err := os.ReadFile("BENCHMARK.md")
	if err != nil {
		t.Fatalf("ReadFile(BENCHMARK.md) error = %v", err)
	}
	for _, token := range []string{
		"BenchmarkReplicationRoutingSnapshotSortedNodesAlternating",
		"BenchmarkReplicationRoutingSnapshotNodeIndex",
		"TestReplicationRoutingSnapshotSortedNodesMatchNodeMap",
		"1.34x faster",
		"16,472",
		"68,232 B",
		"four fewer allocations",
		"test-only wrapper",
	} {
		if !strings.Contains(string(benchmarkData), token) {
			t.Fatalf("BENCHMARK.md missing map-free replication routing token %q", token)
		}
	}
}

func TestDocsDescribeCanonicalReplicationOwnerSlices(t *testing.T) {
	readmeData, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatalf("ReadFile(README.md) error = %v", err)
	}
	if token := "[canonical owner representation](BENCHMARK.md#canonical-replication-owner-slices)"; !strings.Contains(string(readmeData), token) {
		t.Fatalf("README.md missing canonical replication-owner token %q", token)
	}

	benchmarkData, err := os.ReadFile("BENCHMARK.md")
	if err != nil {
		t.Fatalf("ReadFile(BENCHMARK.md) error = %v", err)
	}
	for _, token := range []string{
		"BenchmarkReplicationRoutingLeaderCandidatesConstructionAlternating",
		"BenchmarkReplicationRoutingLeaderCandidatesRouteAlternating",
		"TestReplicationRoutingSnapshotLeaderCandidatesMatchOwnerSlices",
		"4,864 fewer B",
		"65 fewer allocations",
		"1.07x faster",
		"24-byte slice header",
		"hash routes 1.02x-1.03x slower",
	} {
		if !strings.Contains(string(benchmarkData), token) {
			t.Fatalf("BENCHMARK.md missing canonical replication-owner token %q", token)
		}
	}
}

func TestDocsDescribeSparseReplicationLivenessExceptions(t *testing.T) {
	readmeData, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatalf("ReadFile(README.md) error = %v", err)
	}
	if token := "[sparse liveness exceptions](BENCHMARK.md#sparse-replication-liveness-exceptions)"; !strings.Contains(string(readmeData), token) {
		t.Fatalf("README.md missing sparse replication-liveness token %q", token)
	}

	benchmarkData, err := os.ReadFile("BENCHMARK.md")
	if err != nil {
		t.Fatalf("ReadFile(BENCHMARK.md) error = %v", err)
	}
	for _, token := range []string{
		"BenchmarkReplicationRoutingInactiveNodesConstructionAlternating",
		"BenchmarkReplicationRoutingInactiveNodeMembershipAlternating",
		"TestReplicationRoutingSnapshotSparseInactiveMatchesOnlineControl",
		"3,544 fewer B",
		"1.39x faster",
		"1.26x faster",
		"Offline, timeout, and maintenance",
	} {
		if !strings.Contains(string(benchmarkData), token) {
			t.Fatalf("BENCHMARK.md missing sparse replication-liveness token %q", token)
		}
	}
}

func TestBenchmarkDocsRecordRejectedGroupedReplicationTargetBacking(t *testing.T) {
	benchmarkData, err := os.ReadFile("BENCHMARK.md")
	if err != nil {
		t.Fatalf("ReadFile(BENCHMARK.md) error = %v", err)
	}
	for _, token := range []string{
		"Standalone grouped replication target backing",
		"#grouped-replication-target-backing-rollback",
		"TestReplicationRoutingSnapshotPackedTargetsCandidateMatchesPerShardBacking",
		"BenchmarkReplicationRoutingPackedTargetsConstructionAlternating",
		"added 128/256 heap B",
		"1.18x slower",
		"combined owner/target layout",
	} {
		if !strings.Contains(string(benchmarkData), token) {
			t.Fatalf("BENCHMARK.md missing rejected grouped-target token %q", token)
		}
	}
}

func TestDocsDescribeAdaptiveReplicationTargetSorting(t *testing.T) {
	readmeData, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatalf("ReadFile(README.md) error = %v", err)
	}
	if token := "[adaptive target sorting](BENCHMARK.md#adaptive-replication-target-sorting)"; !strings.Contains(string(readmeData), token) {
		t.Fatalf("README.md missing adaptive replication-target sort token %q", token)
	}

	benchmarkData, err := os.ReadFile("BENCHMARK.md")
	if err != nil {
		t.Fatalf("ReadFile(BENCHMARK.md) error = %v", err)
	}
	for _, token := range []string{
		"TestReplicationRoutingSnapshotAdaptiveTargetSortMatchesReflectiveControl",
		"BenchmarkReplicationRoutingAdaptiveTargetSortConstructionAlternating",
		"slices.SortFunc",
		"11,776 fewer B",
		"192 fewer allocations",
		"31/63-target",
		"1.025x slower",
	} {
		if !strings.Contains(string(benchmarkData), token) {
			t.Fatalf("BENCHMARK.md missing adaptive replication-target sort token %q", token)
		}
	}
}

func TestDocsDescribeBorrowedReplicationTopologyGeneration(t *testing.T) {
	readmeData, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatalf("ReadFile(README.md) error = %v", err)
	}
	if token := "[borrow the immutable normalized topology generation](BENCHMARK.md#borrowed-replication-topology-generation)"; !strings.Contains(string(readmeData), token) {
		t.Fatalf("README.md missing borrowed replication-topology token %q", token)
	}

	benchmarkData, err := os.ReadFile("BENCHMARK.md")
	if err != nil {
		t.Fatalf("ReadFile(BENCHMARK.md) error = %v", err)
	}
	for _, token := range []string{
		"TestReplicationRoutingSnapshotBorrowedTopologyMatchesClonedGeneration",
		"TestReplicationRoutingSnapshotBorrowedTopologyConcurrentGenerationReplacement",
		"BenchmarkReplicationRoutingBorrowedTopologyConstructionAlternating",
		"12,032-byte",
		"66 fewer allocations",
		"1.50x lower heap",
		"public topology/routing APIs retain cloned ownership",
	} {
		if !strings.Contains(string(benchmarkData), token) {
			t.Fatalf("BENCHMARK.md missing borrowed replication-topology token %q", token)
		}
	}
}

func TestDocsDescribeGroupedReplicationOwnerBacking(t *testing.T) {
	readmeData, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatalf("ReadFile(README.md) error = %v", err)
	}
	if token := "[four-shard owner backing](BENCHMARK.md#grouped-replication-owner-backing)"; !strings.Contains(string(readmeData), token) {
		t.Fatalf("README.md missing grouped replication-owner token %q", token)
	}

	benchmarkData, err := os.ReadFile("BENCHMARK.md")
	if err != nil {
		t.Fatalf("ReadFile(BENCHMARK.md) error = %v", err)
	}
	for _, token := range []string{
		"Four-shard replication owner backing",
		"TestReplicationRoutingSnapshotPackedOwnerBackingMatchesPerShardOwners",
		"BenchmarkReplicationRoutingPackedOwnerBackingConstructionAlternating",
		"128/256/128",
		"cumulative heap bytes",
		"23,445 ns; 30,208 B; 82 allocs",
		"48 fewer allocations with identical heap",
		"single-shard construction path",
	} {
		if !strings.Contains(string(benchmarkData), token) {
			t.Fatalf("BENCHMARK.md missing grouped replication-owner token %q", token)
		}
	}
}

func TestDocsDescribeCombinedReplicationOwnerTargetBacking(t *testing.T) {
	readmeData, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatalf("ReadFile(README.md) error = %v", err)
	}
	if token := "[combined owner/target backing](BENCHMARK.md#combined-replication-owner-target-backing)"; !strings.Contains(string(readmeData), token) {
		t.Fatalf("README.md missing combined replication-backing token %q", token)
	}

	benchmarkData, err := os.ReadFile("BENCHMARK.md")
	if err != nil {
		t.Fatalf("ReadFile(BENCHMARK.md) error = %v", err)
	}
	for _, token := range []string{
		"Combined replication owner/target backing",
		"TestReplicationRoutingSnapshotCombinedBackingMatchesSeparateTargets",
		"BenchmarkReplicationRoutingCombinedBackingConstructionAlternating",
		"22,196.5 ns; 30,208 B; 82 allocs",
		"20,770.5 ns; 30,208 B; 34 allocs",
		"already-required owner count",
		"11,881 to 12,006 ns",
		"reverted before commit",
	} {
		if !strings.Contains(string(benchmarkData), token) {
			t.Fatalf("BENCHMARK.md missing combined replication-backing token %q", token)
		}
	}
}

func TestDocsDescribeDirectReplicationRouteMembership(t *testing.T) {
	readmeData, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatalf("ReadFile(README.md) error = %v", err)
	}
	if token := "[direct target membership](BENCHMARK.md#direct-replication-route-membership)"; !strings.Contains(string(readmeData), token) {
		t.Fatalf("README.md missing direct replication route-membership token %q", token)
	}

	benchmarkData, err := os.ReadFile("BENCHMARK.md")
	if err != nil {
		t.Fatalf("ReadFile(BENCHMARK.md) error = %v", err)
	}
	for _, token := range []string{
		"BenchmarkReplicationRouteTargetsNodeAlternating",
		"BenchmarkReplicationRouteTargetsNodeValidationAlternating",
		"TestReplicationRouteTargetsNodeMatchesMaterializedControl",
		"7.73x faster",
		"1.25x faster",
		"19.42x faster",
		"6,968 B",
		"all heap and allocations eliminated",
		"Both direct variants remain zero-allocation",
	} {
		if !strings.Contains(string(benchmarkData), token) {
			t.Fatalf("BENCHMARK.md missing direct replication route-membership token %q", token)
		}
	}
}

func TestDocsDescribeDirectSingleTargetDigestInventory(t *testing.T) {
	readmeData, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatalf("ReadFile(README.md) error = %v", err)
	}
	if token := "[direct digest inventory](BENCHMARK.md#direct-single-target-digest-inventory)"; !strings.Contains(string(readmeData), token) {
		t.Fatalf("README.md missing direct digest inventory token %q", token)
	}

	benchmarkData, err := os.ReadFile("BENCHMARK.md")
	if err != nil {
		t.Fatalf("ReadFile(BENCHMARK.md) error = %v", err)
	}
	for _, token := range []string{
		"BenchmarkReplicationDigestInventorySingleTargetAlternating",
		"1.15x faster",
		"312 B lower",
		"two fewer",
		"Shared-loop single-target digest branch",
		"four-target control was 1.8% slower",
	} {
		if !strings.Contains(string(benchmarkData), token) {
			t.Fatalf("BENCHMARK.md missing direct digest inventory token %q", token)
		}
	}
}

func TestDocsDescribeSelectiveSnapshotMutationMaps(t *testing.T) {
	readmeData, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatalf("ReadFile(README.md) error = %v", err)
	}
	if token := "[selective maps](BENCHMARK.md#selective-snapshot-mutation-maps)"; !strings.Contains(string(readmeData), token) {
		t.Fatalf("README.md missing selective snapshot mutation-map token %q", token)
	}

	benchmarkData, err := os.ReadFile("BENCHMARK.md")
	if err != nil {
		t.Fatalf("ReadFile(BENCHMARK.md) error = %v", err)
	}
	for _, token := range []string{
		"BenchmarkSnapshotMutationTrackingCycle",
		"2.71x faster",
		"CPU neutral within 0.6%",
		"1.11x lower heap",
		"Fully lazy snapshot mutation map",
		"1.32x slower",
	} {
		if !strings.Contains(string(benchmarkData), token) {
			t.Fatalf("BENCHMARK.md missing selective snapshot mutation-map token %q", token)
		}
	}
}

func TestDocsDescribeSinglePassExpirationIndexCompaction(t *testing.T) {
	readmeData, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatalf("ReadFile(README.md) error = %v", err)
	}
	for _, token := range []string{
		"[single-pass expiration-index rebuild](BENCHMARK.md#single-pass-expiration-index-compaction)",
		"1.35x faster",
	} {
		if !strings.Contains(string(readmeData), token) {
			t.Fatalf("README.md missing expiration compaction token %q", token)
		}
	}

	benchmarkData, err := os.ReadFile("BENCHMARK.md")
	if err != nil {
		t.Fatalf("ReadFile(BENCHMARK.md) error = %v", err)
	}
	for _, token := range []string{
		"BenchmarkCompactMemoryExpirationIndex10k",
		"436,936 B",
		"35 fewer allocations",
	} {
		if !strings.Contains(string(benchmarkData), token) {
			t.Fatalf("BENCHMARK.md missing expiration compaction token %q", token)
		}
	}
}

func TestDocsDescribeLinearExpirationIndexRebuild(t *testing.T) {
	readmeData, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatalf("ReadFile(README.md) error = %v", err)
	}
	if token := "[linear expiration-index rebuild](BENCHMARK.md#linear-expiration-index-rebuild)"; !strings.Contains(string(readmeData), token) {
		t.Fatalf("README.md missing linear expiration-index token %q", token)
	}

	benchmarkData, err := os.ReadFile("BENCHMARK.md")
	if err != nil {
		t.Fatalf("ReadFile(BENCHMARK.md) error = %v", err)
	}
	for _, token := range []string{
		"5,964,272 ns",
		"exact heap order",
		"identical heap and allocations",
	} {
		if !strings.Contains(string(benchmarkData), token) {
			t.Fatalf("BENCHMARK.md missing linear expiration-index token %q", token)
		}
	}
}

func TestDocsDescribeValidatedBoundedKeyStatsCompaction(t *testing.T) {
	readmeData, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatalf("ReadFile(README.md) error = %v", err)
	}
	for _, token := range []string{
		"[Validated bounded key-stat compaction](BENCHMARK.md#validated-bounded-key-stat-compaction)",
		"1.44x lower cumulative heap",
	} {
		if !strings.Contains(string(readmeData), token) {
			t.Fatalf("README.md missing bounded key-stat compaction token %q", token)
		}
	}

	benchmarkData, err := os.ReadFile("BENCHMARK.md")
	if err != nil {
		t.Fatalf("ReadFile(BENCHMARK.md) error = %v", err)
	}
	for _, token := range []string{
		"BenchmarkCompactMemoryBoundedKeyStats100k",
		"3,495,144 B",
		"repair fallback",
	} {
		if !strings.Contains(string(benchmarkData), token) {
			t.Fatalf("BENCHMARK.md missing bounded key-stat compaction token %q", token)
		}
	}
}

func TestREADMEDocumentsXorFilterBuildHashIndex(t *testing.T) {
	data, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatalf("ReadFile(README.md) error = %v", err)
	}
	readme := string(data)
	for _, token := range []string{
		"[XOR build hash index](BENCHMARK.md#compact-xor-filter-build-hash-index)",
		"seed-independent 8-byte hashes",
	} {
		if !strings.Contains(readme, token) {
			t.Fatalf("README.md missing XOR build hash-index token %q", token)
		}
	}
}

func TestDocsDescribeAdaptiveXorBatchDeduplication(t *testing.T) {
	readmeData, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatalf("ReadFile(README.md) error = %v", err)
	}
	if token := "[adaptive generic batch deduplication](BENCHMARK.md#adaptive-xor-batch-deduplication)"; !strings.Contains(string(readmeData), token) {
		t.Fatalf("README.md missing adaptive XOR batch token %q", token)
	}

	benchmarkData, err := os.ReadFile("BENCHMARK.md")
	if err != nil {
		t.Fatalf("ReadFile(BENCHMARK.md) error = %v", err)
	}
	for _, token := range []string{
		"BenchmarkXorFilterGenericBatchDedupStrategy",
		"1.04x-1.12x faster",
		"transactional validation",
		"heap and allocations unchanged",
	} {
		if !strings.Contains(string(benchmarkData), token) {
			t.Fatalf("BENCHMARK.md missing adaptive XOR batch token %q", token)
		}
	}
}

func TestREADMELinksShardingProposal(t *testing.T) {
	readmeData, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatalf("ReadFile(README.md) error = %v", err)
	}
	proposalData, err := os.ReadFile("SHARDING_PROPOSAL.md")
	if err != nil {
		t.Fatalf("ReadFile(SHARDING_PROPOSAL.md) error = %v", err)
	}
	readme := string(readmeData)
	proposal := string(proposalData)
	for _, token := range []string{
		"[`SHARDING_PROPOSAL.md`](SHARDING_PROPOSAL.md)",
		"Sharding is opt-in",
	} {
		if !strings.Contains(readme, token) {
			t.Fatalf("README.md missing sharding token %q", token)
		}
	}
	for _, token := range []string{
		"Sharding is opt-in",
		"XXH3 64-bit",
		"65,536 logical slots",
		"rendezvous hashing",
		"hash tags",
		"migration states",
		"slot epoch",
		"`MOVED`",
		"`ASK`",
		"journal sequence fence",
		"planner output",
		"rollback",
	} {
		if !strings.Contains(proposal, token) {
			t.Fatalf("SHARDING_PROPOSAL.md missing token %q", token)
		}
	}
}

func TestREADMELinksPartitioningProposal(t *testing.T) {
	readmeData, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatalf("ReadFile(README.md) error = %v", err)
	}
	proposalData, err := os.ReadFile("PARTITIONING_PROPOSAL.md")
	if err != nil {
		t.Fatalf("ReadFile(PARTITIONING_PROPOSAL.md) error = %v", err)
	}
	readme := string(readmeData)
	proposal := string(proposalData)
	for _, token := range []string{
		"[`PARTITIONING_PROPOSAL.md`](PARTITIONING_PROPOSAL.md)",
	} {
		if !strings.Contains(readme, token) {
			t.Fatalf("README.md missing partitioning token %q", token)
		}
	}
	for _, token := range []string{
		"multi-datacenter",
		"`partitioned`",
		"journal sequence fence",
		"PARTITION partition_id host:port topology_epoch",
		"Partitioning and sharding solve different problems",
		"backup boundaries",
		"sharding stays off by default",
	} {
		if !strings.Contains(proposal, token) {
			t.Fatalf("PARTITIONING_PROPOSAL.md missing token %q", token)
		}
	}
}

func TestREADMEDocumentsBackupPartitionValidation(t *testing.T) {
	data, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatalf("ReadFile(README.md) error = %v", err)
	}
	readme := string(data)
	for _, token := range []string{
		"`key_prefixes`",
		"`partition_validation`",
		"invalid key samples",
		"`checked_journal_keys`",
		"`invalid_journal_key_samples`",
		"`restore-bundle` refuses",
	} {
		if !strings.Contains(readme, token) {
			t.Fatalf("README.md missing backup partition validation token %q", token)
		}
	}
}

func TestREADMEDocumentsSaneConfigProfiles(t *testing.T) {
	data, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatalf("ReadFile(README.md) error = %v", err)
	}
	readme := string(data)
	for _, token := range []string{
		"make print-sane-config",
		"CONFIG_PROFILE=dev",
		"CONFIG_PROFILE=bench",
		"`production` enables",
		"`bench` enables",
		"Override any profile default",
	} {
		if !strings.Contains(readme, token) {
			t.Fatalf("README.md missing sane config profile token %q", token)
		}
	}
}

func TestREADMEListsBenchmarkRegressionGuard(t *testing.T) {
	data, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatalf("ReadFile(README.md) error = %v", err)
	}
	readme := string(data)
	for _, token := range []string{
		"make bench-smoke BENCH_SMOKE_CHECK_THRESHOLDS=1",
		"`BENCH_SMOKE_CHECK_THRESHOLDS=1`",
		"`BENCH_SMOKE_MAX_COMMAND_NS_OP`",
		"`BENCH_SMOKE_MAX_TRANSPORT_NS_OP`",
		"`BENCH_SMOKE_MAX_SERIALIZATION_NS_OP`",
		"`BENCH_SMOKE_MAX_B_OP`",
		"`BENCH_SMOKE_MAX_ALLOCS_OP`",
		"`BENCH_SMOKE_ARTIFACT_DIR`",
		"`benchmark-smoke.json`",
		"`benchmark-smoke.md`",
		"`BENCH_SMOKE_BASELINE_JSON`",
		"`BENCH_SMOKE_MAX_REGRESSION_PCT`",
		"`BENCH_SMOKE_COMPARE_MEMORY=1`",
		"Set any max to `0` to disable that specific",
	} {
		if !strings.Contains(readme, token) {
			t.Fatalf("README.md does not document benchmark regression guard token %q", token)
		}
	}
}

func TestREADMEListsFrontendSmoke(t *testing.T) {
	data, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatalf("ReadFile(README.md) error = %v", err)
	}
	readme := string(data)
	for _, token := range []string{
		"make frontend-smoke",
		"make frontend-backend-smoke",
		"Vite preview",
		"dashboard/keys/commands/admin HTML",
		"`FRONTEND_SMOKE_REQUIRE_BROWSER=true`",
		"real `hatrie-cache` monitoring server",
		"`/api/storage/flush`",
	} {
		if !strings.Contains(readme, token) {
			t.Fatalf("README.md does not document frontend smoke token %q", token)
		}
	}
}

func TestCommandSupportScriptListsExecuteCommandAliases(t *testing.T) {
	commandGroups := executeCommandCases(t)
	data, err := os.ReadFile("scripts/command-support.sh")
	if err != nil {
		t.Fatalf("ReadFile(scripts/command-support.sh) error = %v", err)
	}
	script := string(data)
	for _, token := range []string{
		`/^[[:space:]]*case "/`,
		"`\" commands[i] \"`",
		"| Canonical command | Accepted aliases |",
	} {
		if !strings.Contains(script, token) {
			t.Fatalf("command-support.sh missing parser token %q", token)
		}
	}
	if len(commandGroups) == 0 {
		t.Fatal("ExecuteCommand case parser found no command groups")
	}
}

func TestRedisCommandFeatureBenchmarkScriptReportsSecondsPer10K(t *testing.T) {
	data, err := os.ReadFile("scripts/benchmark-redis-command-features.sh")
	if err != nil {
		t.Fatalf("ReadFile(benchmark-redis-command-features.sh) error = %v", err)
	}
	script := string(data)
	for _, token := range []string{
		"REDIS_START_DOCKER",
		"redis-benchmark",
		"BENCHMARK_ARTIFACT_DIR",
		"redis-command-features.tsv",
		"redis-command-memory.tsv",
		"10000 / qps",
		"Seconds / 10k ops",
		"Memory summary",
		"used_memory_rss",
		"SETBIT",
		"PFCOUNT",
		"REDIS_PIPELINE",
		"-P \"$pipeline\"",
		"Pipelined string write",
	} {
		if !strings.Contains(script, token) {
			t.Fatalf("Redis command benchmark script missing token %q", token)
		}
	}
	if !strings.Contains(script, "value = $(NF - 6)") || strings.Count(script, "redis_benchmark_qps)") != 3 {
		t.Fatal("Redis command benchmark does not use the validated trailing-column throughput parser")
	}
}

func TestTarantoolCommandFeatureBenchmarkScriptReportsSecondsPer10K(t *testing.T) {
	data, err := os.ReadFile("scripts/tarantool-command-features.lua")
	if err != nil {
		t.Fatalf("ReadFile(tarantool-command-features.lua) error = %v", err)
	}
	script := string(data)
	for _, token := range []string{
		"TARANTOOL_REQUESTS",
		"TARANTOOL_MEMTX_MEMORY",
		"clock.monotonic",
		"Seconds / 10k feature cycles",
		"Memory summary",
		"box.slab.info",
		"space:replace() + space:delete()",
		"msgpack.encode(tuple)",
		"index:pairs(prefix",
		"TARANTOOL_PIPELINE",
		"seconds_for_ops",
		"Pipelined string write",
	} {
		if !strings.Contains(script, token) {
			t.Fatalf("Tarantool command benchmark script missing token %q", token)
		}
	}
	data, err = os.ReadFile("scripts/benchmark-tarantool-command-features.sh")
	if err != nil {
		t.Fatalf("ReadFile(benchmark-tarantool-command-features.sh) error = %v", err)
	}
	wrapper := string(data)
	for _, token := range []string{
		"BENCHMARK_ARTIFACT_DIR",
		"tarantool-command-features.tsv",
		"tarantool-command-memory.tsv",
	} {
		if !strings.Contains(wrapper, token) {
			t.Fatalf("Tarantool command benchmark wrapper missing token %q", token)
		}
	}
}

func TestHatTrieCommandFeatureBenchmarkScriptReportsRSS(t *testing.T) {
	data, err := os.ReadFile("scripts/benchmark-hatrie-command-features.sh")
	if err != nil {
		t.Fatalf("ReadFile(benchmark-hatrie-command-features.sh) error = %v", err)
	}
	script := string(data)
	for _, token := range []string{
		"HATRIE_BENCH",
		"BENCHMARK_ARTIFACT_DIR",
		"hatrie-command-features.tsv",
		"hatrie-command-memory.tsv",
		"go test -c",
		"-test.benchmem",
		"/usr/bin/time",
		"Max resident set size",
		"HATRIE_PIPELINE_OPS",
		"normalize_output_file",
	} {
		if !strings.Contains(script, token) {
			t.Fatalf("HAT-trie command benchmark script missing token %q", token)
		}
	}
	data, err = os.ReadFile("command_feature_benchmark_test.go")
	if err != nil {
		t.Fatalf("ReadFile(command_feature_benchmark_test.go) error = %v", err)
	}
	benchmarks := string(data)
	if !strings.Contains(benchmarks, "PipelineBatch16") {
		t.Fatal("BenchmarkCommandFeature missing PipelineBatch16")
	}
}

func executeCommandCases(t *testing.T) [][]string {
	t.Helper()
	data, err := os.ReadFile("command.go")
	if err != nil {
		t.Fatalf("ReadFile(command.go) error = %v", err)
	}
	start := bytes.Index(data, []byte("func (ht *HatTrie) ExecuteCommand"))
	if start < 0 {
		t.Fatal("ExecuteCommand function not found")
	}
	data = data[start:]
	end := bytes.Index(data, []byte("\nfunc (ht *HatTrie) executePublicBatchCommand"))
	if end < 0 {
		t.Fatal("ExecuteCommand end marker not found")
	}
	data = data[:end]
	casePattern := regexp.MustCompile(`(?m)^\s*case\s+([^:\n]+):`)
	commandPattern := regexp.MustCompile(`"([^"]+)"`)
	var groups [][]string
	for _, match := range casePattern.FindAllSubmatch(data, -1) {
		commandMatches := commandPattern.FindAllSubmatch(match[1], -1)
		if len(commandMatches) == 0 {
			continue
		}
		group := make([]string, 0, len(commandMatches))
		for _, commandMatch := range commandMatches {
			group = append(group, string(commandMatch[1]))
		}
		groups = append(groups, group)
	}
	return groups
}

func TestREADMEListsAsyncReplicationOptions(t *testing.T) {
	data, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatalf("ReadFile(README.md) error = %v", err)
	}
	readme := string(data)
	for _, token := range []string{
		"REPLICATION_MODE",
		"journal",
		"command",
		"dual",
		"REPLICATION_ASYNC",
		"REPLICATION_QUEUE_SIZE",
		"REPLICATION_RETRY_INTERVAL",
		"REPLICATION_MAX_ATTEMPTS",
		"REPLICATION_DEAD_LETTER_LIMIT",
		"REPLICATION_OUTBOX_PATH",
		"REPLICATION_OUTBOX_FORMAT",
		"REPLICATION_AUTH_TOKEN",
		"REPLICATION_SYNC_INTERVAL",
		"REPLICATION_SYNC_PREFIX",
	} {
		if !strings.Contains(readme, token) {
			t.Fatalf("README.md does not document %s", token)
		}
	}
}

func TestREADMEListsAdminAuditOperations(t *testing.T) {
	data, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatalf("ReadFile(README.md) error = %v", err)
	}
	readme := string(data)
	for _, token := range []string{
		"`AUDIT_LOG_PATH`",
		"explicit confirmation before running flush",
		"`GET /api/audit?limit=25`",
		"`/api/audit`",
		"intentionally omit command values",
	} {
		if !strings.Contains(readme, token) {
			t.Fatalf("README.md does not document admin audit operations token %q", token)
		}
	}
}

func TestREADMEListsGRPCReplication(t *testing.T) {
	data, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatalf("ReadFile(README.md) error = %v", err)
	}
	readme := string(data)
	for _, token := range []string{
		"Replication` RPC",
		"`Topology`",
		"`UpdateTopology`",
		"`Election`",
		"`UpdateElection`",
		"`GET /api/replication`",
		"`POST /api/replication`",
		"`REPLICATION_WIRE_FORMAT=protobuf`",
		"automatically use the previous JSON",
		"`REPLICATION_WIRE_FORMAT=json`",
		"`REPLICATION_TRANSPORT=grpc-stream`",
		"`REPLICATION_HTTP_FALLBACK=false`",
		"`grpc_address`",
		"`ReplicationStream`",
		"oldest queued key/age",
		"per-target drops",
		"`dead_letter_count`",
		"recent `dead_letters`",
		"`REPLICATION_CIRCUIT_BREAKER_FAILURES`",
		"`REPLICATION_CIRCUIT_BREAKER_COOLDOWN`",
		"`circuit_breakers`",
		"`circuit_open`",
		"`CacheService.CommandStream`",
		"one sender",
		"starts no additional",
		"`health_score`",
		"`hatrie_cache_replication_health_score`",
		"`hatrie_cache_replication_dead_letters`",
		"`hatrie_cache_replication_queue_capacity`",
		"`hatrie_cache_replication_queue_enqueued_total`",
		"`hatrie_cache_replication_retried_total`",
		"`hatrie_cache_leveldb_dirty_keys`",
		"`hatrie_cache_storage_operation_running`",
	} {
		if !strings.Contains(readme, token) {
			t.Fatalf("README.md does not document %s", token)
		}
	}
}

func TestREADMEListsStorageFormatTradeoffs(t *testing.T) {
	data, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatalf("ReadFile(README.md) error = %v", err)
	}
	readme := string(data)
	for _, token := range []string{
		"`DB_FORMAT=binary`",
		"`DB_FORMAT=json`",
		"`DB_COMPARE_BEFORE_WRITE=auto`",
		"`DB_COMPARE_BEFORE_WRITE=never`",
		"`DefaultStorageFormat` (`StorageFormatBinary`)",
		"`SaveLevelDBWithFormat(path, StorageFormatJSON)`",
		"`OpenLevelDBStoreWithFormat(path, StorageFormatJSON)`",
		"`DB_COMPACT_INTERVAL`",
		"`DB_COMPACT_START_KEY`",
		"`DB_COMPACT_LIMIT_KEY`",
		"`DB_MEMORY_CAP_BYTES`",
		"`DB_RSS_CAP_BYTES`",
		"`DB_MEMORY_EVICT_INTERVAL`",
		"`DB_MEMORY_EVICT_MIN_VALUE_BYTES`",
		"atomic applied-journal sequence",
		"`DB_BACKEND=auto`",
		"`DB_BACKEND=leveldb`",
		"make bench-storage-backends BENCHTIME=3x COUNT=5",
		"syncs only dirty keys changed by direct APIs",
		"`LevelDBDirtyTracker` plus `LevelDBStore.SaveDirty`",
		"make bench-serialization SERIALIZATION_BENCH='BenchmarkLevelDB(Save|Load)Materialized' BENCHTIME=20x",
		"| LevelDB save | binary materialized values |",
		"| LevelDB save | JSON materialized values |",
		"| LevelDB load | binary materialized values |",
		"| LevelDB load | JSON materialized values |",
		"| Structured journal encode | binary (default) |",
		"| Structured journal decode | binary (default) |",
		"make bench-structured-storage-codec BENCHTIME=1000x COUNT=7",
		"always use the versioned tagged",
		"Version-1 binary values",
		"Set `DB_FORMAT=json`",
		"binary LevelDB format is 26% smaller",
		"structured payload, with lower save/load CPU and heap than JSON",
		"`GET /api/storage` reports whether",
		"`size_bytes`, selected engine `properties`, current `operation`",
		"`last_flush`/`last_compact`",
		"`last_spill`",
		"`hatrie_cache_storage_last_spill_keys`",
		"`/api/storage/flush`",
		"`/api/storage/compact`",
		"`size_bytes_before`",
		"`size_bytes_after`",
		"make storage-flush STORAGE_PEER=http://127.0.0.1:8080",
		"make storage-compact STORAGE_PEER=http://127.0.0.1:8080",
	} {
		if !strings.Contains(readme, token) {
			t.Fatalf("README.md does not document storage format tradeoff token %q", token)
		}
	}
}

func TestREADMETracksImplementedDistributedTransport(t *testing.T) {
	data, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatalf("ReadFile(README.md) error = %v", err)
	}
	readme := string(data)
	if strings.Contains(readme, "TO BE** distributed") || strings.Contains(readme, "TO BE distributed") {
		t.Fatal("README.md still describes distributed operation as future work")
	}
	if strings.Contains(readme, "- [ ] the distributed part") {
		t.Fatal("README.md still has stale unchecked distributed TODO")
	}
	for _, token := range []string{
		"persisted topology",
		"deterministic shard leader",
		"HTTP/protobuf replication",
		"anti-entropy sync",
		"journal pull catch-up",
	} {
		if !strings.Contains(readme, token) {
			t.Fatalf("README.md distributed TODO does not mention %q", token)
		}
	}
}

func TestREADMEDocumentsDeltaFirstJournalRecovery(t *testing.T) {
	data, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatalf("ReadFile(README.md) error = %v", err)
	}
	readme := string(data)
	for _, token := range []string{
		"`GET /api/journal/snapshot`",
		"Journal pull is delta-first by default",
		"including stale-key deletion",
		"`JOURNAL_PULL_FULL_SYNC_FALLBACK=false`",
		"hash of `JOURNAL_PULL_SOURCE`",
		"only then advances `JOURNAL_PULL_STATE_PATH`",
	} {
		if !strings.Contains(readme, token) {
			t.Fatalf("README.md journal recovery section missing %q", token)
		}
	}
}

func TestREADMEDocumentsInternalReplicationBatch(t *testing.T) {
	data, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatalf("ReadFile(README.md) error = %v", err)
	}
	readme := string(data)
	for _, token := range []string{
		"`INTERNALBATCH`",
		"`INTERNALBATCHV2`",
		"`INTERNALSETV2`",
		"`INTERNALSETV3`",
		"`INTERNALDIGESTV1`",
		"keyless binary snapshot values",
		"bounded, sorted",
		"batches multiple internal replication commands",
		"automatically retries the legacy",
		"`REPLICATION_WIRE_FORMAT=json` converts snapshots to legacy JSON before send",
		"accepted only for internal replication traffic",
	} {
		if !strings.Contains(readme, token) {
			t.Fatalf("README.md does not document internal replication batch token %q", token)
		}
	}
}

func TestBenchmarkDocsListInternalBatchPrimitive(t *testing.T) {
	data, err := os.ReadFile("BENCHMARK.md")
	if err != nil {
		t.Fatalf("ReadFile(BENCHMARK.md) error = %v", err)
	}
	document := string(data)
	for _, token := range []string{"`INTERNALBATCH`", "`INTERNALDIGESTV1`", "BenchmarkReplicationDigestIncremental", "49,971x"} {
		if !strings.Contains(document, token) {
			t.Fatalf("BENCHMARK.md does not list replication benchmark token %q", token)
		}
	}
}

func TestBenchmarkDocsLatestCollectionAllocationReductions(t *testing.T) {
	data, err := os.ReadFile("BENCHMARK.md")
	if err != nil {
		t.Fatalf("ReadFile(BENCHMARK.md) error = %v", err)
	}
	document := string(data)
	for _, token := range []string{
		"BenchmarkSetRepresentationSmallValues",
		"Typed priority-queue string slot",
		"Direct radix prefix JSON",
		"20 to 1",
	} {
		if !strings.Contains(document, token) {
			t.Fatalf("BENCHMARK.md does not document collection allocation token %q", token)
		}
	}
}

func TestBenchmarkDocsListReplicationBatchingBenchmark(t *testing.T) {
	data, err := os.ReadFile("BENCHMARK.md")
	if err != nil {
		t.Fatalf("ReadFile(BENCHMARK.md) error = %v", err)
	}
	benchmark := string(data)
	for _, token := range []string{
		"BenchmarkHTTPReplicatorSyncAllBatching",
		"requests/op",
		"wire_B/op",
		"batching request reduction",
		"Final optimized (`69a6018`)",
		"Current optimized (`e5b127d`)",
		"1.51x faster",
		"6.10x less cumulative allocated heap",
		"BenchmarkGzipCompressionLevels",
		"13.4x less compressor allocation",
		"BenchmarkHTTPReplicatorTargetFanout",
		"3.65x",
		"10.43x fewer",
		"BenchmarkReplicationSyncTransport",
		"1.19x",
	} {
		if !strings.Contains(benchmark, token) {
			t.Fatalf("BENCHMARK.md does not document replication batching benchmark token %q", token)
		}
	}
}

func TestBenchmarkDocsListJournalDeltaFirstRecovery(t *testing.T) {
	data, err := os.ReadFile("BENCHMARK.md")
	if err != nil {
		t.Fatalf("ReadFile(BENCHMARK.md) error = %v", err)
	}
	benchmark := string(data)
	for _, token := range []string{
		"BenchmarkJournalCatchUpDeltaVsFullSnapshot",
		"make bench-journal-catchup BENCHTIME=5x COUNT=7",
		"56.55x faster",
		"42.70x slower",
		"329.28x fewer",
	} {
		if !strings.Contains(benchmark, token) {
			t.Fatalf("BENCHMARK.md does not document journal recovery benchmark token %q", token)
		}
	}
}

func TestImprovementReportIncludesLatestReplicationWork(t *testing.T) {
	data, err := os.ReadFile("IMPROVEMENT_REPORT.md")
	if err != nil {
		t.Fatalf("ReadFile(IMPROVEMENT_REPORT.md) error = %v", err)
	}
	report := string(data)
	for _, token := range []string{
		"`a2ca705`",
		"`bb8b86d`",
		"`e899eb8`",
		"`a0c7561`",
		"`f34ea71`",
		"`2c24768`",
		"`675bccc`",
		"`2f3deb6`",
		"`7a69306`",
		"`f187b83`",
		"`d471652`",
		"`70775ee`",
		"`ae64ce3`",
		"`bdf8c70`",
		"`66b1309`",
		"`0ef0207`",
		"`7401e05`",
		"`c70d849`",
		"`2747005`",
		"`2b700e5`",
		"`86fe5ca`",
		"`f871c79`",
		"`69a6018`",
		"`471c229`",
		"`c1bf95a`",
		"`a02c5a5`",
		"`5c6bd2f`",
		"`4c869d0`",
		"`e5b127d`",
		"`532270c`",
		"`3e79248`",
		"`6d148c2`",
		"`c549fb7`",
		"`7f4c1e1`",
		"`943adc2`",
		"`629ccca`",
		"LevelDB replication outbox backend",
		"Batch replication by target",
		"multi-node replication failure tests",
		"native replication batch wire format",
		"Preflight replication batches before apply",
		"Benchmark replication batching",
		"multi-node replication chaos tests",
		"1.51x faster",
		"6.10x less allocated heap",
		"1.68x fewer allocations",
		"13.4x less compressor allocation",
		"3.65x faster",
		"10.43x fewer allocations",
		"56.55x faster",
		"JOURNAL_PULL_FULL_SYNC_FALLBACK=false",
		"49,971x smaller on wire",
		"18.94x faster",
		"3.71x shorter",
	} {
		if !strings.Contains(report, token) {
			t.Fatalf("IMPROVEMENT_REPORT.md does not include latest replication token %q", token)
		}
	}
}
