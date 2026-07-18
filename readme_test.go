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
		"Tarantool benchmark: version=2.6.0-0-g47aa4e01e requests=1000000 keyspace=10000",
		"`SET`",
	} {
		if !strings.Contains(doc, token) {
			t.Fatalf("BENCHMARK.md missing comparison/source token %q", token)
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
		"make bench-ci-smoke BENCH_CI_SMOKE_CHECK_THRESHOLDS=1",
		"`BENCH_CI_SMOKE_CHECK_THRESHOLDS=1`",
		"`BENCH_CI_SMOKE_MAX_COMMAND_NS_OP`",
		"`BENCH_CI_SMOKE_MAX_TRANSPORT_NS_OP`",
		"`BENCH_CI_SMOKE_MAX_SERIALIZATION_NS_OP`",
		"`BENCH_CI_SMOKE_MAX_B_OP`",
		"`BENCH_CI_SMOKE_MAX_ALLOCS_OP`",
		"`BENCH_CI_SMOKE_ARTIFACT_DIR`",
		"`benchmark-ci-smoke.json`",
		"`benchmark-ci-smoke.md`",
		"`BENCH_CI_SMOKE_BASELINE_JSON`",
		"`BENCH_CI_SMOKE_MAX_REGRESSION_PCT`",
		"`BENCH_CI_SMOKE_COMPARE_MEMORY=1`",
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
		"performs one full LevelDB save at startup",
		"syncs only dirty keys changed by HTTP commands, gRPC commands, and journal pull",
		"`LevelDBDirtyTracker` plus `LevelDBStore.SaveDirty`",
		"make bench-serialization SERIALIZATION_BENCH='BenchmarkLevelDB(Save|Load)Materialized' BENCHTIME=20x",
		"| LevelDB save | binary materialized values |",
		"| LevelDB save | JSON materialized values |",
		"| LevelDB load | binary materialized values |",
		"| LevelDB load | JSON materialized values |",
		"| Structured journal encode | binary (default) |",
		"| Structured journal decode | binary (default) |",
		"use the smaller of the compact binary",
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
	} {
		if !strings.Contains(report, token) {
			t.Fatalf("IMPROVEMENT_REPORT.md does not include latest replication token %q", token)
		}
	}
}
