MONITORING_ADDR ?= 127.0.0.1:8080
MONITORING_WEB_DIR ?= svelte-mpa/dist
MONITORING_TLS_CERT ?=
MONITORING_TLS_KEY ?=
MONITORING_AUTH_TOKEN ?=
MONITORING_AUTH_PREVIOUS_TOKEN ?=
MONITORING_AUTH_PREVIOUS_TOKEN_EXPIRES_AT ?=
DIAGNOSTICS_PROFILING ?= false
AUDIT_LOG_PATH ?=
WRITE_PROTECTION ?= false
RATE_LIMIT ?= 0
KEY_STATS_MODE ?= off
KEY_STATS_CAPACITY ?= 100000
LOCAL_PARTITIONS ?= 0
COUNTER_WRITE_STRIPES ?= 0
MEMORY_COMPACTION_INTERVAL ?= 0
MONITORING_READ_HEADER_TIMEOUT ?= 5s
MONITORING_IDLE_TIMEOUT ?= 2m
NODE_ID ?=
TOPOLOGY_PATH ?=
ELECTION_TIMEOUT ?= 15s
REPLICATION ?= false
REPLICATION_MODE ?= journal
REPLICATION_ASYNC ?= false
REPLICATION_QUEUE_SIZE ?= 1024
REPLICATION_RETRY_INTERVAL ?= 250ms
REPLICATION_MAX_ATTEMPTS ?= 3
REPLICATION_DEAD_LETTER_LIMIT ?= 128
REPLICATION_OUTBOX_PATH ?=
REPLICATION_OUTBOX_FORMAT ?= auto
REPLICATION_OUTBOX_CODEC ?= binary
REPLICATION_OUTBOX_BATCH_WINDOW ?= 1ms
REPLICATION_CIRCUIT_BREAKER_FAILURES ?= 5
REPLICATION_CIRCUIT_BREAKER_COOLDOWN ?= 30s
REPLICATION_WIRE_FORMAT ?= protobuf
REPLICATION_TRANSPORT ?= http
REPLICATION_GRPC_WINDOW ?= 32
REPLICATION_GRPC_BATCH_MAX_COMMANDS ?= 32
REPLICATION_GRPC_BATCH_WINDOW ?= 0
REPLICATION_HTTP_FALLBACK ?= true
REPLICATION_AUTH_TOKEN ?=
REPLICATION_AUTH_PREVIOUS_TOKEN ?=
REPLICATION_AUTH_PREVIOUS_TOKEN_EXPIRES_AT ?=
REPLICATION_BATCH_MAX_BYTES ?= 1048576
REPLICATION_MAX_IN_FLIGHT_TARGETS ?= 4
REPLICATION_SYNC_INTERVAL ?= 0
REPLICATION_SYNC_PREFIX ?=
ENFORCE_LEADER_WRITES ?= false
GRPC_ADDR ?=
GRPC_TLS_CERT ?=
GRPC_TLS_KEY ?=
GRPC_CLIENT_CA ?=
DB_PATH ?=
DB_BACKEND ?= auto
DB_FORMAT ?= binary
DB_SYNC_INTERVAL ?= 0
DB_COMPARE_BEFORE_WRITE ?= auto
DB_COMPACT_INTERVAL ?= 0
DB_COMPACT_START_KEY ?=
DB_COMPACT_LIMIT_KEY ?=
DB_HOT_LOAD ?= false
DB_HOT_LOAD_MAX_BYTES ?= 1024
DB_HOT_LOAD_MAX_AGE ?= 1h
DB_HOT_LOAD_MIN_HITS ?= 1000
DB_MEMORY_CAP_BYTES ?= 0
DB_RSS_CAP_BYTES ?= 0
DB_MEMORY_EVICT_INTERVAL ?= 0
DB_MEMORY_EVICT_MIN_VALUE_BYTES ?= 1024
SNAPSHOT_PATH ?=
SNAPSHOT_INTERVAL ?= 0
SNAPSHOT_FORMAT ?= gzip-best-binary
JOURNAL_PATH ?=
JOURNAL_FORMAT ?= binary
JOURNAL_GROUP_COMMIT_WINDOW ?= 0
JOURNAL_GROUP_COMMIT_MAX_BATCH ?= 64
JOURNAL_SEGMENT_MAX_BYTES ?= 67108864
JOURNAL_RETAINED_SEGMENTS ?= 16
JOURNAL_PULL_SOURCE ?=
JOURNAL_PULL_STATE_PATH ?=
JOURNAL_PULL_INTERVAL ?= 0
JOURNAL_PULL_TIMEOUT ?= 30s
JOURNAL_PULL_LIMIT ?= 0
JOURNAL_PULL_MAX_BATCHES ?= 0
JOURNAL_PULL_FULL_SYNC_FALLBACK ?= true
JOURNAL_PULL_CHECKPOINT_BOOTSTRAP ?= true
JOURNAL_PULL_INCREMENTAL_RECOVERY ?= true
JOURNAL_PULL_WIRE_FORMAT ?= binary
DATA_DIR ?= data
BACKUP_DIR ?= backup/latest
BACKUP_OVERWRITE ?= false
RESTORE_OVERWRITE ?= false
DOCTOR_PATH ?= $(BACKUP_DIR)
RESTORE_BUNDLE_PATH ?= backup/latest.tar.gz
RESTORE_BUNDLE_OVERWRITE ?= false
RESTORE_REHEARSAL_PATH ?= $(BACKUP_DIR)
RESTORE_REHEARSAL_WORK_DIR ?=
RESTORE_REHEARSAL_KEEP_WORK_DIR ?= false
RESTORE_REHEARSAL_RUNTIME_CHECK ?= true
RESTORE_REHEARSAL_RUNTIME_GET ?=
RESTORE_REHEARSAL_RUNTIME_SERVER_BIN ?=
CLUSTER_PEER ?= http://127.0.0.1:8080
CLUSTER_PROBE_NODES ?= true
STORAGE_PEER ?= http://127.0.0.1:8080
STORAGE_COMPACT_START_KEY ?=
STORAGE_COMPACT_LIMIT_KEY ?=
SANITIZE_C ?= auto
SANITIZE_C_ALLOW_STRICT_OVERCOMMIT ?= 0
SANITIZE_C_ALLOW_LOW_COMMIT_HEADROOM ?= 0
SANITIZE_C_OVERCOMMIT_MEMORY_PATH ?=
SANITIZE_C_MEMINFO_PATH ?=
SANITIZE_C_ASAN_MIN_COMMIT_HEADROOM_KB ?=
BENCH ?= .
BENCHTIME ?=
COUNT ?= 1
RESERVOIR_SMALL_BENCH ?= ^BenchmarkReservoirSampleSmallGetCommand$$
RESERVOIR_BATCH_PATH_BENCH ?= ^BenchmarkReservoirSample(ExistingBatchCommandPath|BatchCommandPath)$$
RESERVOIR_BATCH_ALTERNATING_BENCH ?= ^BenchmarkReservoirSampleCommandBatchAlternating$$
RESERVOIR_BATCH_ALTERNATING_BENCHTIME ?= 100x
BLOOM_HEADER_LAYOUT_BENCH ?= ^BenchmarkBloomFilterHeaderLayout100k$$
BLOOM_HEADER_OPERATION_BENCH ?= ^BenchmarkBloomFilter(AddKey|ContainsKey)$$
BLOOM_HEADER_LAYOUT_BENCHTIME ?= 1x
COUNT_MIN_ROWS_BENCH ?= ^BenchmarkCountMinSketch(DirectRows|JSONStringRows)$$
FENWICK_ADD_BENCH ?= ^BenchmarkFenwickTree(AddTraversal|FirstAdd)$$
QUANTILE_ADD_BENCH ?= ^BenchmarkQuantileSketchAddValidation$$
QUANTILE_BATCH_PATH_BENCH ?= ^BenchmarkQuantileSketch(Existing|Fresh)BatchCommandPath$$
QUANTILE_BATCH_ALTERNATING_BENCH ?= ^BenchmarkQuantileSketchCommandBatchAlternating$$
QUANTILE_BATCH_ALTERNATING_BENCHTIME ?= 200x
QUANTILE_BATCH_CONTROL_BENCH ?= ^BenchmarkCommandFeature/(QuantileSketchAdd|MixedReadHeavy100|MixedWriteHeavy100)$$
QUANTILE_BATCH_CONTROL_BENCHTIME ?= 500ms
QUANTILE_BATCH_BASELINE_BINARY ?=
QUANTILE_BATCH_CANDIDATE_BINARY ?=
ROARING_BATCH_PATH_BENCH ?= ^BenchmarkRoaringBitmap((Existing|Fresh)Add|ExistingRemove)BatchCommandPath$$
ROARING_BATCH_ALTERNATING_BENCH ?= ^BenchmarkRoaringBitmapAddCommandBatchAlternating$$
ROARING_BATCH_ALTERNATING_BENCHTIME ?= 400x
ROARING_BATCH_CONTROL_BENCH ?= ^BenchmarkCommandFeature/(RoaringAdd|MixedReadHeavy100|MixedWriteHeavy100)$$
ROARING_BATCH_CONTROL_BENCHTIME ?= 500ms
ROARING_BATCH_BASELINE_BINARY ?=
ROARING_BATCH_CANDIDATE_BINARY ?=
TOP_K_SCALAR_BENCH ?= ^BenchmarkTopKGenericScalarDispatch
BLOOM_SCALAR_BENCH ?= ^BenchmarkBloomFilter(ScalarAddChecked|AddCheckedProduction|VariadicBatchControl)
CUCKOO_SCALAR_BENCH ?= ^BenchmarkCuckooFilter(ScalarAddChecked|AddCheckedProduction|VariadicBatchControl|ScalarDelete)
HLL_SCALAR_BENCH ?= ^BenchmarkHyperLogLog(ScalarAddChecked|AddCheckedProduction|VariadicBatchControl)
CMS_SCALAR_BENCH ?= ^BenchmarkCountMinSketch(ScalarAddChecked|AddCheckedProduction|VariadicBatchControl)
SET_SCALAR_GENERIC_BENCH ?= ^BenchmarkSetScalarGeneric(Add|ProductionControls)
PRIORITY_QUEUE_SCALAR_BENCH ?= ^BenchmarkPriorityQueueScalarPush
SERIALIZATION_BENCH ?=
JOURNAL_CATCHUP_BENCH ?= BenchmarkJournalCatchUpDeltaVsFullSnapshot
JOURNAL_WIRE_BENCH ?= ^BenchmarkCommandJournalTail(Wire|Ownership|CompactDecode)10k$$
JOURNAL_APPLY_BENCH ?= ^BenchmarkJournal(PullApplyBatch10K|ScalarApply10K|PullRepresentation10K|WALChunkSize10K)$$
PEBBLE_GENERATION_BENCH ?= ^BenchmarkPebbleFullSaveArchitecture10k$$
PEBBLE_BACKUP_BENCH ?= ^BenchmarkPebbleCheckpointBackup10k$$
INCREMENTAL_BACKUP_BENCH ?= ^BenchmarkIncrementalBackupRepository10k$$
ATOMIC_RESTORE_BENCH ?= ^BenchmarkSinglePassAtomicRestore10k$$
CHECKPOINT_BOOTSTRAP_BENCH ?= ^BenchmarkCheckpointReplicaBootstrap10k$$
EXISTING_RECOVERY_BENCH ?= ^BenchmarkExistingReplicaRecovery10k$$
PARTITION_RESTORE_BENCH ?= ^Benchmark(LocalPartitionRestore100k|SnapshotRestoreGeneration100k)$$
PARTITION_WHOLE_KEYSPACE_BENCH ?= ^BenchmarkLocalPartitionWholeKeyspace100k$$
PARTITION_CURSOR_BENCH ?= ^BenchmarkPartitionReplicationPageTraversal100k$$
BACKUP_BENCH_KEYS ?= 10000
PARTITION_SCAN_BENCH_KEYS ?= 100000
PARTITION_CURSOR_BENCH_KEYS ?= 100000
PARTITION_CURSOR_BENCH_PAGE_SIZE ?= 1000
PARTITION_SNAPSHOT_BENCH ?= ^BenchmarkBigWins/Snapshot$$
PARTITION_SNAPSHOT_BENCH_KEYS ?= 100000
PARTITION_SNAPSHOT_COUNT ?= 16
PARTITION_RESTORE_BENCH_KEYS ?= 100000
PARTITION_RESTORE_COUNT ?= 16
COLD_HYDRATION_BENCH ?= ^BenchmarkColdReferenceParallelHydration32$$
REFERENCE_SLAB_BENCH ?= ^BenchmarkLevelDBReferenceRetainedMemory100k$$
STRING_STORAGE_BENCH ?= ^BenchmarkStringStorageLayout100k$$
STRING_STORAGE_BENCH_KEYS ?= 100000
STRING_COMPACTION_BENCH ?= ^BenchmarkStringCompaction100k$$
STRING_COMPACTION_GC_BENCH ?= ^BenchmarkStringCompactionPostGC100k$$
STRING_COMPACTION_GC_BENCHTIME ?= 20x
LIVE_REPLICATION_BENCH ?= ^BenchmarkReplicationLiveTransport10K/grpc-stream$$
REPLICATION_SPLIT_BENCH ?= BenchmarkSplitReplicationTaskGroupByMaxBytes
REPLICATION_SYNC_BENCH ?= BenchmarkHTTPReplicatorSyncAllBatching/Batched10k
REPLICATION_DIGEST_BENCH ?= BenchmarkReplicationDigestChangesDefaultWire
REPLICATION_ITERATOR_BENCH ?= BenchmarkReplicationDigest(SourceIteratorModes|FallbackCollectionModes)
REPLICATION_OPTIMIZATION_OUTPUT ?= replication-optimization.txt
MERKLE_MAINTENANCE_BENCH ?= ^BenchmarkReplicationMerkle(ChurnSnapshotCycle|SnapshotAfterChurn)$$
MERKLE_WRITE_BENCHTIME ?= 100000x
NATIVE_COMMAND_BATCH_BENCH ?= ^BenchmarkNativeCScalarBatch4096$$
SCALAR_BATCH_BENCH ?= ^BenchmarkBigWins/(NativeBatchStreamCommand|ScalarBatchStreamCommand(RepeatedKeys)?)$$
SCALAR_NATIVE_BATCH_BENCH ?= ^BenchmarkScalarNativeBatch$$
STRUCTURED_BATCH_BENCH ?= ^BenchmarkBigWins/(NativeStructuredBatchStreamCommand|StructuredBatchStreamCommand|StructuredBatchStreamSharedKey(Repeated)?|StructuredBatchStreamSharedColumns|StructuredBatchStreamSharedValue(Repeated)?)$$
CLOCK_BENCH ?= ^Benchmark(ClockSource|TrieClockSource)$$
BIG_WINS_BENCH ?= ^BenchmarkBigWins$$
BIG_WINS_KEYS ?= 100000
BIG_WINS_OPS ?= 100000
REDIS_HOST ?= 127.0.0.1
REDIS_PORT ?= 6379
REDIS_REQUESTS ?= 100000
REDIS_CLIENTS ?= 1
REDIS_KEYSPACE ?= 10000
REDIS_PIPELINE ?= 16
REDIS_START_DOCKER ?= 0
REDIS_DOCKER_IMAGE ?= redis:7.0.4
BENCHMARK_ARTIFACT_DIR ?=
SQL_BENCH_ROWS ?= 1000
SQL_BENCH_ITERATIONS ?= 5
TARANTOOL_REQUESTS ?= 10000
TARANTOOL_KEYSPACE ?= 10000
TARANTOOL_MEMTX_MEMORY ?= 268435456
TARANTOOL_PIPELINE ?= 16
TARANTOOL_BIN ?= tarantool
TARANTOOL_WORK_DIR ?=
HATRIE_COMMAND_BENCH ?= ^BenchmarkCommandFeature$$
HATRIE_TRANSPORT_BENCH ?= ^BenchmarkCommandTransportFeature$$
COMMAND_JSON_STRING_BENCH ?= ^BenchmarkCommandCanonicalJSONString$$
CANONICAL_STRING_LOOKUP_BENCH ?= ^BenchmarkPublicCanonicalStringLookups$$
HATRIE_PIPELINE_OPS ?= 16
BENCH_SMOKE_BENCHTIME ?= 5x
BENCH_SMOKE_COUNT ?= 1
BENCH_SMOKE_COMMAND_BENCH ?= ^BenchmarkCommandFeature/(StringGet|ReservoirSampleAdd)$$
BENCH_SMOKE_TRANSPORT_BENCH ?= ^BenchmarkCommandTransportFeature/InProcess/(StringSet|StringGet)$$
BENCH_SMOKE_SERIALIZATION_BENCH ?= Benchmark(CommandWireJSON|CommandWireProtobuf)$$
BENCH_SMOKE_CHECK_THRESHOLDS ?= 0
BENCH_SMOKE_MAX_COMMAND_NS_OP ?= 250000
BENCH_SMOKE_MAX_TRANSPORT_NS_OP ?= 500000
BENCH_SMOKE_MAX_SERIALIZATION_NS_OP ?= 250000
BENCH_SMOKE_MAX_B_OP ?= 1048576
BENCH_SMOKE_MAX_ALLOCS_OP ?= 512
BENCH_SMOKE_ARTIFACT_DIR ?=
BENCH_SMOKE_BASELINE_JSON ?=
BENCH_SMOKE_MAX_REGRESSION_PCT ?= 20
BENCH_SMOKE_COMPARE_MEMORY ?= 0
BENCH_SMOKE_RUN_ID ?=
VERIFY_LOCAL_DOCKER_COMPOSE ?= 0
BENCHMARK_MD_PATH ?= BENCHMARK.md
CONFIG_PATH ?=
CONFIG_PROFILE ?= production
SERVER_ARGS ?=
CHECK_CONFIG_ARGS ?=
PRINT_CONFIG_ARGS ?=
DOCKER_IMAGE ?= hatrie-cache:latest
DOCKERFILE ?= Dockerfile
DOCKER_BUILD_CONTEXT ?= .
DOCKER_PLATFORM ?=
DOCKER_TARGET ?=
DOCKER_BUILD_ARGS ?=

.PHONY: test verify verify-local verify-local-contract verify-go verify-race verify-c verify-native-cache-dependency verify-frontend verify-ops verify-benchmark-md-update test-sql-language-server format-sql-language-server sql-language-server review-sql-language-server commit-sql-language-server backup restore restore-bundle restore-rehearsal doctor cluster-status storage-status storage-flush storage-compact server check-config print-sane-config docker-build bench bench-serialization bench-journal-catchup bench-journal-wire bench-journal-apply bench-pebble-generation bench-pebble-backup bench-incremental-backup bench-atomic-restore bench-checkpoint-bootstrap bench-existing-recovery bench-partition-restore bench-partition-whole-keyspace bench-partition-cursor bench-partition-snapshot bench-cold-hydration bench-reference-slab bench-string-storage bench-string-compaction bench-structured-storage-codec bench-startup-persistence bench-live-replication bench-replication-optimizations bench-merkle-maintenance bench-native-ahtable-allocator bench-native-hattrie-lookup bench-native-command-batch bench-scalar-batch bench-scalar-native-batch bench-structured-batch bench-fastime bench-big-wins bench-storage-backends bench-default-construction bench-command-features bench-command-json-string bench-canonical-string-lookups bench-reservoir-small bench-reservoir-batch bench-bloom-header bench-bloom-scalar bench-cuckoo-scalar bench-hll-scalar bench-cms-scalar bench-set-scalar-generic bench-priority-queue-scalar bench-count-min-rows bench-fenwick-add bench-quantile-add bench-roaring-batch bench-topk-scalar bench-hatrie-command-features bench-hatrie-transport-features bench-redis-command-features bench-tarantool-command-features bench-command-comparison bench-smoke benchmark-md command-support run generate-proto cli monitoring-server frontend-install frontend-dev frontend-check frontend-test frontend-build frontend-smoke frontend-backend-smoke

test: verify-go

verify: verify-local

verify-local: verify-local-contract verify-go verify-c verify-frontend verify-ops verify-benchmark-md-update

verify-local-contract:
	VERIFY_LOCAL_DOCKER_COMPOSE='$(VERIFY_LOCAL_DOCKER_COMPOSE)' ./scripts/verify-local.sh

verify-go:
	./scripts/verify-go.sh

test-sql-language-server:
	sh ./scripts/test-sql-language-server.sh

test-sql-regex:
	sh ./scripts/test-sql-regex.sh

test-sql-time-zones:
	sh ./scripts/test-sql-time-zones.sh

test-sql-rewrite:
	sh ./scripts/test-sql-rewrite.sh

test-sql-correlated-subqueries:
	sh ./scripts/test-sql-correlated-subqueries.sh

test-sql-lateral:
	sh ./scripts/test-sql-lateral.sh

test-sql-aggregate-filter:
	sh ./scripts/test-sql-aggregate-filter.sh

test-sql-named-windows:
	sh ./scripts/test-sql-named-windows.sh

test-sql-parameterized-views:
	sh ./scripts/test-sql-parameterized-views.sh

test-sql-grouping-sets:
	sh ./scripts/test-sql-grouping-sets.sh

test-sql-pivot:
	sh ./scripts/test-sql-pivot.sh

format-sql-pivot:
	sh ./scripts/format-sql-pivot.sh

review-sql-pivot:
	sh ./scripts/review-sql-pivot.sh

commit-sql-pivot:
	sh ./scripts/commit-sql-pivot.sh

format-sql-grouping-sets:
	sh ./scripts/format-sql-grouping-sets.sh

format-sql-language-server:
	sh ./scripts/format-sql-language-server.sh

format-sql-regex:
	sh ./scripts/format-sql-regex.sh

format-sql-time-zones:
	sh ./scripts/format-sql-time-zones.sh

format-sql-rewrite:
	sh ./scripts/format-sql-rewrite.sh

format-sql-correlated-subqueries:
	sh ./scripts/format-sql-correlated-subqueries.sh

format-sql-lateral:
	sh ./scripts/format-sql-lateral.sh

format-sql-aggregate-filter:
	sh ./scripts/format-sql-aggregate-filter.sh

format-sql-named-windows:
	sh ./scripts/format-sql-named-windows.sh

format-sql-parameterized-views:
	sh ./scripts/format-sql-parameterized-views.sh

review-sql-parameterized-views:
	sh ./scripts/review-sql-parameterized-views.sh

commit-sql-parameterized-views:
	sh ./scripts/commit-sql-parameterized-views.sh

review-sql-named-windows:
	sh ./scripts/review-sql-named-windows.sh

commit-sql-named-windows:
	sh ./scripts/commit-sql-named-windows.sh

review-sql-aggregate-filter:
	sh ./scripts/review-sql-aggregate-filter.sh

commit-sql-aggregate-filter:
	sh ./scripts/commit-sql-aggregate-filter.sh

review-sql-lateral:
	sh ./scripts/review-sql-lateral.sh

commit-sql-lateral:
	sh ./scripts/commit-sql-lateral.sh

review-sql-correlated-subqueries:
	sh ./scripts/review-sql-correlated-subqueries.sh

commit-sql-correlated-subqueries:
	sh ./scripts/commit-sql-correlated-subqueries.sh

review-sql-rewrite:
	sh ./scripts/review-sql-rewrite.sh

commit-sql-rewrite:
	sh ./scripts/commit-sql-rewrite.sh

review-sql-time-zones:
	sh ./scripts/review-sql-time-zones.sh

commit-sql-time-zones:
	sh ./scripts/commit-sql-time-zones.sh

review-sql-regex:
	sh ./scripts/review-sql-regex.sh

commit-sql-regex:
	sh ./scripts/commit-sql-regex.sh

sql-language-server:
	sh ./scripts/sql-language-server.sh

review-sql-language-server:
	sh ./scripts/review-sql-language-server.sh

review-release-lsp-artifact:
	sh ./scripts/review-release-lsp-artifact.sh

commit-release-lsp-artifact:
	sh ./scripts/commit-release-lsp-artifact.sh

commit-sql-language-server:
	sh ./scripts/commit-sql-language-server.sh

verify-race:
	./scripts/verify-race.sh

verify-c:
	./scripts/verify-c-policy-test.sh
	SANITIZE_C='$(SANITIZE_C)' SANITIZE_C_ALLOW_STRICT_OVERCOMMIT='$(SANITIZE_C_ALLOW_STRICT_OVERCOMMIT)' SANITIZE_C_ALLOW_LOW_COMMIT_HEADROOM='$(SANITIZE_C_ALLOW_LOW_COMMIT_HEADROOM)' SANITIZE_C_OVERCOMMIT_MEMORY_PATH='$(SANITIZE_C_OVERCOMMIT_MEMORY_PATH)' SANITIZE_C_MEMINFO_PATH='$(SANITIZE_C_MEMINFO_PATH)' SANITIZE_C_ASAN_MIN_COMMIT_HEADROOM_KB='$(SANITIZE_C_ASAN_MIN_COMMIT_HEADROOM_KB)' ./scripts/verify-c.sh

verify-native-cache-dependency:
	./scripts/verify-native-cache-dependency.sh

verify-frontend:
	./scripts/frontend.sh verify

verify-ops:
	./scripts/verify-ops.sh

verify-benchmark-md-update:
	./scripts/verify-benchmark-md-update.sh

backup:
	DATA_DIR='$(DATA_DIR)' BACKUP_DIR='$(BACKUP_DIR)' BACKUP_OVERWRITE='$(BACKUP_OVERWRITE)' ./scripts/backup.sh

restore:
	DATA_DIR='$(DATA_DIR)' BACKUP_DIR='$(BACKUP_DIR)' RESTORE_OVERWRITE='$(RESTORE_OVERWRITE)' ./scripts/restore.sh

restore-bundle:
	DATA_DIR='$(DATA_DIR)' RESTORE_BUNDLE_PATH='$(RESTORE_BUNDLE_PATH)' RESTORE_BUNDLE_OVERWRITE='$(RESTORE_BUNDLE_OVERWRITE)' ./scripts/restore-bundle.sh

restore-rehearsal:
	RESTORE_REHEARSAL_PATH='$(RESTORE_REHEARSAL_PATH)' RESTORE_REHEARSAL_WORK_DIR='$(RESTORE_REHEARSAL_WORK_DIR)' RESTORE_REHEARSAL_KEEP_WORK_DIR='$(RESTORE_REHEARSAL_KEEP_WORK_DIR)' RESTORE_REHEARSAL_RUNTIME_CHECK='$(RESTORE_REHEARSAL_RUNTIME_CHECK)' RESTORE_REHEARSAL_RUNTIME_GET='$(RESTORE_REHEARSAL_RUNTIME_GET)' RESTORE_REHEARSAL_RUNTIME_SERVER_BIN='$(RESTORE_REHEARSAL_RUNTIME_SERVER_BIN)' ./scripts/restore-rehearsal.sh

doctor:
	DOCTOR_PATH='$(DOCTOR_PATH)' ./scripts/doctor.sh

cluster-status:
	CLUSTER_PEER='$(CLUSTER_PEER)' CLUSTER_PROBE_NODES='$(CLUSTER_PROBE_NODES)' ./scripts/cluster-status.sh

storage-status:
	STORAGE_PEER='$(STORAGE_PEER)' ./scripts/storage-status.sh

storage-flush:
	STORAGE_PEER='$(STORAGE_PEER)' ./scripts/storage-flush.sh

storage-compact:
	STORAGE_PEER='$(STORAGE_PEER)' STORAGE_COMPACT_START_KEY='$(STORAGE_COMPACT_START_KEY)' STORAGE_COMPACT_LIMIT_KEY='$(STORAGE_COMPACT_LIMIT_KEY)' ./scripts/storage-compact.sh

server:
	CONFIG_PATH='$(CONFIG_PATH)' ./scripts/server.sh $(SERVER_ARGS)

check-config:
	CONFIG_PATH='$(CONFIG_PATH)' ./scripts/check-config.sh $(CHECK_CONFIG_ARGS)

print-sane-config:
	CONFIG_PROFILE='$(CONFIG_PROFILE)' ./scripts/print-sane-config.sh $(PRINT_CONFIG_ARGS)

docker-build:
	DOCKER_IMAGE='$(DOCKER_IMAGE)' DOCKERFILE='$(DOCKERFILE)' DOCKER_BUILD_CONTEXT='$(DOCKER_BUILD_CONTEXT)' DOCKER_PLATFORM='$(DOCKER_PLATFORM)' DOCKER_TARGET='$(DOCKER_TARGET)' ./scripts/docker-build.sh $(DOCKER_BUILD_ARGS)

bench:
	go test -run '^$$' -bench='$(BENCH)' -benchmem

.PHONY: bench-sql
bench-sql:
	SQL_BENCH_ROWS='$(SQL_BENCH_ROWS)' SQL_BENCH_ITERATIONS='$(SQL_BENCH_ITERATIONS)' BENCHMARK_ARTIFACT_DIR='$(BENCHMARK_ARTIFACT_DIR)' ./scripts/benchmark-sql.sh

bench-serialization:
	SERIALIZATION_BENCH='$(SERIALIZATION_BENCH)' BENCHTIME='$(BENCHTIME)' COUNT='$(COUNT)' ./scripts/benchmark-serialization.sh

bench-journal-catchup:
	JOURNAL_CATCHUP_BENCH='$(JOURNAL_CATCHUP_BENCH)' BENCHTIME='$(BENCHTIME)' COUNT='$(COUNT)' ./scripts/benchmark-journal-catchup.sh

bench-journal-wire:
	JOURNAL_WIRE_BENCH='$(JOURNAL_WIRE_BENCH)' BENCHTIME='$(BENCHTIME)' COUNT='$(COUNT)' BENCHMARK_ARTIFACT_DIR='$(BENCHMARK_ARTIFACT_DIR)' ./scripts/benchmark-journal-wire.sh

bench-journal-apply:
	JOURNAL_APPLY_BENCH='$(JOURNAL_APPLY_BENCH)' BENCHTIME='$(BENCHTIME)' COUNT='$(COUNT)' BENCHMARK_ARTIFACT_DIR='$(BENCHMARK_ARTIFACT_DIR)' ./scripts/benchmark-journal-apply.sh

bench-pebble-generation:
	PEBBLE_GENERATION_BENCH='$(PEBBLE_GENERATION_BENCH)' BENCHTIME='$(BENCHTIME)' COUNT='$(COUNT)' BENCHMARK_ARTIFACT_DIR='$(BENCHMARK_ARTIFACT_DIR)' ./scripts/benchmark-pebble-generation.sh

bench-pebble-backup:
	PEBBLE_BACKUP_BENCH='$(PEBBLE_BACKUP_BENCH)' BACKUP_BENCH_KEYS='$(BACKUP_BENCH_KEYS)' BENCHTIME='$(BENCHTIME)' COUNT='$(COUNT)' BENCHMARK_ARTIFACT_DIR='$(BENCHMARK_ARTIFACT_DIR)' ./scripts/benchmark-pebble-backup.sh

bench-incremental-backup:
	INCREMENTAL_BACKUP_BENCH='$(INCREMENTAL_BACKUP_BENCH)' BACKUP_BENCH_KEYS='$(BACKUP_BENCH_KEYS)' BENCHTIME='$(BENCHTIME)' COUNT='$(COUNT)' BENCHMARK_ARTIFACT_DIR='$(BENCHMARK_ARTIFACT_DIR)' ./scripts/benchmark-incremental-backup.sh

bench-atomic-restore:
	ATOMIC_RESTORE_BENCH='$(ATOMIC_RESTORE_BENCH)' BACKUP_BENCH_KEYS='$(BACKUP_BENCH_KEYS)' BENCHTIME='$(BENCHTIME)' COUNT='$(COUNT)' BENCHMARK_ARTIFACT_DIR='$(BENCHMARK_ARTIFACT_DIR)' ./scripts/benchmark-atomic-restore.sh

bench-checkpoint-bootstrap:
	CHECKPOINT_BOOTSTRAP_BENCH='$(CHECKPOINT_BOOTSTRAP_BENCH)' BACKUP_BENCH_KEYS='$(BACKUP_BENCH_KEYS)' BENCHTIME='$(BENCHTIME)' COUNT='$(COUNT)' BENCHMARK_ARTIFACT_DIR='$(BENCHMARK_ARTIFACT_DIR)' ./scripts/benchmark-checkpoint-bootstrap.sh

bench-existing-recovery:
	EXISTING_RECOVERY_BENCH='$(EXISTING_RECOVERY_BENCH)' BACKUP_BENCH_KEYS='$(BACKUP_BENCH_KEYS)' BENCHTIME='$(BENCHTIME)' COUNT='$(COUNT)' BENCHMARK_ARTIFACT_DIR='$(BENCHMARK_ARTIFACT_DIR)' ./scripts/benchmark-existing-recovery.sh

bench-partition-restore:
	PARTITION_RESTORE_BENCH='$(PARTITION_RESTORE_BENCH)' PARTITION_RESTORE_BENCH_KEYS='$(PARTITION_RESTORE_BENCH_KEYS)' PARTITION_RESTORE_COUNT='$(PARTITION_RESTORE_COUNT)' BENCHTIME='$(BENCHTIME)' COUNT='$(COUNT)' BENCHMARK_ARTIFACT_DIR='$(BENCHMARK_ARTIFACT_DIR)' ./scripts/benchmark-partition-restore.sh

bench-partition-whole-keyspace:
	PARTITION_WHOLE_KEYSPACE_BENCH='$(PARTITION_WHOLE_KEYSPACE_BENCH)' PARTITION_SCAN_BENCH_KEYS='$(PARTITION_SCAN_BENCH_KEYS)' BENCHTIME='$(BENCHTIME)' COUNT='$(COUNT)' BENCHMARK_ARTIFACT_DIR='$(BENCHMARK_ARTIFACT_DIR)' ./scripts/benchmark-partition-whole-keyspace.sh

bench-partition-cursor:
	PARTITION_CURSOR_BENCH='$(PARTITION_CURSOR_BENCH)' PARTITION_CURSOR_BENCH_KEYS='$(PARTITION_CURSOR_BENCH_KEYS)' PARTITION_CURSOR_BENCH_PAGE_SIZE='$(PARTITION_CURSOR_BENCH_PAGE_SIZE)' BENCHTIME='$(BENCHTIME)' COUNT='$(COUNT)' BENCHMARK_ARTIFACT_DIR='$(BENCHMARK_ARTIFACT_DIR)' ./scripts/benchmark-partition-cursor.sh

bench-partition-snapshot:
	PARTITION_SNAPSHOT_BENCH='$(PARTITION_SNAPSHOT_BENCH)' PARTITION_SNAPSHOT_BENCH_KEYS='$(PARTITION_SNAPSHOT_BENCH_KEYS)' PARTITION_SNAPSHOT_COUNT='$(PARTITION_SNAPSHOT_COUNT)' BENCHTIME='$(BENCHTIME)' COUNT='$(COUNT)' BENCHMARK_ARTIFACT_DIR='$(BENCHMARK_ARTIFACT_DIR)' ./scripts/benchmark-partition-snapshot.sh

bench-cold-hydration:
	COLD_HYDRATION_BENCH='$(COLD_HYDRATION_BENCH)' BENCHTIME='$(BENCHTIME)' COUNT='$(COUNT)' BENCHMARK_ARTIFACT_DIR='$(BENCHMARK_ARTIFACT_DIR)' ./scripts/benchmark-cold-hydration.sh

bench-reference-slab:
	REFERENCE_SLAB_BENCH='$(REFERENCE_SLAB_BENCH)' BENCHTIME='$(BENCHTIME)' COUNT='$(COUNT)' BENCHMARK_ARTIFACT_DIR='$(BENCHMARK_ARTIFACT_DIR)' ./scripts/benchmark-reference-slab.sh

bench-string-storage:
	STRING_STORAGE_BENCH='$(STRING_STORAGE_BENCH)' STRING_STORAGE_BENCH_KEYS='$(STRING_STORAGE_BENCH_KEYS)' BENCHTIME='$(BENCHTIME)' COUNT='$(COUNT)' BENCHMARK_ARTIFACT_DIR='$(BENCHMARK_ARTIFACT_DIR)' ./scripts/benchmark-string-storage.sh

bench-string-compaction:
	STRING_COMPACTION_BENCH='$(STRING_COMPACTION_BENCH)' STRING_COMPACTION_GC_BENCH='$(STRING_COMPACTION_GC_BENCH)' STRING_COMPACTION_GC_BENCHTIME='$(STRING_COMPACTION_GC_BENCHTIME)' STRING_STORAGE_BENCH_KEYS='$(STRING_STORAGE_BENCH_KEYS)' BENCHTIME='$(BENCHTIME)' COUNT='$(COUNT)' BENCHMARK_ARTIFACT_DIR='$(BENCHMARK_ARTIFACT_DIR)' ./scripts/benchmark-string-compaction.sh

bench-structured-storage-codec:
	STRUCTURED_STORAGE_CODEC_BENCH='$(STRUCTURED_STORAGE_CODEC_BENCH)' BENCHTIME='$(BENCHTIME)' COUNT='$(COUNT)' BENCHMARK_ARTIFACT_DIR='$(BENCHMARK_ARTIFACT_DIR)' ./scripts/benchmark-structured-storage-codec.sh

bench-startup-persistence:
	STARTUP_PERSISTENCE_BENCH='$(STARTUP_PERSISTENCE_BENCH)' BENCHTIME='$(BENCHTIME)' COUNT='$(COUNT)' BENCHMARK_ARTIFACT_DIR='$(BENCHMARK_ARTIFACT_DIR)' ./scripts/benchmark-startup-persistence.sh

bench-live-replication:
	LIVE_REPLICATION_BENCH='$(LIVE_REPLICATION_BENCH)' BENCHTIME='$(BENCHTIME)' COUNT='$(COUNT)' BENCHMARK_ARTIFACT_DIR='$(BENCHMARK_ARTIFACT_DIR)' ./scripts/benchmark-live-replication.sh

bench-replication-optimizations:
	REPLICATION_SPLIT_BENCH='$(REPLICATION_SPLIT_BENCH)' REPLICATION_SYNC_BENCH='$(REPLICATION_SYNC_BENCH)' REPLICATION_DIGEST_BENCH='$(REPLICATION_DIGEST_BENCH)' REPLICATION_ITERATOR_BENCH='$(REPLICATION_ITERATOR_BENCH)' REPLICATION_OPTIMIZATION_OUTPUT='$(REPLICATION_OPTIMIZATION_OUTPUT)' BENCHTIME='$(BENCHTIME)' COUNT='$(COUNT)' BENCHMARK_ARTIFACT_DIR='$(BENCHMARK_ARTIFACT_DIR)' ./scripts/benchmark-replication-optimizations.sh

bench-merkle-maintenance:
	MERKLE_MAINTENANCE_BENCH='$(MERKLE_MAINTENANCE_BENCH)' MERKLE_WRITE_BENCHTIME='$(MERKLE_WRITE_BENCHTIME)' BENCHTIME='$(BENCHTIME)' COUNT='$(COUNT)' BENCHMARK_ARTIFACT_DIR='$(BENCHMARK_ARTIFACT_DIR)' ./scripts/benchmark-merkle-maintenance.sh

bench-native-ahtable-allocator:
	NATIVE_AHTABLE_KEYS='$(NATIVE_AHTABLE_KEYS)' NATIVE_AHTABLE_SLOTS='$(NATIVE_AHTABLE_SLOTS)' NATIVE_AHTABLE_LOOKUPS='$(NATIVE_AHTABLE_LOOKUPS)' COUNT='$(COUNT)' BENCHMARK_ARTIFACT_DIR='$(BENCHMARK_ARTIFACT_DIR)' ./scripts/benchmark-native-ahtable-allocator.sh

bench-native-hattrie-lookup:
	NATIVE_HATTRIE_KEYS='$(NATIVE_HATTRIE_KEYS)' NATIVE_HATTRIE_LOOKUPS='$(NATIVE_HATTRIE_LOOKUPS)' NATIVE_HATTRIE_KEY_MODE='$(NATIVE_HATTRIE_KEY_MODE)' NATIVE_HATTRIE_INSERT_REPETITIONS='$(NATIVE_HATTRIE_INSERT_REPETITIONS)' COUNT='$(COUNT)' BENCHMARK_ARTIFACT_DIR='$(BENCHMARK_ARTIFACT_DIR)' ./scripts/benchmark-native-hattrie-lookup.sh

bench-native-command-batch:
	NATIVE_COMMAND_BATCH_BENCH='$(NATIVE_COMMAND_BATCH_BENCH)' BENCHTIME='$(BENCHTIME)' COUNT='$(COUNT)' BENCHMARK_ARTIFACT_DIR='$(BENCHMARK_ARTIFACT_DIR)' ./scripts/benchmark-native-command-batch.sh

bench-scalar-batch:
	SCALAR_BATCH_BENCH='$(SCALAR_BATCH_BENCH)' BIG_WINS_OPS='$(BIG_WINS_OPS)' BENCHTIME='$(BENCHTIME)' COUNT='$(COUNT)' BENCHMARK_ARTIFACT_DIR='$(BENCHMARK_ARTIFACT_DIR)' ./scripts/benchmark-scalar-batch.sh

bench-scalar-native-batch:
	SCALAR_NATIVE_BATCH_BENCH='$(SCALAR_NATIVE_BATCH_BENCH)' BENCHTIME='$(BENCHTIME)' COUNT='$(COUNT)' BENCHMARK_ARTIFACT_DIR='$(BENCHMARK_ARTIFACT_DIR)' ./scripts/benchmark-scalar-native-batch.sh

bench-structured-batch:
	STRUCTURED_BATCH_BENCH='$(STRUCTURED_BATCH_BENCH)' BIG_WINS_OPS='$(BIG_WINS_OPS)' BENCHTIME='$(BENCHTIME)' COUNT='$(COUNT)' BENCHMARK_ARTIFACT_DIR='$(BENCHMARK_ARTIFACT_DIR)' ./scripts/benchmark-structured-batch.sh

bench-fastime:
	CLOCK_BENCH='$(CLOCK_BENCH)' BENCHTIME='$(BENCHTIME)' COUNT='$(COUNT)' BENCHMARK_ARTIFACT_DIR='$(BENCHMARK_ARTIFACT_DIR)' ./scripts/benchmark-fastime.sh

bench-big-wins:
	BIG_WINS_BENCH='$(BIG_WINS_BENCH)' BIG_WINS_KEYS='$(BIG_WINS_KEYS)' BIG_WINS_OPS='$(BIG_WINS_OPS)' BENCHTIME='$(BENCHTIME)' COUNT='$(COUNT)' ./scripts/benchmark-big-wins.sh

bench-storage-backends:
	BENCHTIME='$(BENCHTIME)' COUNT='$(COUNT)' BENCHMARK_ARTIFACT_DIR='$(BENCHMARK_ARTIFACT_DIR)' ./scripts/benchmark-storage-backends.sh

bench-default-construction:
	DEFAULT_CONSTRUCTION_BENCH='$(DEFAULT_CONSTRUCTION_BENCH)' BENCHTIME='$(BENCHTIME)' COUNT='$(COUNT)' ./scripts/benchmark-default-construction.sh

bench-command-features:
	go test -run '^$$' -bench='^BenchmarkCommandFeature$$' -benchmem -count='$(COUNT)' $(if $(BENCHTIME),-benchtime='$(BENCHTIME)')

bench-command-json-string:
	COMMAND_JSON_STRING_BENCH='$(COMMAND_JSON_STRING_BENCH)' BENCHTIME='$(BENCHTIME)' COUNT='$(COUNT)' ./scripts/benchmark-command-json-string.sh

bench-canonical-string-lookups:
	CANONICAL_STRING_LOOKUP_BENCH='$(CANONICAL_STRING_LOOKUP_BENCH)' BENCHTIME='$(BENCHTIME)' COUNT='$(COUNT)' ./scripts/benchmark-canonical-string-lookups.sh

bench-reservoir-small:
	RESERVOIR_SMALL_BENCH='$(RESERVOIR_SMALL_BENCH)' BENCHTIME='$(BENCHTIME)' COUNT='$(COUNT)' ./scripts/benchmark-reservoir-small.sh

bench-reservoir-batch:
	RESERVOIR_BATCH_PATH_BENCH='$(RESERVOIR_BATCH_PATH_BENCH)' RESERVOIR_BATCH_ALTERNATING_BENCH='$(RESERVOIR_BATCH_ALTERNATING_BENCH)' RESERVOIR_BATCH_ALTERNATING_BENCHTIME='$(RESERVOIR_BATCH_ALTERNATING_BENCHTIME)' BENCHTIME='$(BENCHTIME)' COUNT='$(COUNT)' ./scripts/benchmark-reservoir-batch.sh

bench-bloom-header:
	BLOOM_HEADER_LAYOUT_BENCH='$(BLOOM_HEADER_LAYOUT_BENCH)' BLOOM_HEADER_OPERATION_BENCH='$(BLOOM_HEADER_OPERATION_BENCH)' BLOOM_HEADER_LAYOUT_BENCHTIME='$(BLOOM_HEADER_LAYOUT_BENCHTIME)' BENCHTIME='$(BENCHTIME)' COUNT='$(COUNT)' ./scripts/benchmark-bloom-header.sh

bench-count-min-rows:
	COUNT_MIN_ROWS_BENCH='$(COUNT_MIN_ROWS_BENCH)' BENCHTIME='$(BENCHTIME)' COUNT='$(COUNT)' ./scripts/benchmark-count-min-rows.sh

bench-fenwick-add:
	FENWICK_ADD_BENCH='$(FENWICK_ADD_BENCH)' BENCHTIME='$(BENCHTIME)' COUNT='$(COUNT)' ./scripts/benchmark-fenwick-add.sh

bench-quantile-add:
	QUANTILE_ADD_BENCH='$(QUANTILE_ADD_BENCH)' BENCHTIME='$(BENCHTIME)' COUNT='$(COUNT)' ./scripts/benchmark-quantile-add.sh

bench-quantile-batch:
	QUANTILE_BATCH_PATH_BENCH='$(QUANTILE_BATCH_PATH_BENCH)' QUANTILE_BATCH_ALTERNATING_BENCH='$(QUANTILE_BATCH_ALTERNATING_BENCH)' QUANTILE_BATCH_ALTERNATING_BENCHTIME='$(QUANTILE_BATCH_ALTERNATING_BENCHTIME)' QUANTILE_BATCH_CONTROL_BENCH='$(QUANTILE_BATCH_CONTROL_BENCH)' QUANTILE_BATCH_CONTROL_BENCHTIME='$(QUANTILE_BATCH_CONTROL_BENCHTIME)' QUANTILE_BATCH_BASELINE_BINARY='$(QUANTILE_BATCH_BASELINE_BINARY)' QUANTILE_BATCH_CANDIDATE_BINARY='$(QUANTILE_BATCH_CANDIDATE_BINARY)' BENCHTIME='$(BENCHTIME)' COUNT='$(COUNT)' ./scripts/benchmark-quantile-batch.sh

bench-roaring-batch:
	ROARING_BATCH_PATH_BENCH='$(ROARING_BATCH_PATH_BENCH)' ROARING_BATCH_ALTERNATING_BENCH='$(ROARING_BATCH_ALTERNATING_BENCH)' ROARING_BATCH_ALTERNATING_BENCHTIME='$(ROARING_BATCH_ALTERNATING_BENCHTIME)' ROARING_BATCH_CONTROL_BENCH='$(ROARING_BATCH_CONTROL_BENCH)' ROARING_BATCH_CONTROL_BENCHTIME='$(ROARING_BATCH_CONTROL_BENCHTIME)' ROARING_BATCH_BASELINE_BINARY='$(ROARING_BATCH_BASELINE_BINARY)' ROARING_BATCH_CANDIDATE_BINARY='$(ROARING_BATCH_CANDIDATE_BINARY)' BENCHTIME='$(BENCHTIME)' COUNT='$(COUNT)' ./scripts/benchmark-roaring-batch.sh

bench-topk-scalar:
	TOP_K_SCALAR_BENCH='$(TOP_K_SCALAR_BENCH)' BENCHTIME='$(BENCHTIME)' COUNT='$(COUNT)' ./scripts/benchmark-topk-scalar.sh

bench-bloom-scalar:
	BLOOM_SCALAR_BENCH='$(BLOOM_SCALAR_BENCH)' BENCHTIME='$(BENCHTIME)' COUNT='$(COUNT)' ./scripts/benchmark-bloom-scalar.sh

bench-cuckoo-scalar:
	CUCKOO_SCALAR_BENCH='$(CUCKOO_SCALAR_BENCH)' BENCHTIME='$(BENCHTIME)' COUNT='$(COUNT)' ./scripts/benchmark-cuckoo-scalar.sh

bench-hll-scalar:
	HLL_SCALAR_BENCH='$(HLL_SCALAR_BENCH)' BENCHTIME='$(BENCHTIME)' COUNT='$(COUNT)' ./scripts/benchmark-hll-scalar.sh

bench-cms-scalar:
	CMS_SCALAR_BENCH='$(CMS_SCALAR_BENCH)' BENCHTIME='$(BENCHTIME)' COUNT='$(COUNT)' ./scripts/benchmark-cms-scalar.sh

bench-set-scalar-generic:
	SET_SCALAR_GENERIC_BENCH='$(SET_SCALAR_GENERIC_BENCH)' BENCHTIME='$(BENCHTIME)' COUNT='$(COUNT)' ./scripts/benchmark-set-scalar-generic.sh

bench-priority-queue-scalar:
	PRIORITY_QUEUE_SCALAR_BENCH='$(PRIORITY_QUEUE_SCALAR_BENCH)' BENCHTIME='$(BENCHTIME)' COUNT='$(COUNT)' ./scripts/benchmark-priority-queue-scalar.sh

bench-hatrie-command-features:
	HATRIE_BENCH='$(HATRIE_COMMAND_BENCH)' HATRIE_PIPELINE_OPS='$(HATRIE_PIPELINE_OPS)' BENCHTIME='$(BENCHTIME)' COUNT='$(COUNT)' BENCHMARK_ARTIFACT_DIR='$(BENCHMARK_ARTIFACT_DIR)' ./scripts/benchmark-hatrie-command-features.sh

bench-hatrie-transport-features:
	HATRIE_TRANSPORT_BENCH='$(HATRIE_TRANSPORT_BENCH)' BENCHTIME='$(BENCHTIME)' COUNT='$(COUNT)' ./scripts/benchmark-hatrie-transport-features.sh

bench-redis-command-features:
	REDIS_HOST='$(REDIS_HOST)' REDIS_PORT='$(REDIS_PORT)' REDIS_REQUESTS='$(REDIS_REQUESTS)' REDIS_CLIENTS='$(REDIS_CLIENTS)' REDIS_KEYSPACE='$(REDIS_KEYSPACE)' REDIS_PIPELINE='$(REDIS_PIPELINE)' REDIS_START_DOCKER='$(REDIS_START_DOCKER)' REDIS_DOCKER_IMAGE='$(REDIS_DOCKER_IMAGE)' BENCHMARK_ARTIFACT_DIR='$(BENCHMARK_ARTIFACT_DIR)' ./scripts/benchmark-redis-command-features.sh

bench-tarantool-command-features:
	TARANTOOL_REQUESTS='$(TARANTOOL_REQUESTS)' TARANTOOL_KEYSPACE='$(TARANTOOL_KEYSPACE)' TARANTOOL_MEMTX_MEMORY='$(TARANTOOL_MEMTX_MEMORY)' TARANTOOL_PIPELINE='$(TARANTOOL_PIPELINE)' TARANTOOL_BIN='$(TARANTOOL_BIN)' TARANTOOL_WORK_DIR='$(TARANTOOL_WORK_DIR)' BENCHMARK_ARTIFACT_DIR='$(BENCHMARK_ARTIFACT_DIR)' ./scripts/benchmark-tarantool-command-features.sh

bench-command-comparison:
	BENCHMARK_ARTIFACT_DIR='$(BENCHMARK_ARTIFACT_DIR)' ./scripts/benchmark-command-comparison.sh

bench-smoke:
	BENCH_SMOKE_BENCHTIME='$(BENCH_SMOKE_BENCHTIME)' BENCH_SMOKE_COUNT='$(BENCH_SMOKE_COUNT)' BENCH_SMOKE_COMMAND_BENCH='$(BENCH_SMOKE_COMMAND_BENCH)' BENCH_SMOKE_TRANSPORT_BENCH='$(BENCH_SMOKE_TRANSPORT_BENCH)' BENCH_SMOKE_SERIALIZATION_BENCH='$(BENCH_SMOKE_SERIALIZATION_BENCH)' BENCH_SMOKE_CHECK_THRESHOLDS='$(BENCH_SMOKE_CHECK_THRESHOLDS)' BENCH_SMOKE_MAX_COMMAND_NS_OP='$(BENCH_SMOKE_MAX_COMMAND_NS_OP)' BENCH_SMOKE_MAX_TRANSPORT_NS_OP='$(BENCH_SMOKE_MAX_TRANSPORT_NS_OP)' BENCH_SMOKE_MAX_SERIALIZATION_NS_OP='$(BENCH_SMOKE_MAX_SERIALIZATION_NS_OP)' BENCH_SMOKE_MAX_B_OP='$(BENCH_SMOKE_MAX_B_OP)' BENCH_SMOKE_MAX_ALLOCS_OP='$(BENCH_SMOKE_MAX_ALLOCS_OP)' BENCH_SMOKE_ARTIFACT_DIR='$(BENCH_SMOKE_ARTIFACT_DIR)' BENCH_SMOKE_BASELINE_JSON='$(BENCH_SMOKE_BASELINE_JSON)' BENCH_SMOKE_MAX_REGRESSION_PCT='$(BENCH_SMOKE_MAX_REGRESSION_PCT)' BENCH_SMOKE_COMPARE_MEMORY='$(BENCH_SMOKE_COMPARE_MEMORY)' BENCH_SMOKE_RUN_ID='$(BENCH_SMOKE_RUN_ID)' ./scripts/benchmark-smoke.sh

benchmark-md:
	BENCHMARK_ARTIFACT_DIR='$(BENCHMARK_ARTIFACT_DIR)' BENCHMARK_MD_PATH='$(BENCHMARK_MD_PATH)' ./scripts/update-benchmark-md.sh

command-support:
	./scripts/command-support.sh

audit-sql-capabilities:
	sh ./scripts/audit-sql-capabilities.sh

audit-sql-migrations:
	AUDIT_SQL_MODE=migrations sh ./scripts/audit-sql-capabilities.sh

audit-sql-types:
	AUDIT_SQL_MODE=types sh ./scripts/audit-sql-capabilities.sh

audit-sql-constraints:
	AUDIT_SQL_MODE=constraints sh ./scripts/audit-sql-capabilities.sh

audit-sql-quality:
	AUDIT_SQL_MODE=quality sh ./scripts/audit-sql-capabilities.sh

audit-hatcache-boundaries:
	sh ./scripts/audit-hatcache-boundaries.sh

audit-next-sql-improvements:
	sh ./scripts/audit-next-sql-improvements.sh

inspect-sql-engine:
	sh ./scripts/inspect-sql-engine.sh

show-sql-mutation-engine:
	sh ./scripts/show-sql-mutation-engine.sh

show-sql-mutation-tests:
	sh ./scripts/show-sql-mutation-tests.sh

show-hattrie-core:
	sh ./scripts/show-hattrie-core.sh

show-scalar-command-path:
	sh ./scripts/show-scalar-command-path.sh

test-sql-mutations:
	sh ./scripts/test-sql-mutations.sh

verify-sql-mutation-feature:
	sh ./scripts/verify-sql-mutation-feature.sh

commit-sql-mutation-feature:
	sh ./scripts/commit-sql-mutation-feature.sh

show-sql-relational-extension-points:
	sh ./scripts/show-sql-relational-extension-points.sh

check-next-sql-feature-symbols:
	sh ./scripts/check-next-sql-feature-symbols.sh

show-sql-source-parser:
	sh ./scripts/show-sql-source-parser.sh

show-sql-source-execution:
	sh ./scripts/show-sql-source-execution.sh

test-sql-plan-guards:
	sh ./scripts/test-sql-plan-guards.sh

test-sql-limit-by:
	sh ./scripts/test-sql-limit-by.sh

test-sql-hash-aggregate:
	sh ./scripts/test-sql-hash-aggregate.sh

format-sql-hash-aggregate:
	sh ./scripts/format-sql-hash-aggregate.sh

benchmark-sql-hash-aggregate:
	sh ./scripts/benchmark-sql-hash-aggregate.sh

benchmark-sql-hash-aggregate-all:
	HASH_AGGREGATE_BENCH_MODE=all sh ./scripts/benchmark-sql-hash-aggregate.sh

.PHONY: verify-sql-hash-aggregate
verify-sql-hash-aggregate:
	sh ./scripts/verify-sql-hash-aggregate.sh

.PHONY: stage-sql-hash-aggregate
stage-sql-hash-aggregate:
	sh ./scripts/commit-sql-hash-aggregate.sh stage

.PHONY: commit-sql-hash-aggregate
commit-sql-hash-aggregate:
	sh ./scripts/commit-sql-hash-aggregate.sh commit

.PHONY: push-sql-hash-aggregate
push-sql-hash-aggregate:
	sh ./scripts/commit-sql-hash-aggregate.sh push

format-sql-limit-by:
	sh ./scripts/format-sql-limit-by.sh

benchmark-sql-limit-by:
	sh ./scripts/benchmark-sql-limit-by.sh

benchmark-sql-limit-by-all:
	LIMIT_BY_BENCH_MODE=all sh ./scripts/benchmark-sql-limit-by.sh

stage-sql-limit-by:
	sh ./scripts/commit-sql-limit-by.sh stage

commit-sql-limit-by:
	sh ./scripts/commit-sql-limit-by.sh commit

push-sql-limit-by:
	sh ./scripts/commit-sql-limit-by.sh push

test-sql-table-functions:
	sh ./scripts/test-sql-table-functions.sh

format-sql-table-functions:
	sh ./scripts/format-sql-table-functions.sh

verify-sql-table-function-feature:
	sh ./scripts/verify-sql-table-function-feature.sh

inspect-sql-table-function-feature:
	sh ./scripts/inspect-sql-table-function-feature.sh

commit-sql-table-function-feature:
	sh ./scripts/commit-sql-table-function-feature.sh

show-sql-source-model:
	sh ./scripts/show-sql-source-model.sh

show-sql-json-index-engine:
	sh ./scripts/show-sql-json-index-engine.sh

test-sql-json-paths:
	sh ./scripts/test-sql-json-paths.sh

format-sql-json-paths:
	sh ./scripts/format-sql-json-paths.sh

verify-sql-json-path-feature:
	sh ./scripts/verify-sql-json-path-feature.sh

inspect-sql-json-path-feature:
	sh ./scripts/inspect-sql-json-path-feature.sh

commit-sql-json-path-feature:
	sh ./scripts/commit-sql-json-path-feature.sh

show-sql-bitmap-index-engine:
	sh ./scripts/show-sql-bitmap-index-engine.sh

test-sql-bitmap-indexes:
	sh ./scripts/test-sql-bitmap-indexes.sh

test-sql-secondary-indexes:
	sh ./scripts/test-sql-secondary-indexes.sh

format-sql-secondary-indexes:
	sh ./scripts/format-sql-secondary-indexes.sh

review-sql-secondary-indexes:
	sh ./scripts/review-sql-secondary-indexes.sh

commit-sql-secondary-indexes:
	sh ./scripts/commit-sql-secondary-indexes.sh

format-sql-bitmap-indexes:
	sh ./scripts/format-sql-bitmap-indexes.sh

verify-sql-bitmap-index-feature:
	sh ./scripts/verify-sql-bitmap-index-feature.sh

inspect-sql-bitmap-index-feature:
	sh ./scripts/inspect-sql-bitmap-index-feature.sh

commit-sql-bitmap-index-feature:
	sh ./scripts/commit-sql-bitmap-index-feature.sh

show-sql-approx-engine:
	sh ./scripts/show-sql-approx-engine.sh

test-sql-approx-aggregates:
	sh ./scripts/test-sql-approx-aggregates.sh

format-sql-approx-aggregates:
	sh ./scripts/format-sql-approx-aggregates.sh

benchmark-sql-approx-aggregates:
	sh ./scripts/benchmark-sql-approx-aggregates.sh

verify-sql-approx-feature:
	sh ./scripts/verify-sql-approx-feature.sh

inspect-sql-approx-feature:
	sh ./scripts/inspect-sql-approx-feature.sh

commit-sql-approx-feature:
	sh ./scripts/commit-sql-approx-feature.sh

push-sql-approx-feature:
	sh ./scripts/push-sql-approx-feature.sh

show-sql-sampling-engine:
	sh ./scripts/show-sql-sampling-engine.sh

test-sql-table-sampling:
	sh ./scripts/test-sql-table-sampling.sh

show-sql-row-stream-fallback:
	sh ./scripts/show-sql-row-stream-fallback.sh

format-sql-table-sampling:
	sh ./scripts/format-sql-table-sampling.sh

benchmark-sql-table-sampling:
	sh ./scripts/benchmark-sql-table-sampling.sh

verify-sql-table-sampling:
	sh ./scripts/verify-sql-table-sampling.sh

inspect-sql-table-sampling:
	sh ./scripts/inspect-sql-table-sampling.sh

commit-sql-table-sampling:
	sh ./scripts/commit-sql-table-sampling.sh

push-sql-table-sampling:
	sh ./scripts/push-sql-table-sampling.sh

show-sql-telemetry-engine:
	sh ./scripts/show-sql-telemetry-engine.sh

test-sql-telemetry:
	sh ./scripts/test-sql-telemetry.sh

format-sql-telemetry:
	sh ./scripts/format-sql-telemetry.sh

verify-sql-telemetry:
	sh ./scripts/verify-sql-telemetry.sh

inspect-sql-telemetry:
	sh ./scripts/inspect-sql-telemetry.sh

commit-sql-telemetry:
	sh ./scripts/commit-sql-telemetry.sh

push-sql-telemetry:
	sh ./scripts/push-sql-telemetry.sh

verify-sql-improvement-goal:
	sh ./scripts/verify-sql-improvement-goal.sh

inspect-sql-index-freshness:
	sh ./scripts/inspect-sql-index-freshness.sh

test-sql-columnar-layout-cache:
	sh ./scripts/test-sql-columnar-layout-cache.sh

benchmark-sql-columnar-layout-cache:
	sh ./scripts/benchmark-sql-columnar-layout-cache.sh

deliver-sql-columnar-layout-cache:
	sh ./scripts/deliver-sql-columnar-layout-cache.sh

test-sql-job-scheduler:
	sh ./scripts/test-sql-job-scheduler.sh

deliver-sql-job-scheduler:
	sh ./scripts/deliver-sql-job-scheduler.sh

test-sql-alert-rules:
	sh ./scripts/test-sql-alert-rules.sh

deliver-sql-alert-rules:
	sh ./scripts/deliver-sql-alert-rules.sh

test-sql-refresh-scheduler:
	sh ./scripts/test-sql-refresh-scheduler.sh

deliver-sql-refresh-scheduler:
	sh ./scripts/deliver-sql-refresh-scheduler.sh

test-sql-retention:
	sh ./scripts/test-sql-retention.sh

deliver-sql-retention:
	sh ./scripts/deliver-sql-retention.sh

test-sql-index-advisor:
	sh ./scripts/test-sql-index-advisor.sh

test-sql-index-usage:
	sh ./scripts/test-sql-index-usage.sh

deliver-sql-index-intelligence:
	sh ./scripts/deliver-sql-index-intelligence.sh

test-sql-maintenance-window:
	sh ./scripts/test-sql-maintenance-window.sh

deliver-sql-maintenance-window:
	sh ./scripts/deliver-sql-maintenance-window.sh

show-concurrency-coverage:
	sh ./scripts/show-concurrency-coverage.sh

test-command-concurrency:
	sh ./scripts/test-command-concurrency.sh

test-command-concurrency-race:
	sh ./scripts/test-command-concurrency-race.sh

verify-command-concurrency:
	sh ./scripts/verify-command-concurrency.sh

format-command-concurrency:
	sh ./scripts/format-command-concurrency.sh

inspect-command-concurrency:
	sh ./scripts/inspect-command-concurrency.sh

commit-command-concurrency:
	sh ./scripts/commit-command-concurrency.sh

push-command-concurrency:
	sh ./scripts/push-command-concurrency.sh

test-command-protocol:
	sh ./scripts/test-command-protocol.sh

format-command-protocol:
	sh ./scripts/format-command-protocol.sh

commit-command-protocol:
	sh ./scripts/commit-command-protocol.sh

push-command-protocol:
	sh ./scripts/push-command-protocol.sh

.PHONY: format-command-idempotency
format-command-idempotency:
	sh ./scripts/format-command-idempotency.sh

.PHONY: test-command-journal-wire
test-command-journal-wire:
	sh ./scripts/test-command-journal-wire.sh

.PHONY: test-idempotency-wire
test-idempotency-wire:
	sh ./scripts/test-idempotency-wire.sh

.PHONY: test-journal-idempotency-config
test-journal-idempotency-config:
	sh ./scripts/test-journal-idempotency-config.sh

test-sql-tooling:
	sh ./scripts/test-sql-tooling.sh

format-sql-tooling:
	sh ./scripts/format-sql-tooling.sh

commit-sql-tooling:
	sh ./scripts/commit-sql-tooling.sh

push-sql-tooling:
	sh ./scripts/push-sql-tooling.sh

test-cli-sql-repl:
	sh ./scripts/test-cli-sql-repl.sh

format-cli-sql-repl:
	sh ./scripts/format-cli-sql-repl.sh

commit-cli-sql-repl:
	sh ./scripts/commit-cli-sql-repl.sh

push-cli-sql-repl:
	sh ./scripts/push-cli-sql-repl.sh

test-sql-fixtures:
	sh ./scripts/test-sql-fixtures.sh

format-sql-fixtures:
	sh ./scripts/format-sql-fixtures.sh

commit-sql-fixtures:
	sh ./scripts/commit-sql-fixtures.sh

push-sql-fixtures:
	sh ./scripts/push-sql-fixtures.sh

test-sql-governance:
	sh ./scripts/test-sql-governance.sh

format-sql-governance:
	sh ./scripts/format-sql-governance.sh

commit-sql-governance:
	sh ./scripts/commit-sql-governance.sh

push-sql-governance:
	sh ./scripts/push-sql-governance.sh

amend-sql-governance:
	sh ./scripts/amend-sql-governance.sh

test-sql-storage-adapter:
	sh ./scripts/test-sql-storage-adapter.sh

format-sql-storage-adapter:
	sh ./scripts/format-sql-storage-adapter.sh

commit-sql-storage-adapter:
	sh ./scripts/commit-sql-storage-adapter.sh

push-sql-storage-adapter:
	sh ./scripts/push-sql-storage-adapter.sh

test-stream-cipher:
	sh ./scripts/test-stream-cipher.sh

format-stream-cipher:
	sh ./scripts/format-stream-cipher.sh

test-sql-spill-encryption:
	sh ./scripts/test-sql-spill-encryption.sh

format-sql-spill-encryption:
	sh ./scripts/format-sql-spill-encryption.sh

commit-encrypted-spills:
	sh ./scripts/commit-encrypted-spills.sh

push-encrypted-spills:
	sh ./scripts/push-encrypted-spills.sh

test-persistent-encryption:
	sh ./scripts/test-persistent-encryption.sh

format-persistent-encryption:
	sh ./scripts/format-persistent-encryption.sh

commit-leveldb-record-encryption:
	sh ./scripts/commit-leveldb-record-encryption.sh

push-leveldb-record-encryption:
	sh ./scripts/push-leveldb-record-encryption.sh

commit-pebble-record-encryption:
	sh ./scripts/commit-pebble-record-encryption.sh

push-pebble-record-encryption:
	sh ./scripts/push-pebble-record-encryption.sh

release-build:
	sh ./scripts/release-build.sh

.PHONY: verify-release-reproducibility
verify-release-reproducibility:
	sh ./scripts/verify-release-reproducibility.sh

review-release-reproducibility:
	sh ./scripts/review-release-reproducibility.sh

commit-release-reproducibility:
	sh ./scripts/commit-release-reproducibility.sh

release-sbom:
	sh ./scripts/release-sbom.sh

verify-vulnerabilities:
	sh ./scripts/verify-vulnerabilities.sh

review-toolchain-security:
	sh ./scripts/review-toolchain-security.sh

commit-toolchain-security:
	sh ./scripts/commit-toolchain-security.sh

verify-long-running:
	sh ./scripts/verify-long-running.sh

format-release-tooling:
	sh ./scripts/format-release-tooling.sh

commit-release-ci:
	sh ./scripts/commit-release-ci.sh

push-release-ci:
	sh ./scripts/push-release-ci.sh







commit-sql-improvement-goal-verification:
	sh ./scripts/commit-sql-improvement-goal-verification.sh

push-sql-improvement-goal-verification:
	sh ./scripts/push-sql-improvement-goal-verification.sh

show-sql-function-dispatch:
	sh ./scripts/show-sql-function-dispatch.sh

show-sql-aggregate-docs:
	sh ./scripts/show-sql-aggregate-docs.sh

show-benchmark-sql-section:
	sh ./scripts/show-benchmark-sql-section.sh

show-new-packages:
	sh ./scripts/show-new-packages.sh

format-sql-plan-guards:
	sh ./scripts/format-sql-plan-guards.sh

commit-sql-plan-guard:
	sh ./scripts/commit-sql-plan-guard.sh

format-sql-mutations:
	sh ./scripts/format-sql-mutations.sh

show-sql-mutation-docs:
	sh ./scripts/show-sql-mutation-docs.sh

show-sql-keyword-inventory:
	sh ./scripts/show-sql-keyword-inventory.sh

verify-sql-capabilities:
	sh ./scripts/verify-sql-capabilities.sh

commit-sql-audit:
	sh ./scripts/commit-sql-audit.sh

run:
	@CMD='$(CMD)' ./scripts/run.sh

generate-proto:
	./scripts/generate-proto.sh

cli:
	./scripts/cli.sh $(ARGS)

monitoring-server: export DIAGNOSTICS_PROFILING := $(DIAGNOSTICS_PROFILING)
monitoring-server: export MONITORING_ASYNC_COMMANDS := $(MONITORING_ASYNC_COMMANDS)
monitoring-server: export MONITORING_ASYNC_COMMAND_STATUS_CAPACITY := $(MONITORING_ASYNC_COMMAND_STATUS_CAPACITY)
monitoring-server: export JOURNAL_IDEMPOTENCY_CAPACITY := $(JOURNAL_IDEMPOTENCY_CAPACITY)
monitoring-server:
	MONITORING_ADDR='$(MONITORING_ADDR)' MONITORING_WEB_DIR='$(MONITORING_WEB_DIR)' MONITORING_TLS_CERT='$(MONITORING_TLS_CERT)' MONITORING_TLS_KEY='$(MONITORING_TLS_KEY)' MONITORING_AUTH_TOKEN='$(MONITORING_AUTH_TOKEN)' MONITORING_AUTH_PREVIOUS_TOKEN='$(MONITORING_AUTH_PREVIOUS_TOKEN)' MONITORING_AUTH_PREVIOUS_TOKEN_EXPIRES_AT='$(MONITORING_AUTH_PREVIOUS_TOKEN_EXPIRES_AT)' AUDIT_LOG_PATH='$(AUDIT_LOG_PATH)' WRITE_PROTECTION='$(WRITE_PROTECTION)' RATE_LIMIT='$(RATE_LIMIT)' KEY_STATS_MODE='$(KEY_STATS_MODE)' KEY_STATS_CAPACITY='$(KEY_STATS_CAPACITY)' LOCAL_PARTITIONS='$(LOCAL_PARTITIONS)' COUNTER_WRITE_STRIPES='$(COUNTER_WRITE_STRIPES)' MEMORY_COMPACTION_INTERVAL='$(MEMORY_COMPACTION_INTERVAL)' MONITORING_READ_HEADER_TIMEOUT='$(MONITORING_READ_HEADER_TIMEOUT)' MONITORING_IDLE_TIMEOUT='$(MONITORING_IDLE_TIMEOUT)' NODE_ID='$(NODE_ID)' TOPOLOGY_PATH='$(TOPOLOGY_PATH)' ELECTION_TIMEOUT='$(ELECTION_TIMEOUT)' REPLICATION='$(REPLICATION)' REPLICATION_MODE='$(REPLICATION_MODE)' REPLICATION_ASYNC='$(REPLICATION_ASYNC)' REPLICATION_QUEUE_SIZE='$(REPLICATION_QUEUE_SIZE)' REPLICATION_RETRY_INTERVAL='$(REPLICATION_RETRY_INTERVAL)' REPLICATION_MAX_ATTEMPTS='$(REPLICATION_MAX_ATTEMPTS)' REPLICATION_DEAD_LETTER_LIMIT='$(REPLICATION_DEAD_LETTER_LIMIT)' REPLICATION_OUTBOX_PATH='$(REPLICATION_OUTBOX_PATH)' REPLICATION_OUTBOX_FORMAT='$(REPLICATION_OUTBOX_FORMAT)' REPLICATION_OUTBOX_CODEC='$(REPLICATION_OUTBOX_CODEC)' REPLICATION_OUTBOX_BATCH_WINDOW='$(REPLICATION_OUTBOX_BATCH_WINDOW)' REPLICATION_CIRCUIT_BREAKER_FAILURES='$(REPLICATION_CIRCUIT_BREAKER_FAILURES)' REPLICATION_CIRCUIT_BREAKER_COOLDOWN='$(REPLICATION_CIRCUIT_BREAKER_COOLDOWN)' REPLICATION_WIRE_FORMAT='$(REPLICATION_WIRE_FORMAT)' REPLICATION_TRANSPORT='$(REPLICATION_TRANSPORT)' REPLICATION_GRPC_WINDOW='$(REPLICATION_GRPC_WINDOW)' REPLICATION_GRPC_BATCH_MAX_COMMANDS='$(REPLICATION_GRPC_BATCH_MAX_COMMANDS)' REPLICATION_GRPC_BATCH_WINDOW='$(REPLICATION_GRPC_BATCH_WINDOW)' REPLICATION_HTTP_FALLBACK='$(REPLICATION_HTTP_FALLBACK)' REPLICATION_AUTH_TOKEN='$(REPLICATION_AUTH_TOKEN)' REPLICATION_AUTH_PREVIOUS_TOKEN='$(REPLICATION_AUTH_PREVIOUS_TOKEN)' REPLICATION_AUTH_PREVIOUS_TOKEN_EXPIRES_AT='$(REPLICATION_AUTH_PREVIOUS_TOKEN_EXPIRES_AT)' REPLICATION_BATCH_MAX_BYTES='$(REPLICATION_BATCH_MAX_BYTES)' REPLICATION_MAX_IN_FLIGHT_TARGETS='$(REPLICATION_MAX_IN_FLIGHT_TARGETS)' REPLICATION_SYNC_INTERVAL='$(REPLICATION_SYNC_INTERVAL)' REPLICATION_SYNC_PREFIX='$(REPLICATION_SYNC_PREFIX)' ENFORCE_LEADER_WRITES='$(ENFORCE_LEADER_WRITES)' GRPC_ADDR='$(GRPC_ADDR)' GRPC_TLS_CERT='$(GRPC_TLS_CERT)' GRPC_TLS_KEY='$(GRPC_TLS_KEY)' GRPC_CLIENT_CA='$(GRPC_CLIENT_CA)' DB_PATH='$(DB_PATH)' DB_BACKEND='$(DB_BACKEND)' DB_FORMAT='$(DB_FORMAT)' DB_SYNC_INTERVAL='$(DB_SYNC_INTERVAL)' DB_COMPARE_BEFORE_WRITE='$(DB_COMPARE_BEFORE_WRITE)' DB_COMPACT_INTERVAL='$(DB_COMPACT_INTERVAL)' DB_COMPACT_START_KEY='$(DB_COMPACT_START_KEY)' DB_COMPACT_LIMIT_KEY='$(DB_COMPACT_LIMIT_KEY)' DB_HOT_LOAD='$(DB_HOT_LOAD)' DB_HOT_LOAD_MAX_BYTES='$(DB_HOT_LOAD_MAX_BYTES)' DB_HOT_LOAD_MAX_AGE='$(DB_HOT_LOAD_MAX_AGE)' DB_HOT_LOAD_MIN_HITS='$(DB_HOT_LOAD_MIN_HITS)' DB_MEMORY_CAP_BYTES='$(DB_MEMORY_CAP_BYTES)' DB_RSS_CAP_BYTES='$(DB_RSS_CAP_BYTES)' DB_MEMORY_EVICT_INTERVAL='$(DB_MEMORY_EVICT_INTERVAL)' DB_MEMORY_EVICT_MIN_VALUE_BYTES='$(DB_MEMORY_EVICT_MIN_VALUE_BYTES)' SNAPSHOT_PATH='$(SNAPSHOT_PATH)' SNAPSHOT_INTERVAL='$(SNAPSHOT_INTERVAL)' SNAPSHOT_FORMAT='$(SNAPSHOT_FORMAT)' JOURNAL_PATH='$(JOURNAL_PATH)' JOURNAL_FORMAT='$(JOURNAL_FORMAT)' JOURNAL_GROUP_COMMIT_WINDOW='$(JOURNAL_GROUP_COMMIT_WINDOW)' JOURNAL_GROUP_COMMIT_MAX_BATCH='$(JOURNAL_GROUP_COMMIT_MAX_BATCH)' JOURNAL_SEGMENT_MAX_BYTES='$(JOURNAL_SEGMENT_MAX_BYTES)' JOURNAL_RETAINED_SEGMENTS='$(JOURNAL_RETAINED_SEGMENTS)' JOURNAL_PULL_SOURCE='$(JOURNAL_PULL_SOURCE)' JOURNAL_PULL_STATE_PATH='$(JOURNAL_PULL_STATE_PATH)' JOURNAL_PULL_INTERVAL='$(JOURNAL_PULL_INTERVAL)' JOURNAL_PULL_TIMEOUT='$(JOURNAL_PULL_TIMEOUT)' JOURNAL_PULL_LIMIT='$(JOURNAL_PULL_LIMIT)' JOURNAL_PULL_MAX_BATCHES='$(JOURNAL_PULL_MAX_BATCHES)' JOURNAL_PULL_FULL_SYNC_FALLBACK='$(JOURNAL_PULL_FULL_SYNC_FALLBACK)' JOURNAL_PULL_CHECKPOINT_BOOTSTRAP='$(JOURNAL_PULL_CHECKPOINT_BOOTSTRAP)' JOURNAL_PULL_INCREMENTAL_RECOVERY='$(JOURNAL_PULL_INCREMENTAL_RECOVERY)' JOURNAL_PULL_WIRE_FORMAT='$(JOURNAL_PULL_WIRE_FORMAT)' ./scripts/monitoring-server.sh

frontend-install:
	./scripts/frontend.sh install

frontend-dev:
	./scripts/frontend.sh dev

frontend-check:
	./scripts/frontend.sh check

frontend-test:
	./scripts/frontend.sh test

frontend-build:
	./scripts/frontend.sh build

frontend-smoke:
	./scripts/frontend-smoke.sh

frontend-backend-smoke:
	./scripts/frontend-backend-smoke.sh
report-package-layout:
	sh ./scripts/report-package-layout.sh

audit-sql-improvements:
	sh ./scripts/audit-sql-improvements.sh

audit-query-performance-goal:
	sh ./scripts/audit-query-performance-goal.sh

audit-query-performance-details:
	sh ./scripts/audit-query-performance-goal.sh details

audit-query-performance-indexes:
	sh ./scripts/audit-query-performance-goal.sh indexes

audit-query-performance-planner:
	sh ./scripts/audit-query-performance-goal.sh planner

audit-query-performance-bitmap:
	sh ./scripts/audit-query-performance-goal.sh bitmap

audit-query-performance-covering:
	sh ./scripts/audit-query-performance-goal.sh covering

audit-query-performance-covering-inspect:
	sh ./scripts/audit-query-performance-goal.sh covering-inspect

audit-query-performance-maintenance-inspect:
	sh ./scripts/audit-query-performance-goal.sh maintenance-inspect

audit-query-performance-spill-inspect:
	sh ./scripts/audit-query-performance-goal.sh spill-inspect

audit-query-performance-spill-codec-inspect:
	sh ./scripts/audit-query-performance-goal.sh spill-codec-inspect

audit-query-performance-spill-run-inspect:
	sh ./scripts/audit-query-performance-goal.sh spill-run-inspect

audit-query-performance-bloom-inspect:
	sh ./scripts/audit-query-performance-goal.sh bloom-inspect

audit-query-performance-columnar-inspect:
	sh ./scripts/audit-query-performance-goal.sh columnar-inspect

audit-query-performance-columnar-seams:
	sh ./scripts/audit-query-performance-goal.sh columnar-seams

audit-query-performance-columnar-contracts:
	sh ./scripts/audit-query-performance-goal.sh columnar-contracts

audit-query-performance-columnar-tests:
	sh ./scripts/audit-query-performance-goal.sh columnar-tests

test-sql-columnar-scans:
	sh ./scripts/test-sql-columnar-scans.sh

test-sql-columnar-limit-pushdown:
	sh ./scripts/test-sql-columnar-limit-pushdown.sh

test-sql-columnar-topn:
	sh ./scripts/test-sql-columnar-topn.sh

format-sql-columnar-dictionary-group-segment:
	sh ./scripts/format-sql-columnar-dictionary-group-segment.sh

test-sql-columnar-dictionary-group-segment:
	sh ./scripts/test-sql-columnar-dictionary-group-segment.sh

benchmark-sql-columnar-dictionary-group-segment:
	sh ./scripts/benchmark-sql-columnar-dictionary-group-segment.sh

commit-sql-columnar-dictionary-group-segment:
	sh ./scripts/commit-sql-columnar-dictionary-group-segment.sh

format-sql-columnar-dictionary-group-order:
	sh ./scripts/format-sql-columnar-dictionary-group-order.sh

test-sql-columnar-dictionary-group-order:
	sh ./scripts/test-sql-columnar-dictionary-group-order.sh

benchmark-sql-columnar-dictionary-group-order:
	sh ./scripts/benchmark-sql-columnar-dictionary-group-order.sh

commit-sql-columnar-dictionary-group-order:
	sh ./scripts/commit-sql-columnar-dictionary-group-order.sh

test-sql-columnar-topn-layout-preference:
	sh ./scripts/test-sql-columnar-topn-layout-preference.sh

benchmark-sql-columnar-topn-layout-preference:
	sh ./scripts/benchmark-sql-columnar-topn-layout-preference.sh

format-sql-columnar-topn-layout-preference:
	sh ./scripts/format-sql-columnar-topn-layout-preference.sh

stage-sql-columnar-topn-layout-preference:
	sh ./scripts/stage-sql-columnar-topn-layout-preference.sh

commit-sql-columnar-topn-layout-preference:
	sh ./scripts/commit-sql-columnar-topn-layout-preference.sh

test-sql-columnar-sorted-projection:
	sh ./scripts/test-sql-columnar-sorted-projection.sh

benchmark-sql-columnar-sorted-projection:
	sh ./scripts/benchmark-sql-columnar-sorted-projection.sh

.PHONY: benchmark-sql-columnar-directed-composite-projection
benchmark-sql-columnar-directed-composite-projection:
	sh ./scripts/benchmark-sql-columnar-directed-composite-projection.sh

.PHONY: deliver-sql-columnar-directed-composite-projection
deliver-sql-columnar-directed-composite-projection:
	sh ./scripts/deliver-sql-columnar-directed-composite-projection.sh

format-sql-columnar-sorted-projection:
	sh ./scripts/format-sql-columnar-sorted-projection.sh

.PHONY: deliver-sql-columnar-composite-projection
deliver-sql-columnar-composite-projection:
	sh ./scripts/deliver-sql-columnar-composite-projection.sh

stage-sql-columnar-sorted-projection:
	sh ./scripts/stage-sql-columnar-sorted-projection.sh

commit-sql-columnar-sorted-projection:
	sh ./scripts/commit-sql-columnar-sorted-projection.sh

test-sql-columnar-dictionary-in:
	sh ./scripts/test-sql-columnar-dictionary-in.sh

benchmark-sql-columnar-dictionary-in:
	sh ./scripts/benchmark-sql-columnar-dictionary-in.sh

format-sql-columnar-dictionary-in:
	sh ./scripts/format-sql-columnar-dictionary-in.sh

stage-sql-columnar-dictionary-in:
	sh ./scripts/stage-sql-columnar-dictionary-in.sh

commit-sql-columnar-dictionary-in:
	sh ./scripts/commit-sql-columnar-dictionary-in.sh

test-sql-columnar-topn-dictionary-in:
	sh ./scripts/test-sql-columnar-topn-dictionary-in.sh

benchmark-sql-columnar-topn-dictionary-in:
	sh ./scripts/benchmark-sql-columnar-topn-dictionary-in.sh

format-sql-columnar-topn-dictionary-in:
	sh ./scripts/format-sql-columnar-topn-dictionary-in.sh

stage-sql-columnar-topn-dictionary-in:
	sh ./scripts/stage-sql-columnar-topn-dictionary-in.sh

commit-sql-columnar-topn-dictionary-in:
	sh ./scripts/commit-sql-columnar-topn-dictionary-in.sh

test-sql-columnar-aggregate-dictionary-in:
	sh ./scripts/test-sql-columnar-aggregate-dictionary-in.sh

benchmark-sql-columnar-aggregate-dictionary-in:
	sh ./scripts/benchmark-sql-columnar-aggregate-dictionary-in.sh

format-sql-columnar-aggregate-dictionary-in:
	sh ./scripts/format-sql-columnar-aggregate-dictionary-in.sh

stage-sql-columnar-aggregate-dictionary-in:
	sh ./scripts/stage-sql-columnar-aggregate-dictionary-in.sh

commit-sql-columnar-aggregate-dictionary-in:
	sh ./scripts/commit-sql-columnar-aggregate-dictionary-in.sh

test-sql-columnar-dictionary-group-in:
	sh ./scripts/test-sql-columnar-dictionary-group-in.sh

benchmark-sql-columnar-dictionary-group-in:
	sh ./scripts/benchmark-sql-columnar-dictionary-group-in.sh

format-sql-columnar-dictionary-group-in:
	sh ./scripts/format-sql-columnar-dictionary-group-in.sh

stage-sql-columnar-dictionary-group-in:
	sh ./scripts/stage-sql-columnar-dictionary-group-in.sh

commit-sql-columnar-dictionary-group-in:
	sh ./scripts/commit-sql-columnar-dictionary-group-in.sh

test-sql-columnar-distinct-in:
	sh ./scripts/test-sql-columnar-distinct-in.sh

benchmark-sql-columnar-distinct-in:
	sh ./scripts/benchmark-sql-columnar-distinct-in.sh

format-sql-columnar-distinct-in:
	sh ./scripts/format-sql-columnar-distinct-in.sh

stage-sql-columnar-distinct-in:
	sh ./scripts/stage-sql-columnar-distinct-in.sh

commit-sql-columnar-distinct-in:
	sh ./scripts/commit-sql-columnar-distinct-in.sh

test-sql-columnar-dictionary-in-numeric:
	sh ./scripts/test-sql-columnar-dictionary-in-numeric.sh

benchmark-sql-columnar-dictionary-in-numeric:
	sh ./scripts/benchmark-sql-columnar-dictionary-in-numeric.sh

format-sql-columnar-dictionary-in-numeric:
	sh ./scripts/format-sql-columnar-dictionary-in-numeric.sh

stage-sql-columnar-dictionary-in-numeric:
	sh ./scripts/stage-sql-columnar-dictionary-in-numeric.sh

commit-sql-columnar-dictionary-in-numeric:
	sh ./scripts/commit-sql-columnar-dictionary-in-numeric.sh

test-sql-columnar-dictionary-or:
	sh ./scripts/test-sql-columnar-dictionary-or.sh

benchmark-sql-columnar-dictionary-or:
	sh ./scripts/benchmark-sql-columnar-dictionary-or.sh

format-sql-columnar-dictionary-or:
	sh ./scripts/format-sql-columnar-dictionary-or.sh

stage-sql-columnar-dictionary-or:
	sh ./scripts/stage-sql-columnar-dictionary-or.sh

commit-sql-columnar-dictionary-or:
	sh ./scripts/commit-sql-columnar-dictionary-or.sh

test-sql-columnar-dictionary-like:
	sh ./scripts/test-sql-columnar-dictionary-like.sh

benchmark-sql-columnar-dictionary-like:
	sh ./scripts/benchmark-sql-columnar-dictionary-like.sh

format-sql-columnar-dictionary-like:
	sh ./scripts/format-sql-columnar-dictionary-like.sh

stage-sql-columnar-dictionary-like:
	sh ./scripts/stage-sql-columnar-dictionary-like.sh

commit-sql-columnar-dictionary-like:
	sh ./scripts/commit-sql-columnar-dictionary-like.sh

test-sql-columnar-dictionary-group-unordered:
	sh ./scripts/test-sql-columnar-dictionary-group-unordered.sh

benchmark-sql-columnar-dictionary-group-unordered:
	sh ./scripts/benchmark-sql-columnar-dictionary-group-unordered.sh

format-sql-columnar-dictionary-group-unordered:
	sh ./scripts/format-sql-columnar-dictionary-group-unordered.sh

stage-sql-columnar-dictionary-group-unordered:
	sh ./scripts/stage-sql-columnar-dictionary-group-unordered.sh

commit-sql-columnar-dictionary-group-unordered:
	sh ./scripts/commit-sql-columnar-dictionary-group-unordered.sh

test-sql-columnar-distinct:
	sh ./scripts/test-sql-columnar-distinct.sh

benchmark-sql-columnar-distinct:
	sh ./scripts/benchmark-sql-columnar-distinct.sh

format-sql-columnar-distinct:
	sh ./scripts/format-sql-columnar-distinct.sh

deliver-sql-columnar-distinct:
	sh ./scripts/deliver-sql-columnar-distinct.sh

benchmark-sql-columnar-topn:
	sh ./scripts/benchmark-sql-columnar-topn.sh

.PHONY: benchmark-sql-columnar-topn-pruning
benchmark-sql-columnar-topn-pruning:
	sh ./scripts/benchmark-sql-columnar-topn-pruning.sh

.PHONY: deliver-sql-columnar-topn-pruning
deliver-sql-columnar-topn-pruning:
	sh ./scripts/deliver-sql-columnar-topn-pruning.sh

format-sql-columnar-topn:
	sh ./scripts/format-sql-columnar-topn.sh

deliver-sql-columnar-topn:
	sh ./scripts/deliver-sql-columnar-topn.sh

benchmark-sql-columnar-limit-pushdown:
	sh ./scripts/benchmark-sql-columnar-limit-pushdown.sh

format-sql-columnar-limit-pushdown:
	sh ./scripts/format-sql-columnar-limit-pushdown.sh

deliver-sql-columnar-limit-pushdown:
	sh ./scripts/deliver-sql-columnar-limit-pushdown.sh

format-sql-columnar-scans:
	sh ./scripts/format-sql-columnar-scans.sh

benchmark-sql-columnar-scans:
	sh ./scripts/benchmark-sql-columnar-scans.sh

review-sql-columnar-scans:
	sh ./scripts/review-sql-columnar-scans.sh

commit-sql-columnar-scans:
	sh ./scripts/commit-sql-columnar-scans.sh

audit-persisted-compression:
	sh ./scripts/audit-query-performance-goal.sh persisted-compression

audit-persisted-compression-storage-seams:
	sh ./scripts/audit-query-performance-goal.sh persisted-compression-storage-seams

audit-partition-pruning:
	sh ./scripts/audit-query-performance-goal.sh partition-pruning

audit-hot-key-skew:
	sh ./scripts/audit-query-performance-goal.sh hot-key-skew

audit-aggregate-skew:
	sh ./scripts/audit-query-performance-goal.sh aggregate-skew

audit-time-partitioning:
	sh ./scripts/audit-query-performance-goal.sh time-partitioning

audit-mixed-workload-benchmarks:
	sh ./scripts/audit-query-performance-goal.sh mixed-workload-benchmarks

benchmark-mixed-workload:
	sh ./scripts/benchmark-mixed-workload.sh

test-mixed-workload-corpus:
	sh ./scripts/test-mixed-workload-corpus.sh

format-mixed-workload-corpus:
	sh ./scripts/format-mixed-workload-corpus.sh

run-mixed-workload-corpus:
	sh ./scripts/run-mixed-workload-corpus.sh

review-mixed-workload-corpus:
	sh ./scripts/review-mixed-workload-corpus.sh

commit-mixed-workload-corpus:
	sh ./scripts/commit-mixed-workload-corpus.sh

audit-time-series-pruning-seams:
	sh ./scripts/audit-query-performance-goal.sh time-series-pruning-seams

test-sql-time-partition-pruning:
	sh ./scripts/test-sql-time-partition-pruning.sh

format-sql-time-partition-pruning:
	sh ./scripts/format-sql-time-partition-pruning.sh

review-sql-time-partition-pruning:
	sh ./scripts/review-sql-time-partition-pruning.sh

commit-sql-time-partition-pruning:
	sh ./scripts/commit-sql-time-partition-pruning.sh

audit-time-series-bucket-contract:
	sh ./scripts/audit-query-performance-goal.sh time-series-bucket-contract

audit-aggregate-skew-seams:
	sh ./scripts/audit-query-performance-goal.sh aggregate-skew-seams

test-sql-group-skew:
	sh ./scripts/test-sql-group-skew.sh

format-sql-group-skew:
	sh ./scripts/format-sql-group-skew.sh

benchmark-sql-group-skew:
	sh ./scripts/benchmark-sql-group-skew.sh

review-sql-group-skew:
	sh ./scripts/review-sql-group-skew.sh

commit-sql-group-skew:
	sh ./scripts/commit-sql-group-skew.sh

audit-aggregate-skew-implementation:
	sh ./scripts/audit-query-performance-goal.sh aggregate-skew-implementation

audit-sql-catalog-overview:
	sh ./scripts/audit-sql-catalog-goal.sh overview

audit-sql-catalog-contracts:
	sh ./scripts/audit-sql-catalog-goal.sh contracts

test-sql-explain-format:
	sh ./scripts/test-sql-explain-format.sh

format-sql-explain-format:
	sh ./scripts/format-sql-explain-format.sh

review-sql-explain-format:
	sh ./scripts/review-sql-explain-format.sh

commit-sql-explain-format:
	sh ./scripts/commit-sql-explain-format.sh

audit-slow-query-contract:
	sh ./scripts/audit-sql-catalog-goal.sh slow-query

audit-package-layout:
	sh ./scripts/audit-sql-catalog-goal.sh packages

test-sql-slow-query-samples:
	sh ./scripts/test-sql-slow-query-samples.sh

format-sql-slow-query-samples:
	sh ./scripts/format-sql-slow-query-samples.sh

review-sql-slow-query-samples:
	sh ./scripts/review-sql-slow-query-samples.sh

commit-sql-slow-query-samples:
	sh ./scripts/commit-sql-slow-query-samples.sh

test-sql-index-hints:
	sh ./scripts/test-sql-index-hints.sh

format-sql-index-hints:
	sh ./scripts/format-sql-index-hints.sh

review-sql-index-hints:
	sh ./scripts/review-sql-index-hints.sh

commit-sql-index-hints:
	sh ./scripts/commit-sql-index-hints.sh

test-sql-null-semantics:
	sh ./scripts/test-sql-null-semantics.sh

format-sql-null-semantics:
	sh ./scripts/format-sql-null-semantics.sh

review-sql-null-semantics:
	sh ./scripts/review-sql-null-semantics.sh

commit-sql-null-semantics:
	sh ./scripts/commit-sql-null-semantics.sh

test-sql-catalog:
	sh ./scripts/test-sql-catalog.sh

format-sql-catalog:
	sh ./scripts/format-sql-catalog.sh

review-sql-catalog:
	sh ./scripts/review-sql-catalog.sh

commit-sql-catalog:
	sh ./scripts/commit-sql-catalog.sh

test-sql-session:
	sh ./scripts/test-sql-session.sh

format-sql-session:
	sh ./scripts/format-sql-session.sh

commit-sql-session:
	sh ./scripts/commit-sql-session.sh

commit-sql-views:
	sh ./scripts/commit-sql-views.sh

test-schema-materialized:
	sh ./scripts/test-schema-materialized.sh

format-schema-materialized:
	sh ./scripts/format-schema-materialized.sh

commit-schema-materialized:
	sh ./scripts/commit-schema-materialized.sh

test-hat-sql-governance:
	sh ./scripts/test-hat-sql-governance.sh

format-hat-sql-governance:
	sh ./scripts/format-hat-sql-governance.sh

review-fair-query-scheduling:
	sh ./scripts/review-fair-query-scheduling.sh

commit-fair-query-scheduling:
	sh ./scripts/commit-fair-query-scheduling.sh

test-hat-sql-client:
	sh ./scripts/test-hat-sql-client.sh

format-hat-sql-client:
	sh ./scripts/format-hat-sql-client.sh

review-sql-driver-iterator:
	sh ./scripts/review-sql-driver-iterator.sh

commit-sql-driver-iterator:
	sh ./scripts/commit-sql-driver-iterator.sh

test-namespace-lifecycle:
	sh ./scripts/test-namespace-lifecycle.sh

format-namespace-lifecycle:
	sh ./scripts/format-namespace-lifecycle.sh

review-namespace-lifecycle:
	sh ./scripts/review-namespace-lifecycle.sh

commit-namespace-lifecycle:
	sh ./scripts/commit-namespace-lifecycle.sh

test-integrity-repair:
	sh ./scripts/test-integrity-repair.sh

format-integrity-repair:
	sh ./scripts/format-integrity-repair.sh

review-integrity-repair:
	sh ./scripts/review-integrity-repair.sh

commit-integrity-repair:
	sh ./scripts/commit-integrity-repair.sh

test-external-quality:
	sh ./scripts/test-external-quality.sh

format-external-quality:
	sh ./scripts/format-external-quality.sh

review-external-quality:
	sh ./scripts/review-external-quality.sh

commit-external-quality:
	sh ./scripts/commit-external-quality.sh

test-query-template-assertions:
	sh ./scripts/test-query-template-assertions.sh

test-explain-lineage:
	sh ./scripts/test-explain-lineage.sh

format-explain-lineage:
	sh ./scripts/format-explain-lineage.sh

review-explain-lineage:
	sh ./scripts/review-explain-lineage.sh

commit-explain-lineage:
	sh ./scripts/commit-explain-lineage.sh

test-seeded-mutation-workloads:
	sh ./scripts/test-seeded-mutation-workloads.sh

format-seeded-mutation-workloads:
	sh ./scripts/format-seeded-mutation-workloads.sh

review-seeded-mutation-workloads:
	sh ./scripts/review-seeded-mutation-workloads.sh

commit-seeded-mutation-workloads:
	sh ./scripts/commit-seeded-mutation-workloads.sh

test-sql-extensions:
	sh ./scripts/test-sql-extensions.sh

format-sql-extensions:
	sh ./scripts/format-sql-extensions.sh

review-sql-extensions:
	sh ./scripts/review-sql-extensions.sh

commit-sql-extensions:
	sh ./scripts/commit-sql-extensions.sh

test-virtual-sources:
	sh ./scripts/test-virtual-sources.sh

format-virtual-sources:
	sh ./scripts/format-virtual-sources.sh

review-virtual-sources:
	sh ./scripts/review-virtual-sources.sh

commit-virtual-sources:
	sh ./scripts/commit-virtual-sources.sh

test-sql-events:
	sh ./scripts/test-sql-events.sh

format-sql-events:
	sh ./scripts/format-sql-events.sh

review-sql-events:
	sh ./scripts/review-sql-events.sh

commit-sql-events:
	sh ./scripts/commit-sql-events.sh

test-import-diff:
	sh ./scripts/test-import-diff.sh

format-import-diff:
	sh ./scripts/format-import-diff.sh

review-import-diff:
	sh ./scripts/review-import-diff.sh

commit-import-diff:
	sh ./scripts/commit-import-diff.sh

test-sql-contract-harness:
	sh ./scripts/test-sql-contract-harness.sh

format-sql-contract-harness:
	sh ./scripts/format-sql-contract-harness.sh

review-sql-contract-harness:
	sh ./scripts/review-sql-contract-harness.sh

commit-sql-contract-harness:
	sh ./scripts/commit-sql-contract-harness.sh

test-temporal-analytics:
	sh ./scripts/test-temporal-analytics.sh

format-temporal-analytics:
	sh ./scripts/format-temporal-analytics.sh

review-temporal-analytics:
	sh ./scripts/review-temporal-analytics.sh

commit-temporal-analytics:
	sh ./scripts/commit-temporal-analytics.sh

audit-sql-improvements-100:
	sh ./scripts/audit-sql-improvements-100.sh

verify-sql-improvements-100:
	sh ./scripts/verify-sql-improvements-100.sh

review-sql-improvements-100:
	sh ./scripts/review-sql-improvements-100.sh

commit-sql-improvements-100:
	sh ./scripts/commit-sql-improvements-100.sh

audit-analytics-goal:
	sh ./scripts/audit-analytics-goal.sh

inspect-temporal-analytics-goal:
	sh ./scripts/inspect-temporal-analytics-goal.sh

test-sql-geospatial:
	sh ./scripts/test-sql-geospatial.sh

test-sql-external-quality:
	sh ./scripts/test-sql-external-quality.sh

format-sql-geospatial:
	sh ./scripts/format-sql-geospatial.sh

review-sql-geospatial:
	sh ./scripts/review-sql-geospatial.sh

commit-sql-geospatial:
	sh ./scripts/commit-sql-geospatial.sh

test-sql-graph:
	sh ./scripts/test-sql-graph.sh

format-sql-graph:
	sh ./scripts/format-sql-graph.sh

review-sql-graph:
	sh ./scripts/review-sql-graph.sh

commit-sql-graph:
	sh ./scripts/commit-sql-graph.sh

test-sql-sequence:
	sh ./scripts/test-sql-sequence.sh

format-sql-sequence:
	sh ./scripts/format-sql-sequence.sh

review-sql-sequence:
	sh ./scripts/review-sql-sequence.sh

commit-sql-sequence:
	sh ./scripts/commit-sql-sequence.sh

test-sql-rollup:
	sh ./scripts/test-sql-rollup.sh

format-sql-rollup:
	sh ./scripts/format-sql-rollup.sh

review-sql-rollup:
	sh ./scripts/review-sql-rollup.sh

commit-sql-rollup:
	sh ./scripts/commit-sql-rollup.sh

test-sql-approximate-aggregates:
	sh ./scripts/test-sql-approximate-aggregates.sh

test-sql-interval-join:
	sh ./scripts/test-sql-interval-join.sh

format-sql-interval-join:
	sh ./scripts/format-sql-interval-join.sh

review-sql-interval-join:
	sh ./scripts/review-sql-interval-join.sh

commit-sql-interval-join:
	sh ./scripts/commit-sql-interval-join.sh

benchmark-sql-analytics-goal:
	sh ./scripts/benchmark-sql-analytics-goal.sh

format-sql-analytics-benchmarks:
	sh ./scripts/format-sql-analytics-benchmarks.sh

review-sql-analytics-benchmarks:
	sh ./scripts/review-sql-analytics-benchmarks.sh

commit-sql-analytics-benchmarks:
	sh ./scripts/commit-sql-analytics-benchmarks.sh

audit-execution-efficiency-goal:
	sh ./scripts/audit-execution-efficiency-goal.sh

inspect-execution-efficiency-goal:
	sh ./scripts/inspect-execution-efficiency-goal.sh

inspect-columnar-implementation:
	sh ./scripts/inspect-columnar-implementation.sh

inspect-sql-expression-model:
	sh ./scripts/inspect-sql-expression-model.sh

test-sql-columnar-scan:
	sh ./scripts/test-sql-columnar-scan.sh

benchmark-sql-columnar-scan:
	sh ./scripts/benchmark-sql-columnar-scan.sh

benchmark-sql-columnar-stream-materialization:
	sh ./scripts/benchmark-sql-columnar-stream-materialization.sh

format-sql-columnar-stream-materialization:
	sh ./scripts/format-sql-columnar-stream-materialization.sh

review-sql-columnar-stream-materialization:
	sh ./scripts/review-sql-columnar-stream-materialization.sh

commit-sql-columnar-stream-materialization:
	sh ./scripts/commit-sql-columnar-stream-materialization.sh

format-sql-columnar-numeric-filter:
	sh ./scripts/format-sql-columnar-numeric-filter.sh

review-sql-columnar-numeric-filter:
	sh ./scripts/review-sql-columnar-numeric-filter.sh

commit-sql-columnar-numeric-filter:
	sh ./scripts/commit-sql-columnar-numeric-filter.sh

format-sql-columnar-dictionary:
	sh ./scripts/format-sql-columnar-dictionary.sh

review-sql-columnar-dictionary:
	sh ./scripts/review-sql-columnar-dictionary.sh

commit-sql-columnar-dictionary:
	sh ./scripts/commit-sql-columnar-dictionary.sh

test-sql-cache-warming:
	sh ./scripts/test-sql-cache-warming.sh

format-sql-cache-warming:
	sh ./scripts/format-sql-cache-warming.sh

review-sql-cache-warming:
	sh ./scripts/review-sql-cache-warming.sh

commit-sql-cache-warming:
	sh ./scripts/commit-sql-cache-warming.sh

locate-columnar-builder:
	sh ./scripts/locate-columnar-builder.sh

inspect-columnar-builder:
	sh ./scripts/inspect-columnar-builder.sh

inspect-columnar-dispatch:
	sh ./scripts/inspect-columnar-dispatch.sh

inspect-sql-columnar-tests:
	sh ./scripts/inspect-sql-columnar-tests.sh

inspect-sql-stream-aggregates:
	sh ./scripts/inspect-sql-stream-aggregates.sh

format-sql-columnar-numeric-aggregate:
	sh ./scripts/format-sql-columnar-numeric-aggregate.sh

review-sql-columnar-numeric-aggregate:
	sh ./scripts/review-sql-columnar-numeric-aggregate.sh

commit-sql-columnar-numeric-aggregate:
	sh ./scripts/commit-sql-columnar-numeric-aggregate.sh

audit-sql-storage-allocation:
	sh ./scripts/audit-sql-storage-allocation.sh

inspect-sql-adaptive-storage:
	sh ./scripts/inspect-sql-adaptive-storage.sh

inspect-sql-execution-arena:
	sh ./scripts/inspect-sql-execution-arena.sh

verify-github-ci-disabled:
	sh ./scripts/verify-github-ci-disabled.sh

test-local-verification:
	sh ./scripts/test-local-verification.sh

commit-github-ci-disabled-policy:
	sh ./scripts/commit-github-ci-disabled-policy.sh

format-sql-columnar-single-source:
	sh ./scripts/format-sql-columnar-single-source.sh

test-sql-columnar-single-source:
	sh ./scripts/test-sql-columnar-single-source.sh

benchmark-sql-columnar-single-source:
	sh ./scripts/benchmark-sql-columnar-single-source.sh

review-sql-columnar-single-source:
	sh ./scripts/review-sql-columnar-single-source.sh

commit-sql-columnar-single-source:
	sh ./scripts/commit-sql-columnar-single-source.sh

format-sql-columnar-shared-row:
	sh ./scripts/format-sql-columnar-shared-row.sh

test-sql-columnar-shared-row:
	sh ./scripts/test-sql-columnar-shared-row.sh

review-sql-columnar-shared-row:
	sh ./scripts/review-sql-columnar-shared-row.sh

commit-sql-columnar-shared-row:
	sh ./scripts/commit-sql-columnar-shared-row.sh

format-sql-columnar-like:
	sh ./scripts/format-sql-columnar-like.sh

test-sql-columnar-like:
	sh ./scripts/test-sql-columnar-like.sh

review-sql-columnar-like:
	sh ./scripts/review-sql-columnar-like.sh

commit-sql-columnar-like:
	sh ./scripts/commit-sql-columnar-like.sh

benchmark-sql-columnar-mixed-conjunction:
	sh ./scripts/benchmark-sql-columnar-mixed-conjunction.sh

format-sql-columnar-mixed-conjunction:
	sh ./scripts/format-sql-columnar-mixed-conjunction.sh

test-sql-columnar-mixed-conjunction:
	sh ./scripts/test-sql-columnar-mixed-conjunction.sh

review-sql-columnar-mixed-conjunction:
	sh ./scripts/review-sql-columnar-mixed-conjunction.sh

commit-sql-columnar-mixed-conjunction:
	sh ./scripts/commit-sql-columnar-mixed-conjunction.sh

test-sql-execution-arena:
	sh ./scripts/test-sql-execution-arena.sh

benchmark-sql-execution-arena:
	sh ./scripts/benchmark-sql-execution-arena.sh

format-sql-execution-arena:
	sh ./scripts/format-sql-execution-arena.sh

review-sql-execution-arena:
	sh ./scripts/review-sql-execution-arena.sh

commit-sql-execution-arena:
	sh ./scripts/commit-sql-execution-arena.sh

benchmark-sql-columnar-regexp:
	sh ./scripts/benchmark-sql-columnar-regexp.sh

format-sql-columnar-regexp:
	sh ./scripts/format-sql-columnar-regexp.sh

test-sql-columnar-regexp:
	sh ./scripts/test-sql-columnar-regexp.sh

review-sql-columnar-regexp:
	sh ./scripts/review-sql-columnar-regexp.sh

commit-sql-columnar-regexp:
	sh ./scripts/commit-sql-columnar-regexp.sh





commit-disable-github-ci:
	sh ./scripts/commit-disable-github-ci.sh

commit-repair-incomplete-sql-make-targets:
	sh ./scripts/commit-repair-incomplete-sql-make-targets.sh

format-sql-columnar-numeric-aggregate-conjunction:
	sh ./scripts/format-sql-columnar-numeric-aggregate-conjunction.sh

test-sql-columnar-numeric-aggregate-conjunction:
	sh ./scripts/test-sql-columnar-numeric-aggregate-conjunction.sh

benchmark-sql-columnar-numeric-aggregate-conjunction:
	sh ./scripts/benchmark-sql-columnar-numeric-aggregate-conjunction.sh

review-sql-columnar-numeric-aggregate-conjunction:
	sh ./scripts/review-sql-columnar-numeric-aggregate-conjunction.sh

commit-sql-columnar-numeric-aggregate-conjunction:
	sh ./scripts/commit-sql-columnar-numeric-aggregate-conjunction.sh


format-sql-columnar-numeric-conjunction:
	sh ./scripts/format-sql-columnar-numeric-conjunction.sh

test-sql-columnar-numeric-conjunction:
	sh ./scripts/test-sql-columnar-numeric-conjunction.sh

benchmark-sql-columnar-numeric-conjunction:
	sh ./scripts/benchmark-sql-columnar-numeric-conjunction.sh

review-sql-columnar-numeric-conjunction:
	sh ./scripts/review-sql-columnar-numeric-conjunction.sh

commit-sql-columnar-numeric-conjunction:
	sh ./scripts/commit-sql-columnar-numeric-conjunction.sh

format-sql-adaptive-concurrency:
	sh ./scripts/format-sql-adaptive-concurrency.sh

test-sql-adaptive-concurrency:
	sh ./scripts/test-sql-adaptive-concurrency.sh

benchmark-sql-adaptive-concurrency:
	sh ./scripts/benchmark-sql-adaptive-concurrency.sh

review-sql-adaptive-concurrency:
	sh ./scripts/review-sql-adaptive-concurrency.sh

commit-sql-adaptive-concurrency:
	sh ./scripts/commit-sql-adaptive-concurrency.sh

format-sql-columnar-raw-bytes:
	sh ./scripts/format-sql-columnar-raw-bytes.sh

test-sql-columnar-raw-bytes:
	sh ./scripts/test-sql-columnar-raw-bytes.sh

benchmark-sql-columnar-raw-bytes:
	sh ./scripts/benchmark-sql-columnar-raw-bytes.sh

review-sql-columnar-raw-bytes:
	sh ./scripts/review-sql-columnar-raw-bytes.sh

commit-sql-columnar-raw-bytes:
	sh ./scripts/commit-sql-columnar-raw-bytes.sh

inspect-sql-index-keys:
	sh ./scripts/inspect-sql-index-keys.sh

test-sql-index-keys:
	sh ./scripts/test-sql-index-keys.sh

benchmark-sql-index-keys:
	sh ./scripts/benchmark-sql-index-keys.sh

format-sql-index-keys:
	sh ./scripts/format-sql-index-keys.sh

review-sql-index-keys:
	sh ./scripts/review-sql-index-keys.sh

commit-sql-index-keys:
	sh ./scripts/commit-sql-index-keys.sh


format-sql-columnar-json-benchmark:
	sh ./scripts/format-sql-columnar-json-benchmark.sh

review-sql-columnar-json-benchmark:
	sh ./scripts/review-sql-columnar-json-benchmark.sh

commit-sql-columnar-json-benchmark:
	sh ./scripts/commit-sql-columnar-json-benchmark.sh

audit-sql-parallel-contention:
	sh ./scripts/audit-sql-parallel-contention.sh

inspect-sql-spill-budget:
	sh ./scripts/inspect-sql-spill-budget.sh

test-sql-spill-parallel-merge:
	sh ./scripts/test-sql-spill-parallel-merge.sh

benchmark-sql-spill-parallel-merge:
	sh ./scripts/benchmark-sql-spill-parallel-merge.sh

test-race-sql-spill-parallel-merge:
	sh ./scripts/test-race-sql-spill-parallel-merge.sh

format-sql-spill-parallel-merge:
	sh ./scripts/format-sql-spill-parallel-merge.sh

review-sql-spill-parallel-merge:
	sh ./scripts/review-sql-spill-parallel-merge.sh

commit-sql-spill-parallel-merge:
	sh ./scripts/commit-sql-spill-parallel-merge.sh

inspect-sql-cache-locks:
	sh ./scripts/inspect-sql-cache-locks.sh

inspect-sql-result-cache:
	sh ./scripts/inspect-sql-result-cache.sh

test-sql-result-cache:
	sh ./scripts/test-sql-result-cache.sh

benchmark-sql-result-cache:
	sh ./scripts/benchmark-sql-result-cache.sh

format-sql-result-cache-clone:
	sh ./scripts/format-sql-result-cache-clone.sh

review-sql-result-cache-clone:
	sh ./scripts/review-sql-result-cache-clone.sh

commit-sql-result-cache-clone:
	sh ./scripts/commit-sql-result-cache-clone.sh

inspect-sql-prepared-cache:
	sh ./scripts/inspect-sql-prepared-cache.sh

test-sql-prepared-cache:
	sh ./scripts/test-sql-prepared-cache.sh

benchmark-sql-prepared-cache:
	sh ./scripts/benchmark-sql-prepared-cache.sh

format-sql-prepared-cache-lru:
	sh ./scripts/format-sql-prepared-cache-lru.sh

review-sql-prepared-cache-lru:
	sh ./scripts/review-sql-prepared-cache-lru.sh

commit-sql-prepared-cache-lru:
	sh ./scripts/commit-sql-prepared-cache-lru.sh

audit-extensibility-goal:
	sh ./scripts/audit-extensibility-goal.sh

format-query-template-assertions:
	sh ./scripts/format-query-template-assertions.sh

review-query-template-assertions:
	sh ./scripts/review-query-template-assertions.sh

commit-query-template-assertions:
	sh ./scripts/commit-query-template-assertions.sh



audit-time-partition-state:
	sh ./scripts/audit-query-performance-goal.sh time-partition-state

audit-query-performance-columnar-implementation-seams:
	sh ./scripts/audit-query-performance-goal.sh columnar-implementation-seams

audit-query-performance-columnar-ast:
	sh ./scripts/audit-query-performance-goal.sh columnar-ast

audit-query-performance-columnar-build-failure:
	sh ./scripts/audit-query-performance-goal.sh columnar-build-failure

audit-query-performance-columnar-benchmark-fixture:
	sh ./scripts/audit-query-performance-goal.sh columnar-benchmark-fixture

test-sql-spill-compression:
	sh ./scripts/test-sql-spill-compression.sh

test-sql-spill-bloom:
	sh ./scripts/test-sql-spill-bloom.sh

format-sql-spill-bloom:
	sh ./scripts/format-sql-spill-bloom.sh

review-sql-spill-bloom:
	sh ./scripts/review-sql-spill-bloom.sh

commit-sql-spill-bloom:
	sh ./scripts/commit-sql-spill-bloom.sh

format-sql-spill-compression:
	sh ./scripts/format-sql-spill-compression.sh

review-sql-spill-compression:
	sh ./scripts/review-sql-spill-compression.sh

commit-sql-spill-compression:
	sh ./scripts/commit-sql-spill-compression.sh

test-sql-index-maintenance:
	sh ./scripts/test-sql-index-maintenance.sh

format-sql-index-maintenance:
	sh ./scripts/format-sql-index-maintenance.sh

review-sql-index-maintenance:
	sh ./scripts/review-sql-index-maintenance.sh

commit-sql-index-maintenance:
	sh ./scripts/commit-sql-index-maintenance.sh

test-sql-covering-indexes:
	sh ./scripts/test-sql-covering-indexes.sh

format-sql-covering-indexes:
	sh ./scripts/format-sql-covering-indexes.sh

review-sql-covering-indexes:
	sh ./scripts/review-sql-covering-indexes.sh

commit-sql-covering-indexes:
	sh ./scripts/commit-sql-covering-indexes.sh

audit-sql-improvement-matrix:
	sh ./scripts/audit-sql-improvements.sh matrix

verify-sql-improvement-docs:
	sh ./scripts/verify-sql-improvement-docs.sh

review-sql-improvement-docs:
	sh ./scripts/review-sql-improvement-docs.sh

commit-sql-improvement-docs:
	sh ./scripts/commit-sql-improvement-docs.sh

review-sql-grouping-sets:
	sh ./scripts/review-sql-grouping-sets.sh

commit-sql-grouping-sets:
	sh ./scripts/commit-sql-grouping-sets.sh
.PHONY: test-auth-identity
test-auth-identity:
	sh ./scripts/test-auth-identity.sh
.PHONY: test-monitoring-identity
test-monitoring-identity:
	sh ./scripts/test-monitoring-identity.sh
.PHONY: deliver-pluggable-monitoring-identity
deliver-pluggable-monitoring-identity:
	sh ./scripts/deliver-pluggable-monitoring-identity.sh
.PHONY: test-external-ndjson
test-external-ndjson:
	sh ./scripts/test-external-ndjson.sh
.PHONY: deliver-external-ndjson
deliver-external-ndjson:
	sh ./scripts/deliver-external-ndjson.sh
.PHONY: test-pgwire-server
test-pgwire-server:
	sh ./scripts/test-pgwire-server.sh
.PHONY: test-pgwire-sql-adapter
test-pgwire-sql-adapter:
	sh ./scripts/test-pgwire-sql-adapter.sh
.PHONY: format-pgwire
format-pgwire:
	sh ./scripts/format-pgwire.sh
.PHONY: deliver-pgwire
deliver-pgwire:
	sh ./scripts/deliver-pgwire.sh
.PHONY: test-external-arrow
test-external-arrow:
	sh ./scripts/test-external-arrow.sh
.PHONY: add-arrow-dependency
add-arrow-dependency:
	sh ./scripts/add-arrow-dependency.sh
.PHONY: show-arrow-api
show-arrow-api:
	sh ./scripts/show-arrow-api.sh
.PHONY: tidy-arrow-dependency
tidy-arrow-dependency:
	sh ./scripts/tidy-arrow-dependency.sh
.PHONY: deliver-arrow-interchange
deliver-arrow-interchange:
	sh ./scripts/deliver-arrow-interchange.sh
.PHONY: audit-grafana-integration
audit-grafana-integration:
	sh ./scripts/audit-grafana-integration.sh
.PHONY: test-grafana-integration
test-grafana-integration:
	sh ./scripts/test-grafana-integration.sh
.PHONY: deliver-grafana-integration
deliver-grafana-integration:
	sh ./scripts/deliver-grafana-integration.sh
.PHONY: audit-openapi-management
audit-openapi-management:
	sh ./scripts/audit-openapi-management.sh
.PHONY: test-openapi-management
test-openapi-management:
	sh ./scripts/test-openapi-management.sh
.PHONY: deliver-openapi-management
deliver-openapi-management:
	sh ./scripts/deliver-openapi-management.sh
.PHONY: audit-notebook-integration
audit-notebook-integration:
	sh ./scripts/audit-notebook-integration.sh
.PHONY: test-notebook-integration
test-notebook-integration:
	sh ./scripts/test-notebook-integration.sh
.PHONY: deliver-notebook-integration
deliver-notebook-integration:
	sh ./scripts/deliver-notebook-integration.sh
.PHONY: deliver-pgwire-extended
deliver-pgwire-extended:
	sh ./scripts/deliver-pgwire-extended.sh
.PHONY: check-pgwire-client-tools
check-pgwire-client-tools:
	sh ./scripts/check-pgwire-client-tools.sh
.PHONY: inspect-pgwire-protocol
inspect-pgwire-protocol:
	sh ./scripts/inspect-pgwire-protocol.sh
.PHONY: format-sql-external
format-sql-external:
	sh ./scripts/format-sql-external.sh

.PHONY: deliver-sql-external-streaming
deliver-sql-external-streaming:
	sh ./scripts/deliver-sql-external-streaming.sh
.PHONY: bench-sql-external-streaming
bench-sql-external-streaming:
	sh ./scripts/bench-sql-external-streaming.sh
.PHONY: inspect-sql-indexes
inspect-sql-indexes:
	sh ./scripts/inspect-sql-indexes.sh

.PHONY: inspect-allocation-path
inspect-allocation-path:
	sh ./scripts/inspect-allocation-path.sh

.PHONY: test-memory-compaction-safety
test-memory-compaction-safety:
	sh ./scripts/test-memory-compaction-safety.sh

.PHONY: format-memory-compaction-safety
format-memory-compaction-safety:
	sh ./scripts/format-memory-compaction-safety.sh

.PHONY: inspect-memory-compaction-docs
inspect-memory-compaction-docs:
	sh ./scripts/inspect-memory-compaction-docs.sh

.PHONY: bench-memory-compaction-safety
bench-memory-compaction-safety:
	sh ./scripts/bench-memory-compaction-safety.sh

.PHONY: inspect-memory-compaction-benchmark
inspect-memory-compaction-benchmark:
	sh ./scripts/inspect-memory-compaction-benchmark.sh

.PHONY: inspect-memory-compaction-safety
inspect-memory-compaction-safety:
	sh ./scripts/inspect-memory-compaction-safety.sh

.PHONY: deliver-memory-compaction-safety
deliver-memory-compaction-safety:
	sh ./scripts/deliver-memory-compaction-safety.sh

.PHONY: inspect-sql-materialized-order
inspect-sql-materialized-order:
	sh ./scripts/inspect-sql-materialized-order.sh

.PHONY: inspect-sql-index-rebuild
inspect-sql-index-rebuild:
	sh ./scripts/inspect-sql-index-rebuild.sh

.PHONY: inspect-covering-source-benchmark
inspect-covering-source-benchmark:
	sh ./scripts/inspect-covering-source-benchmark.sh

.PHONY: inspect-next-sql-performance-opportunities
inspect-next-sql-performance-opportunities:
	sh ./scripts/inspect-next-sql-performance-opportunities.sh

.PHONY: inspect-sql-secondary-index-source
inspect-sql-secondary-index-source:
	sh ./scripts/inspect-sql-secondary-index-source.sh

.PHONY: inspect-sql-secondary-index-implementation
inspect-sql-secondary-index-implementation:
	sh ./scripts/inspect-sql-secondary-index-implementation.sh

.PHONY: inspect-sql-typed-index-benchmark
inspect-sql-typed-index-benchmark:
	sh ./scripts/inspect-sql-typed-index-benchmark.sh

.PHONY: test-sql-secondary-index-source
test-sql-secondary-index-source:
	sh ./scripts/test-sql-secondary-index-source.sh

.PHONY: bench-sql-secondary-index-source
bench-sql-secondary-index-source:
	sh ./scripts/bench-sql-secondary-index-source.sh

.PHONY: bench-sql-index-freshness-identity
bench-sql-index-freshness-identity:
	sh ./scripts/bench-sql-index-freshness-identity.sh

.PHONY: inspect-sql-direct-string-source
inspect-sql-direct-string-source:
	sh ./scripts/inspect-sql-direct-string-source.sh

.PHONY: test-sql-index-value-key
test-sql-index-value-key:
	sh ./scripts/test-sql-index-value-key.sh

.PHONY: bench-sql-index-value-key
bench-sql-index-value-key:
	sh ./scripts/bench-sql-index-value-key.sh

.PHONY: deliver-sql-index-value-key
deliver-sql-index-value-key:
	sh ./scripts/deliver-sql-index-value-key.sh

.PHONY: format-sql-index-snapshots
format-sql-index-snapshots:
	sh ./scripts/format-sql-index-snapshots.sh

.PHONY: test-sql-index-snapshots
test-sql-index-snapshots:
	sh ./scripts/test-sql-index-snapshots.sh

bench-sql-index-generation:
	sh ./scripts/bench-sql-index-generation.sh

format-sql-index-generation:
	sh ./scripts/format-sql-index-generation.sh

inspect-sql-index-generation-docs:
	sh ./scripts/inspect-sql-index-generation-docs.sh

deliver-sql-index-generation:
	sh ./scripts/deliver-sql-index-generation.sh

deliver-sql-index-generation-docs:
	sh ./scripts/deliver-sql-index-generation-docs.sh

inspect-sql-index-admission:
	sh ./scripts/inspect-sql-index-admission.sh

test-sql-index-admission:
	sh ./scripts/test-sql-index-admission.sh

bench-sql-index-admission:
	sh ./scripts/bench-sql-index-admission.sh

format-sql-index-admission:
	sh ./scripts/format-sql-index-admission.sh

deliver-sql-index-admission:
	sh ./scripts/deliver-sql-index-admission.sh

inspect-sql-bytes-source:
	sh ./scripts/inspect-sql-bytes-source.sh

test-sql-bytes-source:
	sh ./scripts/test-sql-bytes-source.sh

bench-sql-bytes-source:
	sh ./scripts/bench-sql-bytes-source.sh

format-sql-bytes-source:
	sh ./scripts/format-sql-bytes-source.sh

deliver-sql-bytes-source:
	sh ./scripts/deliver-sql-bytes-source.sh

inspect-typed-composite-planner:
	sh ./scripts/inspect-typed-composite-planner.sh

test-sql-typed-composite:
	sh ./scripts/test-sql-typed-composite.sh

.PHONY: bench-sql-index-snapshots
bench-sql-index-snapshots:
	sh ./scripts/bench-sql-index-snapshots.sh

.PHONY: deliver-sql-index-snapshots
deliver-sql-index-snapshots:
	sh ./scripts/deliver-sql-index-snapshots.sh

.PHONY: format-sql-typed-index
format-sql-typed-index:
	sh ./scripts/format-sql-typed-index.sh

.PHONY: test-sql-typed-index
test-sql-typed-index:
	sh ./scripts/test-sql-typed-index.sh

.PHONY: bench-sql-typed-index
bench-sql-typed-index:
	sh ./scripts/bench-sql-typed-index.sh

.PHONY: deliver-sql-typed-index
deliver-sql-typed-index:
	sh ./scripts/deliver-sql-typed-index.sh

.PHONY: deliver-sql-typed-index-order
deliver-sql-typed-index-order:
	sh ./scripts/deliver-sql-typed-index-order.sh

.PHONY: deliver-sql-typed-index-stream
deliver-sql-typed-index-stream:
	sh ./scripts/deliver-sql-typed-index-stream.sh

.PHONY: deliver-sql-typed-index-maintenance
deliver-sql-typed-index-maintenance:
	sh ./scripts/deliver-sql-typed-index-maintenance.sh

.PHONY: deliver-sql-typed-index-stats
deliver-sql-typed-index-stats:
	sh ./scripts/deliver-sql-typed-index-stats.sh

.PHONY: deliver-sql-typed-index-range-estimate
deliver-sql-typed-index-range-estimate:
	sh ./scripts/deliver-sql-typed-index-range-estimate.sh

.PHONY: deliver-sql-typed-index-range-stats
deliver-sql-typed-index-range-stats:
	sh ./scripts/deliver-sql-typed-index-range-stats.sh

.PHONY: deliver-sql-typed-index-value-estimate
deliver-sql-typed-index-value-estimate:
	sh ./scripts/deliver-sql-typed-index-value-estimate.sh

.PHONY: deliver-index-proposal-status
deliver-index-proposal-status:
	sh ./scripts/deliver-index-proposal-status.sh

.PHONY: test-sql-direct-string-source
test-sql-direct-string-source:
	sh ./scripts/test-sql-direct-string-source.sh

.PHONY: bench-sql-columnar-string-source
bench-sql-columnar-string-source:
	sh ./scripts/bench-sql-columnar-string-source.sh

.PHONY: deliver-sql-columnar-string-source
deliver-sql-columnar-string-source:
	sh ./scripts/deliver-sql-columnar-string-source.sh

.PHONY: deliver-sql-index-freshness-benchmark
deliver-sql-index-freshness-benchmark:
	sh ./scripts/deliver-sql-index-freshness-benchmark.sh

.PHONY: deliver-sql-secondary-index-source
deliver-sql-secondary-index-source:
	sh ./scripts/deliver-sql-secondary-index-source.sh

.PHONY: deliver-sql-performance-audit
deliver-sql-performance-audit:
	sh ./scripts/deliver-sql-performance-audit.sh

.PHONY: deliver-sql-covering-source
deliver-sql-covering-source:
	sh ./scripts/deliver-sql-covering-source.sh

.PHONY: test-sql-index-source-snapshot
test-sql-index-source-snapshot:
	sh ./scripts/test-sql-index-source-snapshot.sh

.PHONY: deliver-sql-immutable-source
deliver-sql-immutable-source:
	sh ./scripts/deliver-sql-immutable-source.sh

.PHONY: test-sql-materialized-order
test-sql-materialized-order:
	sh ./scripts/test-sql-materialized-order.sh

.PHONY: benchmark-sql-materialized-topn
benchmark-sql-materialized-topn:
	sh ./scripts/benchmark-sql-materialized-topn.sh

.PHONY: format-sql-materialized-topn
format-sql-materialized-topn:
	sh ./scripts/format-sql-materialized-topn.sh

.PHONY: deliver-sql-materialized-topn
deliver-sql-materialized-topn:
	sh ./scripts/deliver-sql-materialized-topn.sh

.PHONY: deliver-sql-materialized-order
deliver-sql-materialized-order:
	sh ./scripts/deliver-sql-materialized-order.sh

.PHONY: format-sql-materialized-order
format-sql-materialized-order:
	sh ./scripts/format-sql-materialized-order.sh

.PHONY: inspect-sql-execution-budget
inspect-sql-execution-budget:
	sh ./scripts/inspect-sql-execution-budget.sh
.PHONY: bench-sql-typed-index-baseline
bench-sql-typed-index-baseline:
	sh ./scripts/bench-sql-typed-index-baseline.sh

.PHONY: deliver-sql-typed-index-baseline
deliver-sql-typed-index-baseline:
	sh ./scripts/deliver-sql-typed-index-baseline.sh
inspect-sql-typed-composite-benchmark:
	sh ./scripts/inspect-sql-typed-composite-benchmark.sh

benchmark-sql-typed-composite:
	sh ./scripts/benchmark-sql-typed-composite.sh

inspect-sql-typed-composite-docs:
	sh ./scripts/inspect-sql-typed-composite-docs.sh

format-sql-typed-composite:
	sh ./scripts/format-sql-typed-composite.sh

status-sql-typed-composite:
	sh ./scripts/status-sql-typed-composite.sh

.PHONY: test-sql-projection-frontier
test-sql-projection-frontier:
	sh ./scripts/test-sql-projection-frontier.sh

.PHONY: format-sql-projection-frontier
format-sql-projection-frontier:
	sh ./scripts/format-sql-projection-frontier.sh

.PHONY: verify-sql-projection-frontier
verify-sql-projection-frontier:
	sh ./scripts/verify-sql-projection-frontier.sh

.PHONY: benchmark-sql-projection-frontier
benchmark-sql-projection-frontier:
	sh ./scripts/benchmark-sql-projection-frontier.sh

.PHONY: test-race-sql-projection-frontier
test-race-sql-projection-frontier:
	sh ./scripts/test-race-sql-projection-frontier.sh

.PHONY: security-sql-projection-frontier
security-sql-projection-frontier:
	sh ./scripts/security-sql-projection-frontier.sh

.PHONY: inspect-sql-projection-frontier
inspect-sql-projection-frontier:
	sh ./scripts/inspect-sql-projection-frontier.sh

.PHONY: deliver-sql-projection-frontier-commit
deliver-sql-projection-frontier-commit:
	sh ./scripts/deliver-sql-projection-frontier.sh commit

.PHONY: deliver-sql-projection-frontier-push
deliver-sql-projection-frontier-push:
	sh ./scripts/deliver-sql-projection-frontier.sh push

.PHONY: test-sql-typed-arrangements
test-sql-typed-arrangements:
	sh ./scripts/test-sql-typed-arrangements.sh

.PHONY: benchmark-sql-typed-arrangements
benchmark-sql-typed-arrangements:
	sh ./scripts/benchmark-sql-typed-arrangements.sh

.PHONY: format-sql-typed-arrangements
format-sql-typed-arrangements:
	sh ./scripts/format-sql-typed-arrangements.sh

.PHONY: verify-sql-typed-arrangements
verify-sql-typed-arrangements:
	sh ./scripts/verify-sql-typed-arrangements.sh

.PHONY: test-race-sql-typed-arrangements
test-race-sql-typed-arrangements:
	sh ./scripts/test-race-sql-typed-arrangements.sh

.PHONY: security-sql-typed-arrangements
security-sql-typed-arrangements:
	sh ./scripts/security-sql-typed-arrangements.sh

.PHONY: inspect-sql-typed-arrangements
inspect-sql-typed-arrangements:
	sh ./scripts/inspect-sql-typed-arrangements.sh

.PHONY: deliver-sql-typed-arrangements-commit
deliver-sql-typed-arrangements-commit:
	sh ./scripts/deliver-sql-typed-arrangements.sh commit

.PHONY: deliver-sql-typed-arrangements-push
deliver-sql-typed-arrangements-push:
	sh ./scripts/deliver-sql-typed-arrangements.sh push

.PHONY: test-sql-refresh-scheduler-budget
test-sql-refresh-scheduler-budget:
	sh ./scripts/test-sql-refresh-scheduler-budget.sh

.PHONY: benchmark-sql-refresh-scheduler-budget
benchmark-sql-refresh-scheduler-budget:
	sh ./scripts/benchmark-sql-refresh-scheduler-budget.sh

.PHONY: format-sql-refresh-scheduler-budget
format-sql-refresh-scheduler-budget:
	sh ./scripts/format-sql-refresh-scheduler-budget.sh

.PHONY: verify-sql-refresh-scheduler-budget
verify-sql-refresh-scheduler-budget:
	sh ./scripts/verify-sql-refresh-scheduler-budget.sh

.PHONY: test-race-sql-refresh-scheduler-budget
test-race-sql-refresh-scheduler-budget:
	sh ./scripts/test-race-sql-refresh-scheduler-budget.sh

.PHONY: security-sql-refresh-scheduler-budget
security-sql-refresh-scheduler-budget:
	sh ./scripts/security-sql-refresh-scheduler-budget.sh

.PHONY: inspect-sql-refresh-scheduler-budget
inspect-sql-refresh-scheduler-budget:
	sh ./scripts/inspect-sql-refresh-scheduler-budget.sh

.PHONY: deliver-sql-refresh-scheduler-budget-commit
deliver-sql-refresh-scheduler-budget-commit:
	sh ./scripts/deliver-sql-refresh-scheduler-budget.sh commit

.PHONY: deliver-sql-refresh-scheduler-budget-push
deliver-sql-refresh-scheduler-budget-push:
	sh ./scripts/deliver-sql-refresh-scheduler-budget.sh push

.PHONY: verify-adopted-query-engine-ideas
verify-adopted-query-engine-ideas:
	sh ./scripts/verify-adopted-query-engine-ideas.sh

.PHONY: inspect-adopted-query-engine-ideas
inspect-adopted-query-engine-ideas:
	sh ./scripts/inspect-adopted-query-engine-ideas.sh

.PHONY: deliver-adopted-query-engine-ideas-commit
deliver-adopted-query-engine-ideas-commit:
	sh ./scripts/deliver-adopted-query-engine-ideas.sh commit

.PHONY: deliver-adopted-query-engine-ideas-push
deliver-adopted-query-engine-ideas-push:
	sh ./scripts/deliver-adopted-query-engine-ideas.sh push



deliver-sql-typed-composite:
	sh ./scripts/deliver-sql-typed-composite.sh

inspect-sql-indexed-row-ownership:
	sh ./scripts/inspect-sql-indexed-row-ownership.sh

test-sql-composite-range-borrowed:
	sh ./scripts/test-sql-composite-range-borrowed.sh

deliver-sql-composite-range-borrowed:
	sh ./scripts/deliver-sql-composite-range-borrowed.sh

inspect-sql-single-source-envelopes:
	sh ./scripts/inspect-sql-single-source-envelopes.sh

test-sql-single-source-envelope:
	sh ./scripts/test-sql-single-source-envelope.sh

benchmark-sql-single-source-envelope:
	sh ./scripts/benchmark-sql-single-source-envelope.sh

inspect-sql-single-source-envelope-docs:
	sh ./scripts/inspect-sql-single-source-envelope-docs.sh

deliver-sql-single-source-envelope:
	sh ./scripts/deliver-sql-single-source-envelope.sh

audit-sql-metrics-byte-accounting:
	sh ./scripts/audit-sql-metrics-byte-accounting.sh

test-sql-metrics-byte-accounting:
	sh ./scripts/test-sql-metrics-byte-accounting.sh

benchmark-sql-metrics-byte-accounting:
	sh ./scripts/benchmark-sql-metrics-byte-accounting.sh

inspect-sql-metrics-byte-docs:
	sh ./scripts/inspect-sql-metrics-byte-docs.sh

deliver-sql-metrics-byte-accounting:
	sh ./scripts/deliver-sql-metrics-byte-accounting.sh

profile-sql-metrics-disabled-query:
	sh ./scripts/profile-sql-metrics-disabled-query.sh

test-sql-borrowed-source:
	sh ./scripts/test-sql-borrowed-source.sh

deliver-sql-borrowed-source:
	sh ./scripts/deliver-sql-borrowed-source.sh

inspect-sql-observation:
	sh ./scripts/inspect-sql-observation.sh

deliver-sql-unobserved-result-bytes:
	sh ./scripts/deliver-sql-unobserved-result-bytes.sh

inspect-sql-source-ownership:
	sh ./scripts/inspect-sql-source-ownership.sh
ifneq (,$(filter inspect-sql-expression-batch,$(MAKECMDGOALS)))
.PHONY: inspect-sql-expression-batch
inspect-sql-expression-batch:
	sh ./scripts/inspect-sql-expression-batch.sh
endif

ifneq (,$(filter test-sql-expression-batch,$(MAKECMDGOALS)))
.PHONY: test-sql-expression-batch
test-sql-expression-batch:
	sh ./scripts/test-sql-expression-batch.sh
endif

ifneq (,$(filter deliver-sql-direct-batch-leaves,$(MAKECMDGOALS)))
.PHONY: deliver-sql-direct-batch-leaves
deliver-sql-direct-batch-leaves:
	sh ./scripts/deliver-sql-direct-batch-leaves.sh
endif

ifneq (,$(filter deliver-sql-zero-copy-groups,$(MAKECMDGOALS)))
.PHONY: deliver-sql-zero-copy-groups
deliver-sql-zero-copy-groups:
	sh ./scripts/deliver-sql-zero-copy-groups.sh
endif

ifneq (,$(filter deliver-sql-simple-predicate-filter,$(MAKECMDGOALS)))
.PHONY: deliver-sql-simple-predicate-filter
deliver-sql-simple-predicate-filter:
	sh ./scripts/deliver-sql-simple-predicate-filter.sh
endif

ifneq (,$(filter benchmark-sql-query-rows-stream,$(MAKECMDGOALS)))
.PHONY: benchmark-sql-query-rows-stream
benchmark-sql-query-rows-stream:
	sh ./scripts/benchmark-sql-query-rows-stream.sh
endif

ifneq (,$(filter deliver-sql-query-rows-envelope,$(MAKECMDGOALS)))
.PHONY: deliver-sql-query-rows-envelope
deliver-sql-query-rows-envelope:
	sh ./scripts/deliver-sql-query-rows-envelope.sh
endif

ifneq (,$(filter deliver-sql-query-rows-scalar,$(MAKECMDGOALS)))
.PHONY: deliver-sql-query-rows-scalar
deliver-sql-query-rows-scalar:
	sh ./scripts/deliver-sql-query-rows-scalar.sh
endif

ifneq (,$(filter profile-sql-query-rows-stream,$(MAKECMDGOALS)))
.PHONY: profile-sql-query-rows-stream
profile-sql-query-rows-stream:
	sh ./scripts/profile-sql-query-rows-stream.sh
endif

ifneq (,$(filter deliver-sql-query-rows-observation-bytes,$(MAKECMDGOALS)))
.PHONY: deliver-sql-query-rows-observation-bytes
deliver-sql-query-rows-observation-bytes:
	sh ./scripts/deliver-sql-query-rows-observation-bytes.sh
endif
ifneq (,$(filter inspect-sql-columnar-scan,$(MAKECMDGOALS)))
.PHONY: inspect-sql-columnar-scan
inspect-sql-columnar-scan:
	sh ./scripts/inspect-sql-columnar-scan.sh
endif

ifneq (,$(filter test-sql-columnar-query-rows,$(MAKECMDGOALS)))
.PHONY: test-sql-columnar-query-rows
test-sql-columnar-query-rows:
	sh ./scripts/test-sql-columnar-query-rows.sh
endif

ifneq (,$(filter benchmark-sql-query-rows-columnar,$(MAKECMDGOALS)))
.PHONY: benchmark-sql-query-rows-columnar
benchmark-sql-query-rows-columnar:
	sh ./scripts/benchmark-sql-query-rows-columnar.sh
endif

ifneq (,$(filter profile-sql-query-rows-columnar,$(MAKECMDGOALS)))
.PHONY: profile-sql-query-rows-columnar
profile-sql-query-rows-columnar:
	sh ./scripts/profile-sql-query-rows-columnar.sh
endif

ifneq (,$(filter deliver-sql-columnar-query-rows,$(MAKECMDGOALS)))
.PHONY: deliver-sql-columnar-query-rows
deliver-sql-columnar-query-rows:
	sh ./scripts/deliver-sql-columnar-query-rows.sh
endif
ifneq (,$(filter inspect-sql-condition-cache-integration,$(MAKECMDGOALS)))
.PHONY: inspect-sql-condition-cache-integration
inspect-sql-condition-cache-integration:
	sh ./scripts/inspect-sql-condition-cache-integration.sh
endif

ifneq (,$(filter benchmark-sql-query-condition-cache,$(MAKECMDGOALS)))
.PHONY: benchmark-sql-query-condition-cache
benchmark-sql-query-condition-cache:
	sh ./scripts/benchmark-sql-query-condition-cache.sh
endif

ifneq (,$(filter test-sql-query-condition-cache,$(MAKECMDGOALS)))
.PHONY: test-sql-query-condition-cache
test-sql-query-condition-cache:
	sh ./scripts/test-sql-query-condition-cache.sh
endif

ifneq (,$(filter format-sql-query-condition-cache,$(MAKECMDGOALS)))
.PHONY: format-sql-query-condition-cache
format-sql-query-condition-cache:
	sh ./scripts/format-sql-query-condition-cache.sh
endif

ifneq (,$(filter deliver-sql-query-condition-cache,$(MAKECMDGOALS)))
.PHONY: deliver-sql-query-condition-cache
deliver-sql-query-condition-cache:
	sh ./scripts/deliver-sql-query-condition-cache.sh
endif
ifneq (,$(filter inspect-sql-columnar-projection-aggregate,$(MAKECMDGOALS)))
.PHONY: inspect-sql-columnar-projection-aggregate
inspect-sql-columnar-projection-aggregate:
	sh ./scripts/inspect-sql-columnar-projection-aggregate.sh
endif

ifneq (,$(filter test-sql-columnar-group-aggregate,$(MAKECMDGOALS)))
.PHONY: test-sql-columnar-group-aggregate
test-sql-columnar-group-aggregate:
	sh ./scripts/test-sql-columnar-group-aggregate.sh
endif

ifneq (,$(filter benchmark-sql-columnar-group-aggregate,$(MAKECMDGOALS)))
.PHONY: benchmark-sql-columnar-group-aggregate
benchmark-sql-columnar-group-aggregate:
	sh ./scripts/benchmark-sql-columnar-group-aggregate.sh
endif

ifneq (,$(filter format-sql-columnar-group-aggregate,$(MAKECMDGOALS)))
.PHONY: format-sql-columnar-group-aggregate
format-sql-columnar-group-aggregate:
	sh ./scripts/format-sql-columnar-group-aggregate.sh
endif

ifneq (,$(filter deliver-sql-columnar-group-aggregate,$(MAKECMDGOALS)))
.PHONY: deliver-sql-columnar-group-aggregate
deliver-sql-columnar-group-aggregate:
	sh ./scripts/deliver-sql-columnar-group-aggregate.sh
endif
test-sql-borrowed-columnar-source:
	sh ./scripts/test-sql-borrowed-columnar-source.sh
format-sql-borrowed-columnar-source:
	sh ./scripts/format-sql-borrowed-columnar-source.sh
benchmark-sql-borrowed-columnar-source:
	sh ./scripts/benchmark-sql-borrowed-columnar-source.sh
deliver-sql-borrowed-columnar-source:
	sh ./scripts/deliver-sql-borrowed-columnar-source.sh
test-sql-columnar-segment-skip:
	sh ./scripts/test-sql-columnar-segment-skip.sh
format-sql-columnar-segment-skip:
	sh ./scripts/format-sql-columnar-segment-skip.sh
benchmark-sql-columnar-segment-skip:
	sh ./scripts/benchmark-sql-columnar-segment-skip.sh
deliver-sql-columnar-segment-skip:
	sh ./scripts/deliver-sql-columnar-segment-skip.sh
test-sql-columnar-dictionary-segment-skip:
	sh ./scripts/test-sql-columnar-segment-skip.sh
format-sql-columnar-dictionary-segment-skip:
	sh ./scripts/format-sql-columnar-segment-skip.sh
benchmark-sql-columnar-dictionary-segment-skip:
	sh ./scripts/benchmark-sql-columnar-segment-skip.sh
deliver-sql-columnar-dictionary-segment-skip:
	sh ./scripts/deliver-sql-columnar-dictionary-segment-skip.sh
deliver-sql-columnar-dictionary-segment-benchmark-docs:
	sh ./scripts/deliver-sql-columnar-dictionary-segment-benchmark-docs.sh
test-sql-columnar-string-bloom-segment:
	sh ./scripts/test-sql-columnar-string-bloom-segment.sh
format-sql-columnar-string-bloom-segment:
	sh ./scripts/format-sql-columnar-string-bloom-segment.sh
benchmark-sql-columnar-string-bloom-segment:
	sh ./scripts/benchmark-sql-columnar-string-bloom-segment.sh
deliver-sql-columnar-string-bloom-segment:
	sh ./scripts/deliver-sql-columnar-string-bloom-segment.sh
deliver-sql-columnar-string-bloom-segment-benchmark-docs:
	sh ./scripts/deliver-sql-columnar-string-bloom-segment-benchmark-docs.sh
.PHONY: test-sql-incremental-projection
test-sql-incremental-projection:
	sh ./scripts/test-sql-incremental-projection.sh

.PHONY: format-sql-incremental-projection
format-sql-incremental-projection:
	sh ./scripts/format-sql-incremental-projection.sh

.PHONY: benchmark-sql-incremental-projection
benchmark-sql-incremental-projection:
	sh ./scripts/benchmark-sql-incremental-projection.sh

.PHONY: verify-incremental-projection-docs
verify-incremental-projection-docs:
	sh ./scripts/verify-incremental-projection-docs.sh

.PHONY: test-race-sql-incremental-projection
test-race-sql-incremental-projection:
	sh ./scripts/test-race-sql-incremental-projection.sh

.PHONY: audit-sql-incremental-projection-security
audit-sql-incremental-projection-security:
	sh ./scripts/audit-sql-incremental-projection-security.sh

.PHONY: deliver-sql-incremental-projection
deliver-sql-incremental-projection:
	sh ./scripts/deliver-sql-incremental-projection.sh
.PHONY: inspect-sql-stream-journal inspect-sql-stream-journal-impl inspect-sql-stream-table inspect-sql-stream-events inspect-sql-stream-segment inspect-sql-stream-join
inspect-sql-stream-journal:
	MODE=journal sh ./scripts/inspect-sql-stream-contracts.sh

inspect-sql-stream-journal-impl:
	MODE=journal-impl sh ./scripts/inspect-sql-stream-contracts.sh

inspect-sql-stream-table:
	MODE=table sh ./scripts/inspect-sql-stream-contracts.sh

inspect-sql-stream-events:
	MODE=events sh ./scripts/inspect-sql-stream-contracts.sh

inspect-sql-stream-segment:
	MODE=segment sh ./scripts/inspect-sql-stream-contracts.sh

inspect-sql-stream-join:
	MODE=join sh ./scripts/inspect-sql-stream-contracts.sh

.PHONY: inspect-sql-stream-journal inspect-sql-stream-journal-impl inspect-sql-stream-table inspect-sql-stream-events inspect-sql-stream-segment inspect-sql-stream-join inspect-sql-journal-projection-runner format-sql-journal-projection-runner inspect-incremental-projection-docs inspect-sql-projection-retention-delivery test-sql-projection-retention test-race-sql-projection-retention format-sql-projection-retention deliver-sql-projection-retention
inspect-sql-journal-projection-runner:
	sh ./scripts/inspect-sql-journal-projection-runner.sh

.PHONY: inspect-sql-analytics-primitives test-sql-typed-table format-sql-typed-table benchmark-sql-typed-table test-race-sql-typed-table inspect-readme-sql-docs verify-sql-typed-table-docs audit-sql-typed-table-security inspect-sql-typed-table-delivery deliver-sql-typed-table
inspect-sql-analytics-primitives:
	sh ./scripts/inspect-sql-analytics-primitives.sh

inspect-sql-columnar-string-path:
	sh ./scripts/inspect-sql-columnar-string-path.sh

test-sql-columnar-ngram:
	sh ./scripts/test-sql-columnar-ngram.sh

format-sql-columnar-ngram:
	sh ./scripts/format-sql-columnar-ngram.sh

benchmark-sql-columnar-ngram:
	sh ./scripts/benchmark-sql-columnar-ngram.sh

inspect-sql-advisor-contracts:
	sh ./scripts/inspect-sql-advisor-contracts.sh

test-sql-projection-advisor:
	sh ./scripts/test-sql-projection-advisor.sh

format-sql-projection-advisor:
	sh ./scripts/format-sql-projection-advisor.sh

benchmark-sql-projection-advisor:
	sh ./scripts/benchmark-sql-projection-advisor.sh

test-race-sql-projection-advisor:
	sh ./scripts/test-race-sql-projection-advisor.sh

verify-sql-projection-advisor-docs:
	sh ./scripts/verify-sql-projection-advisor-docs.sh

audit-sql-projection-advisor-security:
	sh ./scripts/audit-sql-projection-advisor-security.sh

.PHONY: inspect-sql-advisor-contracts test-sql-projection-advisor format-sql-projection-advisor benchmark-sql-projection-advisor test-race-sql-projection-advisor verify-sql-projection-advisor-docs audit-sql-projection-advisor-security deliver-sql-projection-advisor
deliver-sql-projection-advisor:
	sh ./scripts/deliver-sql-projection-advisor.sh

verify-sql-columnar-ngram-docs:
	sh ./scripts/verify-sql-columnar-ngram-docs.sh

test-race-sql-columnar-ngram:
	sh ./scripts/test-race-sql-columnar-ngram.sh

audit-sql-columnar-ngram-security:
	sh ./scripts/audit-sql-columnar-ngram-security.sh

.PHONY: inspect-sql-columnar-string-path test-sql-columnar-ngram format-sql-columnar-ngram benchmark-sql-columnar-ngram verify-sql-columnar-ngram-docs test-race-sql-columnar-ngram audit-sql-columnar-ngram-security deliver-sql-columnar-ngram
deliver-sql-columnar-ngram:
	sh ./scripts/deliver-sql-columnar-ngram.sh

.PHONY: audit-two-week-compatibility
audit-two-week-compatibility:
	sh ./scripts/audit-two-week-compatibility.sh

.PHONY: deliver-two-week-compatibility-audit
deliver-two-week-compatibility-audit:
	sh ./scripts/deliver-two-week-compatibility-audit.sh

test-sql-typed-table:
	sh ./scripts/test-sql-typed-table.sh

.PHONY: benchmark-sql-typed-table-adaptive-segments
benchmark-sql-typed-table-adaptive-segments:
	sh ./scripts/benchmark-sql-typed-table-adaptive-segments.sh

.PHONY: deliver-sql-typed-table-adaptive-segments
deliver-sql-typed-table-adaptive-segments:
	sh ./scripts/deliver-sql-typed-table-adaptive-segments.sh plan

.PHONY: verify-sql-typed-table-adaptive-segments-delivery
verify-sql-typed-table-adaptive-segments-delivery:
	sh ./scripts/deliver-sql-typed-table-adaptive-segments.sh verify

.PHONY: apply-sql-typed-table-adaptive-segments-delivery
apply-sql-typed-table-adaptive-segments-delivery:
	sh ./scripts/deliver-sql-typed-table-adaptive-segments.sh apply

.PHONY: unstage-sql-typed-table-adaptive-segments-delivery
unstage-sql-typed-table-adaptive-segments-delivery:
	sh ./scripts/deliver-sql-typed-table-adaptive-segments.sh unstage

.PHONY: benchmark-sql-typed-table-join
benchmark-sql-typed-table-join:
	sh ./scripts/benchmark-sql-typed-table-join.sh

.PHONY: benchmark-sql-typed-table-join-coalescing
benchmark-sql-typed-table-join-coalescing:
	sh ./scripts/benchmark-sql-typed-table-join-coalescing.sh

.PHONY: deliver-sql-typed-table-join-arrangements
deliver-sql-typed-table-join-arrangements:
	sh ./scripts/deliver-sql-typed-table-join-arrangements.sh plan

.PHONY: verify-sql-typed-table-join-arrangements-delivery
verify-sql-typed-table-join-arrangements-delivery:
	sh ./scripts/deliver-sql-typed-table-join-arrangements.sh verify

.PHONY: apply-sql-typed-table-join-arrangements-delivery
apply-sql-typed-table-join-arrangements-delivery:
	sh ./scripts/deliver-sql-typed-table-join-arrangements.sh apply

.PHONY: unstage-sql-typed-table-join-arrangements-delivery
unstage-sql-typed-table-join-arrangements-delivery:
	sh ./scripts/deliver-sql-typed-table-join-arrangements.sh unstage

format-sql-typed-table:
	sh ./scripts/format-sql-typed-table.sh

benchmark-sql-typed-table:
	sh ./scripts/benchmark-sql-typed-table.sh

test-race-sql-typed-table:
	sh ./scripts/test-race-sql-typed-table.sh

inspect-readme-sql-docs:
	sh ./scripts/inspect-readme-sql-docs.sh

verify-sql-typed-table-docs:
	sh ./scripts/verify-sql-typed-table-docs.sh

audit-sql-typed-table-security:
	sh ./scripts/audit-sql-typed-table-security.sh

inspect-sql-typed-table-delivery:
	sh ./scripts/inspect-sql-typed-table-delivery.sh

deliver-sql-typed-table:
	sh ./scripts/deliver-sql-typed-table.sh

format-sql-journal-projection-runner:
	sh ./scripts/format-sql-journal-projection-runner.sh

inspect-incremental-projection-docs:
	sh ./scripts/inspect-incremental-projection-docs.sh

inspect-sql-projection-retention-delivery:
	sh ./scripts/inspect-sql-projection-retention-delivery.sh

test-sql-projection-retention:
	sh ./scripts/test-sql-projection-retention.sh

test-race-sql-projection-retention:
	sh ./scripts/test-race-sql-projection-retention.sh

.PHONY: format-sql-projection-retention
format-sql-projection-retention:
	sh ./scripts/format-sql-projection-retention.sh

deliver-sql-projection-retention:
	sh ./scripts/deliver-sql-projection-retention.sh
.PHONY: test-sql-expression-index
test-sql-expression-index:
	sh ./scripts/test-sql-expression-index.sh
.PHONY: benchmark-sql-expression-index
benchmark-sql-expression-index:
	sh ./scripts/benchmark-sql-expression-index.sh
.PHONY: test-race-sql-expression-index
test-race-sql-expression-index:
	sh ./scripts/test-race-sql-expression-index.sh
.PHONY: verify-sql-expression-index
verify-sql-expression-index:
	sh ./scripts/verify-sql-expression-index.sh
.PHONY: deliver-sql-expression-index-plan verify-sql-expression-index-delivery deliver-sql-expression-index unstage-sql-expression-index check-sql-expression-index-stage
deliver-sql-expression-index-plan:
	sh ./scripts/deliver-sql-expression-index.sh plan
verify-sql-expression-index-delivery:
	sh ./scripts/deliver-sql-expression-index.sh verify
deliver-sql-expression-index:
	sh ./scripts/deliver-sql-expression-index.sh apply
unstage-sql-expression-index:
	sh ./scripts/deliver-sql-expression-index.sh unstage
check-sql-expression-index-stage:
	sh ./scripts/deliver-sql-expression-index.sh check-stage
.PHONY: test-race-sql-index-in
test-race-sql-index-in:
	sh ./scripts/test-race-sql-index-in.sh

.PHONY: verify-sql-index-in
verify-sql-index-in:
	sh ./scripts/verify-sql-index-in.sh
.PHONY: test-sql-index-in
test-sql-index-in:
	sh ./scripts/test-sql-index-in.sh
.PHONY: benchmark-sql-index-in
benchmark-sql-index-in:
	sh ./scripts/benchmark-sql-index-in.sh
.PHONY: deliver-sql-index-in-plan verify-sql-index-in-delivery deliver-sql-index-in check-sql-index-in-stage
deliver-sql-index-in-plan:
	sh ./scripts/deliver-sql-index-in.sh plan
verify-sql-index-in-delivery:
	sh ./scripts/deliver-sql-index-in.sh verify
deliver-sql-index-in:
	sh ./scripts/deliver-sql-index-in.sh apply
check-sql-index-in-stage:
	sh ./scripts/deliver-sql-index-in.sh check-stage
test-sql-borrowed-indexed-join:
	sh ./scripts/test-sql-borrowed-indexed-join.sh

test-sql-runtime-join-filter:
	sh ./scripts/test-sql-runtime-join-filter.sh

benchmark-sql-runtime-join-filter:
	sh ./scripts/benchmark-sql-runtime-join-filter.sh

format-sql-runtime-join-filter:
	sh ./scripts/format-sql-runtime-join-filter.sh

deliver-sql-runtime-join-filter:
	sh ./scripts/deliver-sql-runtime-join-filter.sh apply

check-sql-runtime-join-filter-stage:
	sh ./scripts/deliver-sql-runtime-join-filter.sh check

commit-sql-runtime-join-filter:
	sh ./scripts/deliver-sql-runtime-join-filter.sh commit

push-sql-runtime-join-filter:
	sh ./scripts/deliver-sql-runtime-join-filter.sh push
benchmark-sql-borrowed-indexed-join:
	sh ./scripts/benchmark-sql-borrowed-indexed-join.sh
verify-sql-borrowed-indexed-join:
	sh ./scripts/verify-sql-borrowed-indexed-join.sh

deliver-sql-borrowed-indexed-join-plan:
	sh ./scripts/deliver-sql-borrowed-indexed-join-plan.sh

deliver-sql-borrowed-indexed-join:
	sh ./scripts/deliver-sql-borrowed-indexed-join.sh apply
check-sql-borrowed-indexed-join-stage:
	sh ./scripts/deliver-sql-borrowed-indexed-join.sh check
test-sql-typed-minmax:
	sh ./scripts/test-sql-typed-minmax.sh
benchmark-sql-typed-minmax:
	sh ./scripts/benchmark-sql-typed-minmax.sh
verify-sql-typed-minmax:
	sh ./scripts/verify-sql-typed-minmax.sh

deliver-sql-typed-minmax-plan:
	sh ./scripts/deliver-sql-typed-minmax-plan.sh

deliver-sql-typed-minmax:
	sh ./scripts/deliver-sql-typed-minmax.sh apply
check-sql-typed-minmax-stage:
	sh ./scripts/deliver-sql-typed-minmax.sh check
test-sql-temporal-storage:
audit-engine-surface:
	bash scripts/audit-engine-surface.sh

audit-engine-surface-typed-table:
	DETAIL=typed-table bash scripts/audit-engine-surface.sh

audit-engine-surface-storage:
	DETAIL=storage bash scripts/audit-engine-surface.sh

audit-engine-surface-query:
	DETAIL=query bash scripts/audit-engine-surface.sh

audit-engine-surface-typed-table-symbols:
	DETAIL=typed-table-symbols bash scripts/audit-engine-surface.sh

audit-engine-surface-mvcc:
	DETAIL=mvcc bash scripts/audit-engine-surface.sh

audit-engine-surface-mvcc-docs:
	DETAIL=mvcc-docs bash scripts/audit-engine-surface.sh

audit-engine-surface-makefile:
	DETAIL=makefile bash scripts/audit-engine-surface.sh

audit-engine-surface-delivery:
	DETAIL=delivery bash scripts/audit-engine-surface.sh

test-sql-typed-table-mvcc:
	bash scripts/test-sql-typed-table-mvcc.sh

format-sql-typed-table-mvcc:
	bash scripts/format-sql-typed-table-mvcc.sh

benchmark-sql-typed-table-mvcc:
	bash scripts/benchmark-sql-typed-table-mvcc.sh
deliver-query-engine-adoption-docs:
	bash scripts/deliver-query-engine-adoption-docs.sh apply
check-query-engine-adoption-docs-stage:
	bash scripts/deliver-query-engine-adoption-docs.sh check
	sh ./scripts/test-sql-temporal-storage.sh

benchmark-sql-temporal-storage:
	sh ./scripts/benchmark-sql-temporal-storage.sh
verify-sql-temporal-storage:
	sh ./scripts/verify-sql-temporal-storage.sh

deliver-sql-temporal-storage-plan:
	sh ./scripts/deliver-sql-temporal-storage-plan.sh

deliver-sql-temporal-storage:
	sh ./scripts/deliver-sql-temporal-storage.sh apply

test-sql-typed-distinct:
	sh ./scripts/test-sql-typed-distinct.sh

benchmark-sql-typed-distinct:
	sh ./scripts/benchmark-sql-typed-distinct.sh
verify-sql-typed-distinct:
	sh ./scripts/verify-sql-typed-distinct.sh

deliver-sql-typed-distinct-plan:
	sh ./scripts/deliver-sql-typed-distinct-plan.sh

deliver-sql-typed-distinct:
	sh ./scripts/deliver-sql-typed-distinct.sh apply
check-sql-typed-distinct-stage:
	sh ./scripts/deliver-sql-typed-distinct.sh check

test-sql-typed-dictionary-storage:
	sh ./scripts/test-sql-typed-dictionary-storage.sh

benchmark-sql-typed-dictionary-storage:
	sh ./scripts/benchmark-sql-typed-dictionary-storage.sh
verify-sql-typed-dictionary-storage:
	sh ./scripts/verify-sql-typed-dictionary-storage.sh

deliver-sql-typed-dictionary-storage-plan:
	sh ./scripts/deliver-sql-typed-dictionary-storage-plan.sh

deliver-sql-typed-dictionary-storage:
	sh ./scripts/deliver-sql-typed-dictionary-storage.sh apply
check-sql-typed-dictionary-storage-stage:
	sh ./scripts/deliver-sql-typed-dictionary-storage.sh check

test-sql-partial-index:
	sh ./scripts/test-sql-partial-index.sh

benchmark-sql-partial-index:
	sh ./scripts/benchmark-sql-partial-index.sh
verify-sql-partial-index:
	sh ./scripts/verify-sql-partial-index.sh

deliver-sql-partial-index-plan:
	sh ./scripts/deliver-sql-partial-index-plan.sh

deliver-sql-partial-index:
	sh ./scripts/deliver-sql-partial-index.sh apply
check-sql-partial-index-stage:
	sh ./scripts/deliver-sql-partial-index.sh check
check-sql-temporal-storage-stage:
test-sql-typed-table-patch-parts:
	bash scripts/test-sql-typed-table-patch-parts.sh

.PHONY: format-sql-typed-table-patch-parts
format-sql-typed-table-patch-parts:
	bash scripts/format-sql-typed-table-patch-parts.sh

.PHONY: deliver-sql-typed-table-patch-parts
deliver-sql-typed-table-patch-parts:
	bash scripts/deliver-sql-typed-table-patch-parts.sh apply

.PHONY: check-sql-typed-table-patch-parts
check-sql-typed-table-patch-parts:
	bash scripts/deliver-sql-typed-table-patch-parts.sh check

.PHONY: commit-sql-typed-table-patch-parts
commit-sql-typed-table-patch-parts:
	bash scripts/deliver-sql-typed-table-patch-parts.sh commit

.PHONY: push-sql-typed-table-patch-parts
push-sql-typed-table-patch-parts:
	bash scripts/deliver-sql-typed-table-patch-parts.sh push

.PHONY: benchmark-sql-typed-table-patch-parts
benchmark-sql-typed-table-patch-parts:
	bash scripts/benchmark-sql-typed-table-patch-parts.sh

.PHONY: test-sql-json-path-skip
test-sql-json-path-skip:
	bash scripts/test-sql-json-path-skip.sh

.PHONY: benchmark-sql-json-path-skip
benchmark-sql-json-path-skip:
	bash scripts/benchmark-sql-json-path-skip.sh

.PHONY: format-sql-json-path-skip
format-sql-json-path-skip:
	bash scripts/format-sql-json-path-skip.sh

.PHONY: test-race-sql-json-path-skip
test-race-sql-json-path-skip:
	bash scripts/test-race-sql-json-path-skip.sh
	sh ./scripts/deliver-sql-temporal-storage.sh check
.PHONY: test-sql-prewhere
test-sql-prewhere:
	bash scripts/test-sql-prewhere.sh

.PHONY: test-race-sql-prewhere
test-race-sql-prewhere:
	bash scripts/test-race-sql-prewhere.sh

.PHONY: format-sql-prewhere
format-sql-prewhere:
	bash scripts/format-sql-prewhere.sh

.PHONY: benchmark-sql-prewhere
benchmark-sql-prewhere:
	bash scripts/benchmark-sql-prewhere.sh

.PHONY: check-sql-prewhere
check-sql-prewhere:
	bash scripts/check-sql-prewhere.sh

.PHONY: deliver-sql-prewhere
deliver-sql-prewhere:
	bash scripts/deliver-sql-prewhere.sh

.PHONY: commit-sql-prewhere
commit-sql-prewhere:
	bash scripts/deliver-sql-prewhere.sh commit

.PHONY: push-sql-prewhere
push-sql-prewhere:
	bash scripts/deliver-sql-prewhere.sh push

.PHONY: test-typed-table-arrangement-hydration
test-typed-table-arrangement-hydration:
	bash scripts/test-typed-table-arrangement-hydration.sh

.PHONY: format-typed-table-arrangement-hydration
format-typed-table-arrangement-hydration:
	bash scripts/format-typed-table-arrangement-hydration.sh

.PHONY: benchmark-typed-table-arrangement-hydration
benchmark-typed-table-arrangement-hydration:
	bash scripts/benchmark-typed-table-arrangement-hydration.sh

.PHONY: test-race-typed-table-arrangement-hydration
test-race-typed-table-arrangement-hydration:
	bash scripts/test-race-typed-table-arrangement-hydration.sh

.PHONY: check-typed-table-arrangement-hydration
check-typed-table-arrangement-hydration:
	bash scripts/check-typed-table-arrangement-hydration.sh

.PHONY: deliver-typed-table-arrangement-hydration
deliver-typed-table-arrangement-hydration:
	bash scripts/deliver-typed-table-arrangement-hydration.sh apply

.PHONY: commit-typed-table-arrangement-hydration
commit-typed-table-arrangement-hydration:
	bash scripts/deliver-typed-table-arrangement-hydration.sh commit

.PHONY: push-typed-table-arrangement-hydration
push-typed-table-arrangement-hydration:

test-command-idempotency:
		bash scripts/test-command-idempotency.sh

benchmark-command-idempotency:
		bash scripts/benchmark-command-idempotency.sh

inspect-command-idempotency:
		bash scripts/deliver-command-idempotency.sh status

deliver-command-idempotency:
		bash scripts/deliver-command-idempotency.sh apply

commit-command-idempotency:
		bash scripts/deliver-command-idempotency.sh commit

push-command-idempotency:
		bash scripts/deliver-command-idempotency.sh push

test-sql-projection-selection:
\tsh ./scripts/test-sql-projection-selection.sh

benchmark-sql-projection-selection:
\tsh ./scripts/benchmark-sql-projection-selection.sh

format-sql-projection-selection:
\tsh ./scripts/format-sql-projection-selection.sh

deliver-transparent-projections:
\tsh ./scripts/deliver-transparent-projections.sh apply

commit-transparent-projections:
\tsh ./scripts/deliver-transparent-projections.sh commit

push-transparent-projections:
\tsh ./scripts/deliver-transparent-projections.sh push

test-sql-subscription-frontier:
\tsh ./scripts/test-sql-subscription-frontier.sh

format-sql-subscription-frontier:
\tsh ./scripts/format-sql-subscription-frontier.sh

benchmark-sql-subscription-frontier:
\tsh ./scripts/benchmark-sql-subscription-frontier.sh

deliver-subscription-frontiers:
\tsh ./scripts/deliver-subscription-frontiers.sh apply

commit-subscription-frontiers:
\tsh ./scripts/deliver-subscription-frontiers.sh commit

push-subscription-frontiers:
\tsh ./scripts/deliver-subscription-frontiers.sh push

.PHONY: deliver-async-command
deliver-async-command:
\tsh ./scripts/deliver-async-command.sh apply

.PHONY: commit-async-command
commit-async-command:
\tsh ./scripts/deliver-async-command.sh commit

.PHONY: push-async-command
push-async-command:
\tsh ./scripts/deliver-async-command.sh push

.PHONY: test-async-command
test-async-command:
\tsh ./scripts/test-async-command.sh

.PHONY: format-async-command
format-async-command:
\tsh ./scripts/format-async-command.sh

.PHONY: benchmark-async-command
benchmark-async-command:
\tsh ./scripts/benchmark-async-command.sh

.PHONY: deliver-async-command
deliver-async-command:
\tsh ./scripts/deliver-async-command.sh apply

.PHONY: commit-async-command
commit-async-command:
\tsh ./scripts/deliver-async-command.sh commit

.PHONY: push-async-command
push-async-command:
\tsh ./scripts/deliver-async-command.sh push

.PHONY: test-async-http
test-async-http:
	sh ./scripts/test-async-http.sh

.PHONY: benchmark-async-http
benchmark-async-http:
	sh ./scripts/benchmark-async-http.sh

.PHONY: deliver-async-http
deliver-async-http:
	sh ./scripts/deliver-async-http.sh apply

.PHONY: check-async-http-stage
check-async-http-stage:
	sh ./scripts/deliver-async-http.sh check

.PHONY: commit-async-http
commit-async-http:
	sh ./scripts/deliver-async-http.sh commit

.PHONY: push-async-http
push-async-http:
	sh ./scripts/deliver-async-http.sh push


gen-root-api:
	sh ./scripts/generate-root-api.sh

format-sql-whatif:
	sh ./scripts/format-sql-whatif.sh

test-sql-whatif:
	sh ./scripts/test-sql-whatif.sh

benchmark-sql-whatif:
	sh ./scripts/benchmark-sql-whatif.sh

verify-sql-whatif:
	sh ./scripts/verify-sql-whatif.sh

deliver-sql-whatif:
	sh ./scripts/deliver-sql-whatif.sh apply

commit-sql-whatif:
	sh ./scripts/deliver-sql-whatif.sh commit

push-sql-whatif:
	sh ./scripts/deliver-sql-whatif.sh push


format-sql-keyset:
	sh ./scripts/format-sql-keyset.sh

test-sql-keyset:
	sh ./scripts/test-sql-keyset.sh

benchmark-sql-keyset:
	sh ./scripts/benchmark-sql-keyset.sh

test-sql-keyset-hattrie:
	sh ./scripts/test-sql-keyset-hattrie.sh

benchmark-sql-keyset-hattrie:
	sh ./scripts/benchmark-sql-keyset-hattrie.sh

deliver-sql-keyset:
	sh ./scripts/deliver-sql-keyset.sh apply

commit-sql-keyset:
	sh ./scripts/deliver-sql-keyset.sh commit

push-sql-keyset:
	sh ./scripts/deliver-sql-keyset.sh push


format-memory-report:
	bash scripts/format-memory-report.sh

test-memory-report:
	bash scripts/test-memory-report.sh

benchmark-memory-report:
	bash scripts/benchmark-memory-report.sh

deliver-memory-report:
	bash scripts/deliver-memory-report.sh apply

check-memory-report-stage:
	bash scripts/deliver-memory-report.sh check

commit-memory-report:
	bash scripts/deliver-memory-report.sh commit

push-memory-report:
	bash scripts/deliver-memory-report.sh push


format-sql-prefix-index:
	bash scripts/format-sql-prefix-index.sh

test-sql-prefix-index:
	bash scripts/test-sql-prefix-index.sh

benchmark-sql-prefix-index:
	bash scripts/benchmark-sql-prefix-index.sh


deliver-sql-borrowed-prefix-index:
	bash scripts/deliver-sql-borrowed-prefix-index.sh apply

check-sql-borrowed-prefix-index-stage:
	bash scripts/deliver-sql-borrowed-prefix-index.sh check

commit-sql-borrowed-prefix-index:
	bash scripts/deliver-sql-borrowed-prefix-index.sh commit

push-sql-borrowed-prefix-index:
	bash scripts/deliver-sql-borrowed-prefix-index.sh push
