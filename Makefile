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
JOURNAL_RETAINED_BYTES ?= 0
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
RESTORE_BUNDLE_RESUME ?= false
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

.PHONY: test-t147
test-t147:
	bash ./scripts/test-t147.sh

.PHONY: bench-t147
bench-t147:
	bash ./scripts/bench-t147.sh

.PHONY: review-t147
review-t147:
	bash ./scripts/review-t147.sh

.PHONY: stage-t147
stage-t147:
	bash ./scripts/stage-t147.sh

.PHONY: commit-t147
commit-t147:
	bash ./scripts/commit-t147.sh

.PHONY: push-t147
push-t147:
	bash ./scripts/push-t147.sh

.PHONY: format-t147
format-t147:
	bash ./scripts/format-t147.sh


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
	DATA_DIR='$(DATA_DIR)' RESTORE_BUNDLE_PATH='$(RESTORE_BUNDLE_PATH)' RESTORE_BUNDLE_OVERWRITE='$(RESTORE_BUNDLE_OVERWRITE)' RESTORE_BUNDLE_RESUME='$(RESTORE_BUNDLE_RESUME)' ./scripts/restore-bundle.sh

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

test-sql-vectorized:
	sh ./scripts/test-sql-vectorized.sh

test-race-sql-vectorized:
	sh ./scripts/test-race-sql-vectorized.sh

vet-sql-vectorized:
	sh ./scripts/vet-sql-vectorized.sh

format-sql-vectorized:
	sh ./scripts/format-sql-vectorized.sh

benchmark-sql-vectorized:
	sh ./scripts/benchmark-sql-vectorized.sh

benchmark-sql-vectorized-long:
	BENCHTIME=1s sh ./scripts/benchmark-sql-vectorized.sh

stage-sql-vectorized:
	sh ./scripts/commit-sql-vectorized.sh stage

commit-sql-vectorized:
	sh ./scripts/commit-sql-vectorized.sh commit

push-sql-vectorized:
	sh ./scripts/commit-sql-vectorized.sh push

verify-inspiration:
	sh ./scripts/verify-inspiration.sh

test-sql-two-level:
	sh ./scripts/test-sql-two-level.sh

benchmark-sql-two-level:
	sh ./scripts/benchmark-sql-two-level.sh

format-sql-two-level:
	sh ./scripts/format-sql-two-level.sh

test-race-sql-two-level:
	sh ./scripts/test-race-sql-two-level.sh

vet-sql-two-level:
	sh ./scripts/vet-sql-two-level.sh

benchmark-sql-two-level-before:
	sh ./scripts/benchmark-sql-two-level-before.sh

benchmark-sql-two-level-long:
	BENCHTIME=3s sh ./scripts/benchmark-sql-two-level.sh

benchmark-sql-two-level-before-long:
	BENCHTIME=3s sh ./scripts/benchmark-sql-two-level-before.sh

stage-sql-two-level:
	sh ./scripts/commit-sql-two-level.sh stage

commit-sql-two-level:
	sh ./scripts/commit-sql-two-level.sh commit

push-sql-two-level:
	sh ./scripts/commit-sql-two-level.sh push




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
test-sql-index-progress:
	sh ./scripts/test-sql-index-progress.sh
test-sql-index-checkpoint:
	sh ./scripts/test-sql-index-checkpoint.sh
format-sql-index-checkpoint:
	sh ./scripts/format-sql-index-checkpoint.sh
review-sql-index-checkpoint:
	sh ./scripts/review-sql-index-checkpoint.sh
test-race-sql-index-checkpoint:
	sh ./scripts/test-race-sql-index-checkpoint.sh
benchmark-sql-index-checkpoint:
	sh ./scripts/benchmark-sql-index-checkpoint.sh
vet-sql-index-checkpoint:
	sh ./scripts/vet-sql-index-checkpoint.sh
commit-sql-index-checkpoint:
	sh ./scripts/commit-sql-index-checkpoint.sh
push-sql-index-checkpoint:
	sh ./scripts/push-sql-index-checkpoint.sh
format-sql-index-progress:
	sh ./scripts/format-sql-index-progress.sh
benchmark-sql-index-progress:
	sh ./scripts/benchmark-sql-index-progress.sh
test-sql-index-worker:
	sh ./scripts/test-sql-index-worker.sh
format-sql-index-worker:
	sh ./scripts/format-sql-index-worker.sh
benchmark-sql-index-worker:
	sh ./scripts/benchmark-sql-index-worker.sh
test-sql-covering-advisor:
	sh ./scripts/test-sql-covering-advisor.sh
format-sql-covering-advisor:
	sh ./scripts/format-sql-covering-advisor.sh
benchmark-sql-covering-advisor:
	sh ./scripts/benchmark-sql-covering-advisor.sh

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

.PHONY: test-conditional-command
test-conditional-command:
	sh ./scripts/test-conditional-command.sh

.PHONY: benchmark-conditional-command
benchmark-conditional-command:
	sh ./scripts/benchmark-conditional-command.sh

.PHONY: format-conditional-command
format-conditional-command:
	sh ./scripts/format-conditional-command.sh

.PHONY: race-conditional-command
race-conditional-command:
	sh ./scripts/race-conditional-command.sh

.PHONY: review-conditional-command commit-conditional-command push-conditional-command
review-conditional-command:
	sh ./scripts/review-conditional-command.sh

commit-conditional-command:
	sh ./scripts/commit-conditional-command.sh

push-conditional-command:
	sh ./scripts/push-conditional-command.sh

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

test-query-fingerprint:
	bash ./scripts/test-query-fingerprint.sh

format-query-fingerprint:
	bash ./scripts/format-query-fingerprint.sh

benchmark-query-fingerprint:
	bash ./scripts/benchmark-query-fingerprint.sh

test-row-binary:
	bash ./scripts/test-row-binary.sh

format-row-binary:
	bash ./scripts/format-row-binary.sh

benchmark-row-binary:
	bash ./scripts/benchmark-row-binary.sh

test-compressed-blocks:
	bash ./scripts/test-compressed-blocks.sh

format-compressed-blocks:
	bash ./scripts/format-compressed-blocks.sh

benchmark-compressed-blocks:
	bash ./scripts/benchmark-compressed-blocks.sh

test-row-binary-dictionary:
	bash ./scripts/test-row-binary-dictionary.sh

format-row-binary-dictionary:
	bash ./scripts/format-row-binary-dictionary.sh

benchmark-row-binary-dictionary:
	bash ./scripts/benchmark-row-binary-dictionary.sh

test-row-binary-stats:
	bash ./scripts/test-row-binary-stats.sh

format-row-binary-stats:
	bash ./scripts/format-row-binary-stats.sh

benchmark-row-binary-stats:
	bash ./scripts/benchmark-row-binary-stats.sh

generate-proto:
	./scripts/generate-proto.sh

.PHONY: inspect-t042-context
inspect-t042-context:
	bash ./scripts/inspect-t042-context.sh




cli:
	./scripts/cli.sh $(ARGS)

monitoring-server: export DIAGNOSTICS_PROFILING := $(DIAGNOSTICS_PROFILING)
monitoring-server: export MONITORING_ASYNC_COMMANDS := $(MONITORING_ASYNC_COMMANDS)
monitoring-server: export MONITORING_ASYNC_COMMAND_STATUS_CAPACITY := $(MONITORING_ASYNC_COMMAND_STATUS_CAPACITY)
monitoring-server: export JOURNAL_IDEMPOTENCY_CAPACITY := $(JOURNAL_IDEMPOTENCY_CAPACITY)
monitoring-server: export JOURNAL_RETAINED_BYTES := $(JOURNAL_RETAINED_BYTES)
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

.PHONY: test-sql-range-partition-pruning
test-sql-range-partition-pruning:
	bash ./scripts/test-sql-range-partition-pruning.sh

.PHONY: benchmark-sql-range-partition-pruning
benchmark-sql-range-partition-pruning:
	bash ./scripts/benchmark-sql-range-partition-pruning.sh

.PHONY: format-sql-range-partition-pruning
format-sql-range-partition-pruning:
	bash ./scripts/format-sql-range-partition-pruning.sh

.PHONY: verify-sql-range-partition-pruning-docs
verify-sql-range-partition-pruning-docs:
	bash ./scripts/verify-sql-range-partition-pruning-docs.sh

.PHONY: test-race-sql-range-partition-pruning
test-race-sql-range-partition-pruning:
	bash ./scripts/test-race-sql-range-partition-pruning.sh

.PHONY: vet-sql-range-partition-pruning
vet-sql-range-partition-pruning:
	bash ./scripts/vet-sql-range-partition-pruning.sh

.PHONY: review-sql-range-partition-pruning
review-sql-range-partition-pruning:
	bash ./scripts/review-sql-range-partition-pruning.sh

.PHONY: commit-sql-range-partition-pruning
commit-sql-range-partition-pruning:
	bash ./scripts/commit-sql-range-partition-pruning.sh

.PHONY: push-sql-range-partition-pruning
push-sql-range-partition-pruning:
	bash ./scripts/push-sql-range-partition-pruning.sh


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

commit-sql-prepared-plan-cache:
	sh ./scripts/commit-sql-prepared-plan-cache.sh

commit-sql-prepared-invalidation:
	sh ./scripts/commit-sql-prepared-invalidation.sh

test-sql-mutation:
	sh ./scripts/test-sql-mutation.sh

format-sql-mutation:
	sh ./scripts/format-sql-mutation.sh

benchmark-sql-mutation:
	sh ./scripts/benchmark-sql-mutation.sh

commit-sql-on-conflict:
	sh ./scripts/commit-sql-on-conflict.sh

test-sql-result-cache:
	sh ./scripts/test-sql-result-cache.sh

format-sql-result-cache:
	sh ./scripts/format-sql-result-cache.sh

benchmark-sql-result-cache:
	sh ./scripts/benchmark-sql-result-cache.sh

commit-sql-result-cache:
	sh ./scripts/commit-sql-result-cache.sh

test-sql-trace-export:
	sh ./scripts/test-sql-trace-export.sh

format-sql-trace-export:
	sh ./scripts/format-sql-trace-export.sh

benchmark-sql-trace-export:
	sh ./scripts/benchmark-sql-trace-export.sh

commit-sql-trace-export:
	sh ./scripts/commit-sql-trace-export.sh

test-sql-primary-order-advisor:
	sh ./scripts/test-sql-primary-order-advisor.sh

format-sql-primary-order-advisor:
	sh ./scripts/format-sql-primary-order-advisor.sh

benchmark-sql-primary-order-advisor:
	sh ./scripts/benchmark-sql-primary-order-advisor.sh

commit-sql-primary-order-advisor:
	sh ./scripts/commit-sql-primary-order-advisor.sh

.PHONY: test-sql-sparse-primary
test-sql-sparse-primary:
	sh ./scripts/test-sql-sparse-primary.sh

.PHONY: format-sql-sparse-primary
format-sql-sparse-primary:
	sh ./scripts/format-sql-sparse-primary.sh

.PHONY: benchmark-sql-sparse-primary
benchmark-sql-sparse-primary:
	sh ./scripts/benchmark-sql-sparse-primary.sh

.PHONY: commit-sql-sparse-primary
commit-sql-sparse-primary:
	sh ./scripts/commit-sql-sparse-primary.sh

.PHONY: test-sql-predicate-order
test-sql-predicate-order:
	sh ./scripts/test-sql-predicate-order.sh

.PHONY: benchmark-sql-predicate-order
benchmark-sql-predicate-order:
	sh ./scripts/benchmark-sql-predicate-order.sh

.PHONY: format-sql-predicate-order
format-sql-predicate-order:
	sh ./scripts/format-sql-predicate-order.sh

.PHONY: commit-sql-predicate-order
commit-sql-predicate-order:
	sh ./scripts/commit-sql-predicate-order.sh
.PHONY: test-sql-explain-pipeline
test-sql-explain-pipeline:
	sh ./scripts/test-sql-explain-pipeline.sh

.PHONY: benchmark-sql-explain-pipeline
benchmark-sql-explain-pipeline:
	sh ./scripts/benchmark-sql-explain-pipeline.sh

.PHONY: format-sql-explain-pipeline
format-sql-explain-pipeline:
	sh ./scripts/format-sql-explain-pipeline.sh

.PHONY: commit-sql-explain-pipeline
commit-sql-explain-pipeline:
	sh ./scripts/commit-sql-explain-pipeline.sh
.PHONY: test-sql-logical-short-circuit
test-sql-logical-short-circuit:
	sh ./scripts/test-sql-logical-short-circuit.sh

.PHONY: benchmark-sql-logical-short-circuit
benchmark-sql-logical-short-circuit:
	sh ./scripts/benchmark-sql-logical-short-circuit.sh

.PHONY: format-sql-logical-short-circuit
format-sql-logical-short-circuit:
	sh ./scripts/format-sql-logical-short-circuit.sh

.PHONY: commit-sql-logical-short-circuit
commit-sql-logical-short-circuit:
	sh ./scripts/commit-sql-logical-short-circuit.sh
.PHONY: test-sql-cse
test-sql-cse:
	bash ./scripts/test-sql-cse.sh

.PHONY: benchmark-sql-cse
benchmark-sql-cse:
	bash ./scripts/benchmark-sql-cse.sh

.PHONY: format-sql-cse
format-sql-cse:
	bash ./scripts/format-sql-cse.sh

.PHONY: test-sql-cse-race
test-sql-cse-race:
	bash ./scripts/test-sql-cse-race.sh

.PHONY: commit-sql-cse
commit-sql-cse:
	bash ./scripts/commit-sql-cse.sh
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

audit-adoption-next-idea:
	sh ./scripts/audit-adoption-next-idea.sh

inspect-arrangement-core:
	sh ./scripts/inspect-arrangement-core.sh




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
check-sql-temporal-storage-stage:
	sh ./scripts/deliver-sql-temporal-storage.sh check

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

test-race-sql-typed-table-mvcc:
	bash scripts/test-race-sql-typed-table-mvcc.sh

deliver-sql-typed-table-mvcc:
	bash scripts/deliver-sql-typed-table-mvcc.sh apply

check-sql-typed-table-mvcc-stage:
	bash scripts/deliver-sql-typed-table-mvcc.sh check

deliver-query-engine-adoption-docs:
	bash scripts/deliver-query-engine-adoption-docs.sh apply

check-query-engine-adoption-docs-stage:
	bash scripts/deliver-query-engine-adoption-docs.sh check

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

.PHONY: deliver-sql-json-path-skip
deliver-sql-json-path-skip:
	bash scripts/deliver-sql-json-path-skip.sh apply

.PHONY: check-sql-json-path-skip
check-sql-json-path-skip:
	bash scripts/deliver-sql-json-path-skip.sh check

.PHONY: commit-sql-json-path-skip
commit-sql-json-path-skip:
	bash scripts/deliver-sql-json-path-skip.sh commit

.PHONY: push-sql-json-path-skip
push-sql-json-path-skip:
	bash scripts/deliver-sql-json-path-skip.sh push





.PHONY: benchmark-sql-typed-table-patch-parts
benchmark-sql-typed-table-patch-parts:
	bash scripts/benchmark-sql-typed-table-patch-parts.sh
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
	bash scripts/deliver-typed-table-arrangement-hydration.sh push

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

.PHONY: inspect-sql-surface
inspect-sql-surface:
	sh ./scripts/inspect-sql-surface.sh symbols

.PHONY: inspect-sql-index
inspect-sql-index:
	sh ./scripts/inspect-sql-surface.sh index

.PHONY: inspect-sql-query
inspect-sql-query:
	sh ./scripts/inspect-sql-surface.sh query

.PHONY: inspect-sql-typed
inspect-sql-typed:
	sh ./scripts/inspect-sql-surface.sh typed

.PHONY: inspect-sql-secondary
inspect-sql-secondary:
	sh ./scripts/inspect-sql-surface.sh secondary

.PHONY: inspect-sql-stats
inspect-sql-stats:
	sh ./scripts/inspect-sql-surface.sh stats

.PHONY: inspect-sql-transaction
inspect-sql-transaction:
	sh ./scripts/inspect-sql-surface.sh transaction

.PHONY: inspect-sql-result
inspect-sql-result:
	sh ./scripts/inspect-sql-surface.sh result

.PHONY: inspect-sql-subscription
inspect-sql-subscription:
	sh ./scripts/inspect-sql-surface.sh subscription

.PHONY: inspect-sql-join
inspect-sql-join:
	sh ./scripts/inspect-sql-surface.sh join

inspect-sql-join-core:
	sh ./scripts/inspect-sql-join-core.sh

.PHONY: inspect-sql-async
inspect-sql-async:
	sh ./scripts/inspect-sql-surface.sh async

.PHONY: inspect-sql-asof
inspect-sql-asof:
	sh ./scripts/inspect-sql-surface.sh asof

.PHONY: inspect-sql-projection
inspect-sql-projection:
	sh ./scripts/inspect-sql-surface.sh projection

inspect-sql-projection-core:
	sh ./scripts/inspect-sql-projection-core.sh

inspect-sql-contracts-core:
	sh ./scripts/inspect-sql-contracts-core.sh

test-sql-projection-selection:
	sh ./scripts/test-sql-projection-selection.sh

benchmark-sql-projection-selection:
	sh ./scripts/benchmark-sql-projection-selection.sh

format-sql-projection-selection:
	sh ./scripts/format-sql-projection-selection.sh

inspect-doc-tails:
	sh ./scripts/inspect-doc-tails.sh

inspect-projection-worktree:
	sh ./scripts/inspect-projection-worktree.sh

inspect-whatif-context:
	sh ./scripts/inspect-whatif-context.sh

inspect-whatif-narrow:
	sh ./scripts/inspect-whatif-narrow.sh "$(WHATIF_MODE)"

inspect-whatif-options:
	sh ./scripts/inspect-whatif-narrow.sh options

inspect-whatif-contracts:
	sh ./scripts/inspect-whatif-narrow.sh contracts

inspect-whatif-metadata:
	sh ./scripts/inspect-whatif-narrow.sh metadata

inspect-whatif-explain:
	sh ./scripts/inspect-whatif-narrow.sh explain

inspect-whatif-parser:
	sh ./scripts/inspect-whatif-narrow.sh parser

inspect-whatif-columnar:
	sh ./scripts/inspect-whatif-narrow.sh columnar

inspect-whatif-api:
	sh ./scripts/inspect-whatif-narrow.sh api

inspect-whatif-apihead:
	sh ./scripts/inspect-whatif-narrow.sh apihead

inspect-whatif-rootgen:
	sh ./scripts/inspect-whatif-narrow.sh rootgen

inspect-whatif-test:
	sh ./scripts/inspect-whatif-narrow.sh test

inspect-whatif-docs:
	sh ./scripts/inspect-whatif-narrow.sh docs

inspect-whatif-headmake:
	sh ./scripts/inspect-whatif-narrow.sh headmake

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

inspect-whatif-diff:
	sh ./scripts/inspect-whatif-diff.sh

deliver-sql-whatif:
	sh ./scripts/deliver-sql-whatif.sh apply

commit-sql-whatif:
	sh ./scripts/deliver-sql-whatif.sh commit

push-sql-whatif:
	sh ./scripts/deliver-sql-whatif.sh push

inspect-whatif-advisor:
	sh ./scripts/inspect-whatif-narrow.sh advisor

inspect-whatif-files:
	sh ./scripts/inspect-whatif-narrow.sh files

inspect-whatif-core:
	sh ./scripts/inspect-whatif-narrow.sh core

inspect-whatif-coresql:
	sh ./scripts/inspect-whatif-narrow.sh coresql

inspect-whatif-corequery:
	sh ./scripts/inspect-whatif-narrow.sh corequery

.PHONY: inspect-command-surface
inspect-command-surface:
	sh ./scripts/inspect-command-surface.sh

.PHONY: inspect-async-write-core
inspect-async-write-core:
	sh ./scripts/inspect-async-write-core.sh

.PHONY: inspect-async-http-core
inspect-async-http-core:
	sh ./scripts/inspect-async-http-core.sh

.PHONY: test-async-command
test-async-command:
	sh ./scripts/test-async-command.sh

.PHONY: format-async-command
format-async-command:
	sh ./scripts/format-async-command.sh

.PHONY: benchmark-async-command
benchmark-async-command:
	sh ./scripts/benchmark-async-command.sh

.PHONY: deliver-async-command
deliver-async-command:
	sh ./scripts/deliver-async-command.sh apply

.PHONY: commit-async-command
commit-async-command:
	sh ./scripts/deliver-async-command.sh commit

.PHONY: push-async-command
push-async-command:
	sh ./scripts/deliver-async-command.sh push

.PHONY: inspect-readme-links
inspect-readme-links:
	sh ./scripts/inspect-readme-links.sh

deliver-transparent-projections:
	sh ./scripts/deliver-transparent-projections.sh apply

commit-transparent-projections:
	sh ./scripts/deliver-transparent-projections.sh commit

push-transparent-projections:
	sh ./scripts/deliver-transparent-projections.sh push

test-sql-subscription-frontier:
	sh ./scripts/test-sql-subscription-frontier.sh

format-sql-subscription-frontier:
	sh ./scripts/format-sql-subscription-frontier.sh

benchmark-sql-subscription-frontier:
	sh ./scripts/benchmark-sql-subscription-frontier.sh

deliver-subscription-frontiers:
	sh ./scripts/deliver-subscription-frontiers.sh apply

commit-subscription-frontiers:
	sh ./scripts/deliver-subscription-frontiers.sh commit

push-subscription-frontiers:
	sh ./scripts/deliver-subscription-frontiers.sh push

.PHONY: inspect-sql-constraint
inspect-sql-constraint:
	sh ./scripts/inspect-sql-surface.sh constraint

.PHONY: inspect-command-atomic
inspect-command-atomic:
	sh ./scripts/inspect-command-surface.sh atomic

.PHONY: inspect-command-ingest
inspect-command-ingest:
	sh ./scripts/inspect-command-surface.sh ingest

.PHONY: inspect-command-replication
inspect-command-replication:
	sh ./scripts/inspect-command-surface.sh replication

.PHONY: inspect-command-types
inspect-command-types:
	sh ./scripts/inspect-command-surface.sh types

.PHONY: inspect-command-locate
inspect-command-locate:
	sh ./scripts/inspect-command-surface.sh locate

.PHONY: inspect-command-request
inspect-command-request:
	sh ./scripts/inspect-command-surface.sh request

.PHONY: inspect-command-defs
inspect-command-defs:
	sh ./scripts/inspect-command-surface.sh defs

inspect-command-request-type:
	sh ./scripts/inspect-command-request.sh

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

.PHONY: inspect-whatif-delivery
inspect-whatif-delivery:
	sh ./scripts/inspect-whatif-delivery.sh

.PHONY: verify-whatif-targets
verify-whatif-targets:
	sh ./scripts/verify-whatif-targets.sh

.PHONY: inspect-sql-whatif-source
inspect-sql-whatif-source:
	sh ./scripts/inspect-sql-whatif-source.sh

.PHONY: inspect-sql-whatif-test
inspect-sql-whatif-test:
	sh ./scripts/inspect-sql-whatif-test.sh

.PHONY: inspect-sql-whatif-stage
inspect-sql-whatif-stage:
	sh ./scripts/inspect-sql-whatif-stage.sh

.PHONY: inspect-sql-whatif-docs
inspect-sql-whatif-docs:
	sh ./scripts/inspect-sql-whatif-docs.sh

.PHONY: inspect-sql-runtime-join
inspect-sql-runtime-join:
	sh ./scripts/inspect-sql-runtime-join.sh

.PHONY: inspect-sql-bloom-api
inspect-sql-bloom-api:
	sh ./scripts/inspect-sql-bloom-api.sh

.PHONY: inspect-sql-pagination
inspect-sql-pagination:
	sh ./scripts/inspect-sql-pagination.sh

.PHONY: inspect-ordered-source
inspect-ordered-source:
	sh ./scripts/inspect-ordered-source.sh

.PHONY: inspect-keyset-index
inspect-keyset-index:
	sh ./scripts/inspect-keyset-index.sh

.PHONY: format-sql-keyset
format-sql-keyset:
	sh ./scripts/format-sql-keyset.sh

.PHONY: test-sql-keyset
test-sql-keyset:
	sh ./scripts/test-sql-keyset.sh

.PHONY: benchmark-sql-keyset
benchmark-sql-keyset:
	sh ./scripts/benchmark-sql-keyset.sh

.PHONY: test-sql-keyset-hattrie
test-sql-keyset-hattrie:
	sh ./scripts/test-sql-keyset-hattrie.sh
.PHONY: inspect-keyset-hattrie-method
inspect-keyset-hattrie-method:
	sh ./scripts/inspect-keyset-hattrie-method.sh
.PHONY: inspect-sql-keyset-hattrie-test
inspect-sql-keyset-hattrie-test:
	sh ./scripts/inspect-sql-keyset-hattrie-test.sh
.PHONY: inspect-sql-order-null-semantics
inspect-sql-order-null-semantics:
	sh ./scripts/inspect-sql-order-null-semantics.sh
.PHONY: inspect-keyset-facade
inspect-keyset-facade:
	sh ./scripts/inspect-keyset-facade.sh
.PHONY: inspect-sql-query-request-path
inspect-sql-query-request-path:
	sh ./scripts/inspect-sql-query-request-path.sh
.PHONY: inspect-sql-client-page
inspect-sql-client-page:
	sh ./scripts/inspect-sql-client-page.sh
.PHONY: benchmark-sql-keyset-hattrie
benchmark-sql-keyset-hattrie:
	sh ./scripts/benchmark-sql-keyset-hattrie.sh
.PHONY: inspect-test-trie
inspect-test-trie:
	sh ./scripts/inspect-test-trie.sh
.PHONY: inspect-keyset-worktree
inspect-keyset-worktree:
	sh ./scripts/inspect-keyset-worktree.sh

.PHONY: deliver-sql-keyset
deliver-sql-keyset:
	sh ./scripts/deliver-sql-keyset.sh apply

.PHONY: inspect-keyset-stage
inspect-keyset-stage:
	sh ./scripts/inspect-keyset-stage.sh

.PHONY: commit-sql-keyset
commit-sql-keyset:
	sh ./scripts/deliver-sql-keyset.sh commit

.PHONY: push-sql-keyset
push-sql-keyset:
	sh ./scripts/deliver-sql-keyset.sh push
inspect-memory-apis:
	bash scripts/inspect-memory-apis.sh $(MEMORY_SECTION)

inspect-memory-apis-summary:
	bash scripts/inspect-memory-apis.sh summary

inspect-memory-apis-main:
	bash scripts/inspect-memory-apis.sh main

inspect-memory-apis-monitoring:
	bash scripts/inspect-memory-apis.sh monitoring

inspect-memory-apis-cli:
	bash scripts/inspect-memory-apis.sh cli

inspect-memory-apis-routes:
	bash scripts/inspect-memory-apis.sh routes

inspect-memory-apis-model:
	bash scripts/inspect-memory-apis.sh model

inspect-memory-apis-tests:
	bash scripts/inspect-memory-apis.sh tests

inspect-memory-apis-module:
	bash scripts/inspect-memory-apis.sh module

test-memory-report:
	bash scripts/test-memory-report.sh

format-memory-report:
	bash scripts/format-memory-report.sh

inspect-memory-apis-openapi:
	bash scripts/inspect-memory-apis.sh openapi

inspect-memory-apis-api:
	bash scripts/inspect-memory-apis.sh api

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

inspect-memory-apis-docs:
	bash scripts/inspect-memory-apis.sh docs

inspect-memory-apis-docs-top:
	bash scripts/inspect-memory-apis.sh docs-top

inspect-memory-apis-docs-monitoring:
	bash scripts/inspect-memory-apis.sh docs-monitoring

inspect-memory-apis-benchmark-top:
	bash scripts/inspect-memory-apis.sh benchmark-top

inspect-memory-apis-adoption-tail:
	bash scripts/inspect-memory-apis.sh adoption-tail

inspect-memory-apis-telemetry:
	bash scripts/inspect-memory-apis.sh telemetry

inspect-memory-apis-telemetry-detail:
	bash scripts/inspect-memory-apis.sh telemetry-detail

inspect-memory-apis-workers:
	bash scripts/inspect-memory-apis.sh workers

inspect-memory-apis-adaptive:
	bash scripts/inspect-memory-apis.sh adaptive

inspect-memory-apis-adaptive-use:
	bash scripts/inspect-memory-apis.sh adaptive-use

inspect-memory-apis-adaptive-cache:
	bash scripts/inspect-memory-apis.sh adaptive-cache

inspect-memory-apis-string-index:
	bash scripts/inspect-memory-apis.sh string-index

inspect-memory-delivery:
	bash scripts/inspect-memory-delivery.sh

inspect-memory-staged:
	bash scripts/inspect-memory-delivery.sh staged

format-sql-prefix-index:
	bash scripts/format-sql-prefix-index.sh

test-sql-prefix-index:
	bash scripts/test-sql-prefix-index.sh

benchmark-sql-prefix-index:
	bash scripts/benchmark-sql-prefix-index.sh

deliver-sql-prefix-index:
	bash scripts/deliver-sql-prefix-index.sh apply

check-sql-prefix-index-stage:
	bash scripts/deliver-sql-prefix-index.sh check

commit-sql-prefix-index:
	bash scripts/deliver-sql-prefix-index.sh commit

push-sql-prefix-index:
	bash scripts/deliver-sql-prefix-index.sh push

deliver-sql-borrowed-prefix-index:
	bash scripts/deliver-sql-borrowed-prefix-index.sh apply

check-sql-borrowed-prefix-index-stage:
	bash scripts/deliver-sql-borrowed-prefix-index.sh check

commit-sql-borrowed-prefix-index:
	bash scripts/deliver-sql-borrowed-prefix-index.sh commit

push-sql-borrowed-prefix-index:
	bash scripts/deliver-sql-borrowed-prefix-index.sh push



.PHONY: inspect-sql-prefix-index-delivery-script
inspect-sql-prefix-index-delivery-script:
	sh ./scripts/inspect-sql-prefix-index-delivery-script.sh
.PHONY: audit-next-inspiration
audit-next-inspiration:
	sh ./scripts/audit-next-inspiration.sh

.PHONY: audit-inspiration-unchecked
audit-inspiration-unchecked:
	sh ./scripts/audit-inspiration-unchecked.sh




.PHONY: test-token-bloom
test-token-bloom:
	sh ./scripts/test-token-bloom.sh

.PHONY: format-token-bloom
format-token-bloom:
	sh ./scripts/format-token-bloom.sh

.PHONY: test-token-bloom-api
test-token-bloom-api:
	sh ./scripts/test-token-bloom-api.sh

.PHONY: benchmark-token-bloom
benchmark-token-bloom:
	sh ./scripts/benchmark-token-bloom.sh

.PHONY: test-race-token-bloom
test-race-token-bloom:
	sh ./scripts/test-race-token-bloom.sh

.PHONY: vet-token-bloom
vet-token-bloom:
	sh ./scripts/vet-token-bloom.sh

.PHONY: stage-token-bloom
stage-token-bloom:
	sh ./scripts/commit-token-bloom.sh stage

.PHONY: commit-token-bloom
commit-token-bloom:
	sh ./scripts/commit-token-bloom.sh commit

.PHONY: push-token-bloom
push-token-bloom:
	sh ./scripts/commit-token-bloom.sh push


.PHONY: test-sql-constant-folding
test-sql-constant-folding:
	sh ./scripts/test-sql-constant-folding.sh

.PHONY: benchmark-sql-constant-folding
benchmark-sql-constant-folding:
	sh ./scripts/benchmark-sql-constant-folding.sh

.PHONY: format-sql-constant-folding
format-sql-constant-folding:
	sh ./scripts/format-sql-constant-folding.sh

.PHONY: test-race-sql-constant-folding
test-race-sql-constant-folding:
	sh ./scripts/test-race-sql-constant-folding.sh

.PHONY: vet-sql-constant-folding
vet-sql-constant-folding:
	sh ./scripts/vet-sql-constant-folding.sh

.PHONY: stage-sql-constant-folding
stage-sql-constant-folding:
	sh ./scripts/commit-sql-constant-folding.sh stage

.PHONY: commit-sql-constant-folding
commit-sql-constant-folding:
	sh ./scripts/commit-sql-constant-folding.sh commit

.PHONY: push-sql-constant-folding
push-sql-constant-folding:
	sh ./scripts/commit-sql-constant-folding.sh push

.PHONY: test-delay-queue
test-delay-queue:
	sh ./scripts/test-delay-queue.sh

.PHONY: format-delay-queue
format-delay-queue:
	sh ./scripts/format-delay-queue.sh

.PHONY: benchmark-delay-queue
benchmark-delay-queue:
	sh ./scripts/benchmark-delay-queue.sh

.PHONY: test-race-delay-queue
test-race-delay-queue:
	sh ./scripts/test-race-delay-queue.sh

.PHONY: vet-delay-queue
vet-delay-queue:
	sh ./scripts/vet-delay-queue.sh
.PHONY: stage-delay-queue
stage-delay-queue:
	sh ./scripts/commit-delay-queue.sh stage

.PHONY: commit-delay-queue
commit-delay-queue:
	sh ./scripts/commit-delay-queue.sh commit

.PHONY: push-delay-queue
push-delay-queue:
	sh ./scripts/commit-delay-queue.sh push
.PHONY: format-cli-output
format-cli-output:
	sh ./scripts/format-cli-output.sh

.PHONY: test-cli-output
test-cli-output:
	sh ./scripts/test-cli-output.sh

.PHONY: test-race-cli-output
test-race-cli-output:
	sh ./scripts/test-race-cli-output.sh

.PHONY: vet-cli-output
vet-cli-output:
	sh ./scripts/vet-cli-output.sh

.PHONY: benchmark-cli-output
benchmark-cli-output:
	sh ./scripts/benchmark-cli-output.sh
.PHONY: stage-cli-output
stage-cli-output:
	sh ./scripts/commit-cli-output.sh stage

.PHONY: commit-cli-output
commit-cli-output:
	sh ./scripts/commit-cli-output.sh commit

.PHONY: push-cli-output
push-cli-output:
	sh ./scripts/commit-cli-output.sh push
.PHONY: test-dead-letter-queue
test-dead-letter-queue:
	sh ./scripts/test-dead-letter-queue.sh

.PHONY: stage-dead-letter-queue
stage-dead-letter-queue:
	sh ./scripts/commit-dead-letter-queue.sh stage

.PHONY: commit-dead-letter-queue
commit-dead-letter-queue:
	sh ./scripts/commit-dead-letter-queue.sh commit

.PHONY: push-dead-letter-queue
push-dead-letter-queue:
	sh ./scripts/commit-dead-letter-queue.sh push

.PHONY: format-dead-letter-queue
format-dead-letter-queue:
	sh ./scripts/format-dead-letter-queue.sh

.PHONY: test-race-dead-letter-queue
test-race-dead-letter-queue:
	sh ./scripts/test-race-dead-letter-queue.sh

.PHONY: vet-dead-letter-queue
vet-dead-letter-queue:
	sh ./scripts/vet-dead-letter-queue.sh

.PHONY: benchmark-dead-letter-queue
benchmark-dead-letter-queue:
	sh ./scripts/benchmark-dead-letter-queue.sh
.PHONY: test-journal-retention
test-journal-retention:
	bash ./scripts/test-journal-retention.sh
.PHONY: format-journal-retention
format-journal-retention:
	bash ./scripts/format-journal-retention.sh
.PHONY: benchmark-journal-retention
benchmark-journal-retention:
	bash ./scripts/benchmark-journal-retention.sh
.PHONY: test-race-journal-retention
test-race-journal-retention:
	bash ./scripts/test-race-journal-retention.sh

.PHONY: vet-journal-retention
vet-journal-retention:
	bash ./scripts/vet-journal-retention.sh
.PHONY: review-journal-retention
review-journal-retention:
	bash ./scripts/review-journal-retention.sh
.PHONY: stage-journal-retention
stage-journal-retention:
	bash ./scripts/commit-journal-retention.sh stage

.PHONY: commit-journal-retention
commit-journal-retention:
	bash ./scripts/commit-journal-retention.sh commit

.PHONY: push-journal-retention
push-journal-retention:
	bash ./scripts/commit-journal-retention.sh push
.PHONY: test-replay-progress
test-replay-progress:
	bash ./scripts/test-replay-progress.sh
.PHONY: format-replay-progress
format-replay-progress:
	bash ./scripts/format-replay-progress.sh
.PHONY: benchmark-replay-progress
benchmark-replay-progress:
	bash ./scripts/benchmark-replay-progress.sh
.PHONY: test-race-replay-progress
test-race-replay-progress:
	bash ./scripts/test-race-replay-progress.sh

.PHONY: vet-replay-progress
vet-replay-progress:
	bash ./scripts/vet-replay-progress.sh
.PHONY: review-replay-progress
review-replay-progress:
	bash ./scripts/review-replay-progress.sh

.PHONY: stage-replay-progress
stage-replay-progress:
	bash ./scripts/commit-replay-progress.sh stage

.PHONY: commit-replay-progress
commit-replay-progress:
	bash ./scripts/commit-replay-progress.sh commit

.PHONY: push-replay-progress
push-replay-progress:
	bash ./scripts/commit-replay-progress.sh push
.PHONY: test-t062
test-t062:
	bash ./scripts/test-t062.sh
.PHONY: format-t062
format-t062:
	bash ./scripts/format-t062.sh
.PHONY: test-race-t062
test-race-t062:
	bash ./scripts/test-race-t062.sh

.PHONY: vet-t062
vet-t062:
	bash ./scripts/vet-t062.sh
.PHONY: benchmark-t062
benchmark-t062:
	bash ./scripts/benchmark-t062.sh
.PHONY: stage-t062
stage-t062:
	bash ./scripts/commit-t062.sh stage

.PHONY: commit-t062
commit-t062:
	bash ./scripts/commit-t062.sh commit

.PHONY: push-t062
push-t062:
	bash ./scripts/commit-t062.sh push

.PHONY: review-t062
review-t062:
	bash ./scripts/review-t062.sh

.PHONY: test-t063
test-t063:
	bash ./scripts/test-t063.sh

.PHONY: format-t063
format-t063:
	bash ./scripts/format-t063.sh

.PHONY: test-race-t063
test-race-t063:
	bash ./scripts/test-race-t063.sh

.PHONY: vet-t063
vet-t063:
	bash ./scripts/vet-t063.sh

.PHONY: benchmark-t063
benchmark-t063:
	bash ./scripts/benchmark-t063.sh

.PHONY: review-t063
review-t063:
	bash ./scripts/review-t063.sh

.PHONY: stage-t063
stage-t063:
	bash ./scripts/commit-t063.sh stage

.PHONY: commit-t063
commit-t063:
	bash ./scripts/commit-t063.sh commit

.PHONY: push-t063
push-t063:
	bash ./scripts/commit-t063.sh push

test-t112:
	bash ./scripts/test-t112.sh

format-t112:
	bash ./scripts/format-t112.sh

benchmark-t112:
	bash ./scripts/benchmark-t112.sh

test-race-t112:
	bash ./scripts/test-race-t112.sh

vet-t112:
	bash ./scripts/vet-t112.sh

review-t112:
	bash ./scripts/review-t112.sh

stage-t112:
	bash ./scripts/commit-t112.sh stage

commit-t112:
	bash ./scripts/commit-t112.sh commit

push-t112:
	bash ./scripts/commit-t112.sh push

test-t111:
	bash ./scripts/test-t111.sh

format-t111:
	bash ./scripts/format-t111.sh

benchmark-t111:
	bash ./scripts/benchmark-t111.sh

test-race-t111:
	bash ./scripts/test-race-t111.sh

vet-t111:
	bash ./scripts/vet-t111.sh

review-t111:
	bash ./scripts/review-t111.sh

stage-t111:
	bash ./scripts/stage-t111.sh

commit-t111:
	bash ./scripts/commit-t111.sh

push-t111:
	bash ./scripts/push-t111.sh

test-t115:
	bash ./scripts/test-t115.sh
format-t115:
	bash ./scripts/format-t115.sh

test-race-t115:
	bash ./scripts/test-race-t115.sh

vet-t115:
	bash ./scripts/vet-t115.sh

review-t115:
	bash ./scripts/review-t115.sh

stage-t115:
	bash ./scripts/stage-t115.sh

commit-t115:
	bash ./scripts/commit-t115.sh

push-t115:
	bash ./scripts/push-t115.sh
test-t051:
	bash ./scripts/test-t051.sh

.PHONY: format-t051
format-t051:
	bash ./scripts/format-t051.sh

.PHONY: test-race-t051
test-race-t051:
	bash ./scripts/test-race-t051.sh

.PHONY: vet-t051
vet-t051:
	bash ./scripts/vet-t051.sh

.PHONY: review-t051
review-t051:
	bash ./scripts/review-t051.sh

.PHONY: stage-t051
stage-t051:
	bash ./scripts/stage-t051.sh

.PHONY: commit-t051
commit-t051:
	bash ./scripts/commit-t051.sh

.PHONY: push-t051
push-t051:
	bash ./scripts/push-t051.sh

.PHONY: test-t046
test-t046:
	bash ./scripts/test-t046.sh
.PHONY: format-t046
format-t046:
	bash ./scripts/format-t046.sh
.PHONY: test-race-t046
test-race-t046:
	bash ./scripts/test-race-t046.sh

.PHONY: vet-t046
vet-t046:
	bash ./scripts/vet-t046.sh
.PHONY: review-t046
review-t046:
	bash ./scripts/review-t046.sh

.PHONY: stage-t046
stage-t046:
	bash ./scripts/stage-t046.sh

.PHONY: commit-t046
commit-t046:
	bash ./scripts/commit-t046.sh

.PHONY: push-t046
push-t046:
	bash ./scripts/push-t046.sh
.PHONY: test-t154
test-t154:
	bash ./scripts/test-t154.sh
.PHONY: format-t154
format-t154:
	bash ./scripts/format-t154.sh
.PHONY: benchmark-t154
benchmark-t154:
	bash ./scripts/benchmark-t154.sh
.PHONY: test-race-t154
test-race-t154:
	bash ./scripts/test-race-t154.sh

.PHONY: vet-t154
vet-t154:
	bash ./scripts/vet-t154.sh

.PHONY: review-t154
review-t154:
	bash ./scripts/review-t154.sh

.PHONY: stage-t154
stage-t154:
	bash ./scripts/stage-t154.sh

.PHONY: commit-t154
commit-t154:
	bash ./scripts/commit-t154.sh

.PHONY: push-t154
push-t154:
	bash ./scripts/push-t154.sh

.PHONY: test-t152
test-t152:
	bash ./scripts/test-t152.sh

.PHONY: format-t152
format-t152:
	bash ./scripts/format-t152.sh

.PHONY: test-race-t152
test-race-t152:
	bash ./scripts/test-race-t152.sh

.PHONY: review-t152
review-t152:
	bash ./scripts/review-t152.sh

.PHONY: stage-t152
stage-t152:
	bash ./scripts/stage-t152.sh

.PHONY: commit-t152
commit-t152:
	bash ./scripts/commit-t152.sh

.PHONY: push-t152
push-t152:
	bash ./scripts/push-t152.sh

.PHONY: test-t155
test-t155:
	bash ./scripts/test-t155.sh

.PHONY: format-t155
format-t155:
	bash ./scripts/format-t155.sh

.PHONY: benchmark-t155
benchmark-t155:
	bash ./scripts/benchmark-t155.sh

.PHONY: test-race-t155
test-race-t155:
	bash ./scripts/test-race-t155.sh

.PHONY: vet-t155
vet-t155:
	bash ./scripts/vet-t155.sh

.PHONY: review-t155
review-t155:
	bash ./scripts/review-t155.sh

.PHONY: stage-t155
stage-t155:
	bash ./scripts/stage-t155.sh

.PHONY: commit-t155
commit-t155:
	bash ./scripts/commit-t155.sh

.PHONY: push-t155
push-t155:
	bash ./scripts/push-t155.sh

.PHONY: review-t155-checklist
review-t155-checklist:
	bash ./scripts/review-t155-checklist.sh

.PHONY: stage-t155-checklist
stage-t155-checklist:
	bash ./scripts/stage-t155-checklist.sh

.PHONY: commit-t155-checklist
commit-t155-checklist:
	bash ./scripts/commit-t155-checklist.sh

.PHONY: push-t155-checklist
push-t155-checklist:
	bash ./scripts/push-t155-checklist.sh

.PHONY: audit-inspiration-state
audit-inspiration-state:
	bash ./scripts/audit-inspiration-state.sh



.PHONY: test-t045
test-t045:
	bash ./scripts/test-t045.sh

.PHONY: format-t045
format-t045:
	bash ./scripts/format-t045.sh

.PHONY: test-race-t045
test-race-t045:
	bash ./scripts/test-race-t045.sh

.PHONY: vet-t045
vet-t045:
	bash ./scripts/vet-t045.sh

.PHONY: review-t045
review-t045:
	bash ./scripts/review-t045.sh

.PHONY: stage-t045
stage-t045:
	bash ./scripts/stage-t045.sh

.PHONY: commit-t045
commit-t045:
	bash ./scripts/commit-t045.sh

.PHONY: push-t045
push-t045:
	bash ./scripts/push-t045.sh

.PHONY: test-t041
test-t041:
	bash ./scripts/test-t041.sh

.PHONY: format-t041
format-t041:
	bash ./scripts/format-t041.sh

.PHONY: benchmark-t041
benchmark-t041:
	bash ./scripts/benchmark-t041.sh

.PHONY: test-race-t041
test-race-t041:
	bash ./scripts/test-race-t041.sh

.PHONY: vet-t041
vet-t041:
	bash ./scripts/vet-t041.sh

.PHONY: review-t041
review-t041:
	bash ./scripts/review-t041.sh

.PHONY: stage-t041
stage-t041:
	bash ./scripts/stage-t041.sh

.PHONY: commit-t041
commit-t041:
	bash ./scripts/commit-t041.sh

.PHONY: push-t041
push-t041:
	bash ./scripts/push-t041.sh

.PHONY: test-t069
test-t069:
	bash ./scripts/test-t069.sh

.PHONY: format-t069
format-t069:
	bash ./scripts/format-t069.sh

.PHONY: test-race-t069
test-race-t069:
	bash ./scripts/test-race-t069.sh

.PHONY: vet-t069
vet-t069:
	bash ./scripts/vet-t069.sh

.PHONY: review-t069
review-t069:
	bash ./scripts/review-t069.sh

.PHONY: stage-t069
stage-t069:
	bash ./scripts/stage-t069.sh

.PHONY: commit-t069
commit-t069:
	bash ./scripts/commit-t069.sh

.PHONY: push-t069
push-t069:
	bash ./scripts/push-t069.sh
.PHONY: test-sql-query-manager
test-sql-query-manager:
	bash ./scripts/test-sql-query-manager.sh
.PHONY: format-sql-query-manager
format-sql-query-manager:
	bash ./scripts/format-sql-query-manager.sh
.PHONY: benchmark-sql-query-manager
benchmark-sql-query-manager:
	bash ./scripts/benchmark-sql-query-manager.sh
format-sql-index-advisor:
	sh ./scripts/format-sql-index-advisor.sh
benchmark-sql-index-advisor:
	sh ./scripts/benchmark-sql-index-advisor.sh
test-sql-multikey:
	sh ./scripts/test-sql-multikey.sh

format-sql-multikey:
	sh ./scripts/format-sql-multikey.sh

benchmark-sql-multikey:
	sh ./scripts/benchmark-sql-multikey.sh

test-t028:
	sh ./scripts/test-t028.sh

format-t028:
	sh ./scripts/format-t028.sh

test-t029:
	sh ./scripts/test-t029.sh

format-t029:
	sh ./scripts/format-t029.sh

test-t030:
	sh ./scripts/test-t030.sh

format-t030:
	sh ./scripts/format-t030.sh

test-t031:
	sh ./scripts/test-t031.sh
.PHONY: commit-c024-audit
commit-c024-audit:
	bash ./scripts/commit-c024-audit.sh
.PHONY: test-c036-durable-mutation-queue
test-c036-durable-mutation-queue:
	bash ./scripts/test-c036-durable-mutation-queue.sh
.PHONY: commit-c036-audit
commit-c036-audit:
	bash ./scripts/commit-c036-audit.sh
.PHONY: test-sql-query-trace-spans
test-sql-query-trace-spans:
	bash ./scripts/test-sql-query-trace-spans.sh
.PHONY: format-sql-query-trace-spans
format-sql-query-trace-spans:
	bash ./scripts/format-sql-query-trace-spans.sh
.PHONY: benchmark-sql-query-trace-spans
benchmark-sql-query-trace-spans:
	bash ./scripts/benchmark-sql-query-trace-spans.sh
.PHONY: test-race-sql-query-trace-spans
test-race-sql-query-trace-spans:
	bash ./scripts/test-race-sql-query-trace-spans.sh
.PHONY: verify-sql-query-trace-spans
verify-sql-query-trace-spans:
	bash ./scripts/verify-sql-query-trace-spans.sh
.PHONY: commit-sql-query-trace-spans
commit-sql-query-trace-spans:
	bash ./scripts/commit-sql-query-trace-spans.sh

.PHONY: test-typed-table-aggregate-key
test-typed-table-aggregate-key:
	bash scripts/test-typed-table-aggregate-key.sh

.PHONY: format-typed-table-aggregate-key
format-typed-table-aggregate-key:
	bash scripts/format-typed-table-aggregate-key.sh

.PHONY: verify-typed-table-aggregate-key
verify-typed-table-aggregate-key:
	bash scripts/verify-typed-table-aggregate-key.sh

.PHONY: test-race-typed-table-aggregate-key
test-race-typed-table-aggregate-key:
	bash scripts/test-race-typed-table-aggregate-key.sh

.PHONY: commit-typed-table-aggregate-key
commit-typed-table-aggregate-key:
	bash scripts/deliver-typed-table-aggregate-key.sh commit

.PHONY: push-typed-table-aggregate-key
push-typed-table-aggregate-key:
	bash scripts/deliver-typed-table-aggregate-key.sh push

.PHONY: test-journal-replay-fastpath
test-journal-replay-fastpath:
	sh ./scripts/test-journal-replay-fastpath.sh test

.PHONY: format-journal-replay-fastpath
format-journal-replay-fastpath:
	sh ./scripts/test-journal-replay-fastpath.sh format

.PHONY: race-journal-replay-fastpath
race-journal-replay-fastpath:
	sh ./scripts/test-journal-replay-fastpath.sh race

.PHONY: benchmark-journal-replay-fastpath
benchmark-journal-replay-fastpath:
	sh ./scripts/test-journal-replay-fastpath.sh bench

.PHONY: test-runtime-introspection
test-runtime-introspection:
	sh ./scripts/test-runtime-introspection.sh test

.PHONY: http-runtime-introspection
http-runtime-introspection:
	sh ./scripts/test-runtime-introspection.sh http

.PHONY: race-http-runtime-introspection
race-http-runtime-introspection:
	sh ./scripts/test-runtime-introspection.sh race-http

.PHONY: race-runtime-introspection
race-runtime-introspection:
	sh ./scripts/test-runtime-introspection.sh race

.PHONY: benchmark-runtime-introspection
benchmark-runtime-introspection:
	sh ./scripts/test-runtime-introspection.sh bench

.PHONY: format-runtime-introspection
format-runtime-introspection:
	sh ./scripts/test-runtime-introspection.sh format

.PHONY: inspect-runtime-introspection-delivery
inspect-runtime-introspection-delivery:
	sh ./scripts/deliver-runtime-introspection.sh inspect

.PHONY: commit-runtime-introspection
commit-runtime-introspection:
	sh ./scripts/deliver-runtime-introspection.sh commit

.PHONY: push-runtime-introspection
push-runtime-introspection:
	sh ./scripts/deliver-runtime-introspection.sh push

.PHONY: test-geospatial-index
test-geospatial-index:
	sh ./scripts/test-geospatial-index.sh test

.PHONY: test-geospatial-all
test-geospatial-all:
	sh ./scripts/test-geospatial-index.sh all

.PHONY: race-geospatial-index
race-geospatial-index:
	sh ./scripts/test-geospatial-index.sh race

.PHONY: benchmark-geospatial-index
benchmark-geospatial-index:
	sh ./scripts/test-geospatial-index.sh bench

.PHONY: format-geospatial-index
format-geospatial-index:
	sh ./scripts/test-geospatial-index.sh format

.PHONY: inspect-geospatial-delivery
inspect-geospatial-delivery:
	sh ./scripts/deliver-geospatial-index.sh inspect

.PHONY: commit-geospatial-index
commit-geospatial-index:
	sh ./scripts/deliver-geospatial-index.sh commit

.PHONY: push-geospatial-index
push-geospatial-index:
	sh ./scripts/deliver-geospatial-index.sh push

.PHONY: test-query-governor
test-query-governor:
	sh ./scripts/test-query-governor.sh test

.PHONY: race-query-governor
race-query-governor:
	sh ./scripts/test-query-governor.sh race

.PHONY: format-query-governor
format-query-governor:
	sh ./scripts/test-query-governor.sh format

.PHONY: benchmark-query-governor
benchmark-query-governor:
	sh ./scripts/test-query-governor.sh bench

.PHONY: inspect-query-governor-delivery
inspect-query-governor-delivery:
	sh ./scripts/deliver-query-governor.sh inspect

.PHONY: commit-query-governor
commit-query-governor:
	sh ./scripts/deliver-query-governor.sh commit

.PHONY: push-query-governor
push-query-governor:
	sh ./scripts/deliver-query-governor.sh push

.PHONY: test-query-quota
test-query-quota:
	sh ./scripts/test-query-quota.sh test

.PHONY: race-query-quota
race-query-quota:
	sh ./scripts/test-query-quota.sh race

.PHONY: benchmark-query-quota
benchmark-query-quota:
	sh ./scripts/test-query-quota.sh bench

.PHONY: format-query-quota
format-query-quota:
	sh ./scripts/test-query-quota.sh format

.PHONY: inspect-query-quota-delivery
inspect-query-quota-delivery:
	sh ./scripts/deliver-query-quota.sh inspect

.PHONY: commit-query-quota
commit-query-quota:
	sh ./scripts/deliver-query-quota.sh commit

.PHONY: push-query-quota
push-query-quota:
	sh ./scripts/deliver-query-quota.sh push

.PHONY: test-rowbinary-fixed-width
test-rowbinary-fixed-width:
	sh ./scripts/test-rowbinary-fixed-width.sh test

.PHONY: race-rowbinary-fixed-width
race-rowbinary-fixed-width:
	sh ./scripts/test-rowbinary-fixed-width.sh race

.PHONY: format-rowbinary-fixed-width
format-rowbinary-fixed-width:
	sh ./scripts/test-rowbinary-fixed-width.sh format

.PHONY: inspect-rowbinary-fixed-width-delivery
inspect-rowbinary-fixed-width-delivery:
	sh ./scripts/deliver-rowbinary-fixed-width.sh inspect

.PHONY: commit-rowbinary-fixed-width
commit-rowbinary-fixed-width:
	sh ./scripts/deliver-rowbinary-fixed-width.sh commit

.PHONY: push-rowbinary-fixed-width
push-rowbinary-fixed-width:
	sh ./scripts/deliver-rowbinary-fixed-width.sh push

.PHONY: test-tenant-resource-limits
test-tenant-resource-limits:
	sh ./scripts/test-tenant-resource-limits.sh test

.PHONY: race-tenant-resource-limits
race-tenant-resource-limits:
	sh ./scripts/test-tenant-resource-limits.sh race

.PHONY: format-tenant-resource-limits
format-tenant-resource-limits:
	sh ./scripts/test-tenant-resource-limits.sh format

.PHONY: inspect-tenant-resource-limits-delivery
inspect-tenant-resource-limits-delivery:
	sh ./scripts/deliver-tenant-resource-limits.sh inspect

.PHONY: commit-tenant-resource-limits
commit-tenant-resource-limits:
	sh ./scripts/deliver-tenant-resource-limits.sh commit

.PHONY: push-tenant-resource-limits
push-tenant-resource-limits:
	sh ./scripts/deliver-tenant-resource-limits.sh push

.PHONY: test-query-history-sampling
test-query-history-sampling:
	sh ./scripts/test-query-history-sampling.sh test

.PHONY: race-query-history-sampling
race-query-history-sampling:
	sh ./scripts/test-query-history-sampling.sh race

.PHONY: format-query-history-sampling
format-query-history-sampling:
	sh ./scripts/test-query-history-sampling.sh format

.PHONY: inspect-query-history-sampling-delivery
inspect-query-history-sampling-delivery:
	sh ./scripts/deliver-query-history-sampling.sh inspect

.PHONY: commit-query-history-sampling
commit-query-history-sampling:
	sh ./scripts/deliver-query-history-sampling.sh commit

.PHONY: push-query-history-sampling
push-query-history-sampling:
	sh ./scripts/deliver-query-history-sampling.sh push

.PHONY: test-sql-row-binary-delta
test-sql-row-binary-delta:
	sh ./scripts/test-sql-row-binary-delta.sh

.PHONY: format-sql-row-binary-delta
format-sql-row-binary-delta:
	sh ./scripts/format-sql-row-binary-delta.sh

.PHONY: benchmark-sql-row-binary-delta
benchmark-sql-row-binary-delta:
	sh ./scripts/benchmark-sql-row-binary-delta.sh

.PHONY: test-race-sql-row-binary-delta
test-race-sql-row-binary-delta:
	sh ./scripts/test-race-sql-row-binary-delta.sh

.PHONY: deliver-sql-row-binary-delta
deliver-sql-row-binary-delta:
	sh ./scripts/deliver-sql-row-binary-delta.sh preview

.PHONY: commit-sql-row-binary-delta
commit-sql-row-binary-delta:
	sh ./scripts/deliver-sql-row-binary-delta.sh commit

.PHONY: push-sql-row-binary-delta
push-sql-row-binary-delta:
	sh ./scripts/deliver-sql-row-binary-delta.sh push

.PHONY: test-sql-subscription-status
test-sql-subscription-status:
	sh ./scripts/test-sql-subscription-status.sh

.PHONY: format-sql-subscription-status
format-sql-subscription-status:
	sh ./scripts/format-sql-subscription-status.sh

.PHONY: test-race-sql-subscription-status
test-race-sql-subscription-status:
	sh ./scripts/test-race-sql-subscription-status.sh

.PHONY: benchmark-sql-subscription-status
benchmark-sql-subscription-status:
	sh ./scripts/benchmark-sql-subscription-status.sh

.PHONY: deliver-sql-subscription-status
deliver-sql-subscription-status:
	sh ./scripts/deliver-sql-subscription-status.sh preview

.PHONY: commit-sql-subscription-status
commit-sql-subscription-status:
	sh ./scripts/deliver-sql-subscription-status.sh commit

.PHONY: push-sql-subscription-status
push-sql-subscription-status:
	sh ./scripts/deliver-sql-subscription-status.sh push

.PHONY: test-sql-row-binary-adaptive
test-sql-row-binary-adaptive:
	sh ./scripts/test-sql-row-binary-adaptive.sh

.PHONY: format-sql-row-binary-adaptive
format-sql-row-binary-adaptive:
	sh ./scripts/format-sql-row-binary-adaptive.sh

.PHONY: test-race-sql-row-binary-adaptive
test-race-sql-row-binary-adaptive:
	sh ./scripts/test-race-sql-row-binary-adaptive.sh

.PHONY: benchmark-sql-row-binary-adaptive
benchmark-sql-row-binary-adaptive:
	sh ./scripts/benchmark-sql-row-binary-adaptive.sh

.PHONY: deliver-sql-row-binary-adaptive
deliver-sql-row-binary-adaptive:
	sh ./scripts/deliver-sql-row-binary-adaptive.sh preview

.PHONY: commit-sql-row-binary-adaptive
commit-sql-row-binary-adaptive:
	sh ./scripts/deliver-sql-row-binary-adaptive.sh commit

.PHONY: push-sql-row-binary-adaptive
push-sql-row-binary-adaptive:
	sh ./scripts/deliver-sql-row-binary-adaptive.sh push

.PHONY: test-sql-differential-rows
test-sql-differential-rows:
	sh ./scripts/test-sql-differential-rows.sh

.PHONY: format-sql-differential-rows
format-sql-differential-rows:
	sh ./scripts/format-sql-differential-rows.sh

.PHONY: test-race-sql-differential-rows
test-race-sql-differential-rows:
	sh ./scripts/test-race-sql-differential-rows.sh

.PHONY: benchmark-sql-differential-rows
benchmark-sql-differential-rows:
	sh ./scripts/benchmark-sql-differential-rows.sh

.PHONY: deliver-sql-differential-rows
deliver-sql-differential-rows:
	sh ./scripts/deliver-sql-differential-rows.sh preview

.PHONY: commit-sql-differential-rows
commit-sql-differential-rows:
	sh ./scripts/deliver-sql-differential-rows.sh commit

.PHONY: push-sql-differential-rows
push-sql-differential-rows:
	sh ./scripts/deliver-sql-differential-rows.sh push

.PHONY: test-sql-row-binary-read-stats
test-sql-row-binary-read-stats:
	sh ./scripts/test-sql-row-binary-read-stats.sh

.PHONY: format-sql-row-binary-read-stats
format-sql-row-binary-read-stats:
	sh ./scripts/format-sql-row-binary-read-stats.sh

.PHONY: test-race-sql-row-binary-read-stats
test-race-sql-row-binary-read-stats:
	sh ./scripts/test-race-sql-row-binary-read-stats.sh

.PHONY: benchmark-sql-row-binary-read-stats
benchmark-sql-row-binary-read-stats:
	sh ./scripts/benchmark-sql-row-binary-read-stats.sh

.PHONY: deliver-sql-row-binary-read-stats
deliver-sql-row-binary-read-stats:
	sh ./scripts/deliver-sql-row-binary-read-stats.sh preview

.PHONY: commit-sql-row-binary-read-stats
commit-sql-row-binary-read-stats:
	sh ./scripts/deliver-sql-row-binary-read-stats.sh commit

.PHONY: push-sql-row-binary-read-stats
push-sql-row-binary-read-stats:
	sh ./scripts/deliver-sql-row-binary-read-stats.sh push

.PHONY: test-sql-row-binary-bitmap
test-sql-row-binary-bitmap:
	sh ./scripts/test-sql-row-binary-bitmap.sh

.PHONY: format-sql-row-binary-bitmap
format-sql-row-binary-bitmap:
	sh ./scripts/format-sql-row-binary-bitmap.sh

.PHONY: test-race-sql-row-binary-bitmap
test-race-sql-row-binary-bitmap:
	sh ./scripts/test-race-sql-row-binary-bitmap.sh

.PHONY: benchmark-sql-row-binary-bitmap
benchmark-sql-row-binary-bitmap:
	sh ./scripts/benchmark-sql-row-binary-bitmap.sh

.PHONY: deliver-sql-row-binary-bitmap
deliver-sql-row-binary-bitmap:
	sh ./scripts/deliver-sql-row-binary-bitmap.sh preview

.PHONY: commit-sql-row-binary-bitmap
commit-sql-row-binary-bitmap:
	sh ./scripts/deliver-sql-row-binary-bitmap.sh commit

.PHONY: push-sql-row-binary-bitmap
push-sql-row-binary-bitmap:
	sh ./scripts/deliver-sql-row-binary-bitmap.sh push

.PHONY: test-sql-string-dictionary
test-sql-string-dictionary:
	sh ./scripts/test-sql-string-dictionary.sh

.PHONY: format-sql-string-dictionary
format-sql-string-dictionary:
	sh ./scripts/format-sql-string-dictionary.sh

.PHONY: test-race-sql-string-dictionary
test-race-sql-string-dictionary:
	sh ./scripts/test-race-sql-string-dictionary.sh

.PHONY: benchmark-sql-string-dictionary
benchmark-sql-string-dictionary:
	sh ./scripts/benchmark-sql-string-dictionary.sh

.PHONY: deliver-sql-string-dictionary
deliver-sql-string-dictionary:
	sh ./scripts/deliver-sql-string-dictionary.sh preview

.PHONY: commit-sql-string-dictionary
commit-sql-string-dictionary:
	sh ./scripts/deliver-sql-string-dictionary.sh commit

.PHONY: push-sql-string-dictionary
push-sql-string-dictionary:
	sh ./scripts/deliver-sql-string-dictionary.sh push

.PHONY: test-sql-row-binary-stats-pruning
test-sql-row-binary-stats-pruning:
	sh ./scripts/test-sql-row-binary-stats-pruning.sh

.PHONY: format-sql-row-binary-stats-pruning
format-sql-row-binary-stats-pruning:
	sh ./scripts/format-sql-row-binary-stats-pruning.sh

.PHONY: test-race-sql-row-binary-stats-pruning
test-race-sql-row-binary-stats-pruning:
	sh ./scripts/test-race-sql-row-binary-stats-pruning.sh

.PHONY: benchmark-sql-row-binary-stats-pruning
benchmark-sql-row-binary-stats-pruning:
	sh ./scripts/benchmark-sql-row-binary-stats-pruning.sh

.PHONY: deliver-sql-row-binary-stats-pruning
deliver-sql-row-binary-stats-pruning:
	sh ./scripts/deliver-sql-row-binary-stats-pruning.sh preview

.PHONY: commit-sql-row-binary-stats-pruning
commit-sql-row-binary-stats-pruning:
	sh ./scripts/deliver-sql-row-binary-stats-pruning.sh commit

.PHONY: push-sql-row-binary-stats-pruning
push-sql-row-binary-stats-pruning:
	sh ./scripts/deliver-sql-row-binary-stats-pruning.sh push

.PHONY: benchmark-sql-row-binary-codec-accounting
benchmark-sql-row-binary-codec-accounting:
	bash ./scripts/benchmark-sql-row-binary-codec-accounting.sh

.PHONY: deliver-sql-row-binary-codec-accounting
deliver-sql-row-binary-codec-accounting:
	bash ./scripts/deliver-sql-row-binary-codec-accounting.sh preview

.PHONY: commit-sql-row-binary-codec-accounting
commit-sql-row-binary-codec-accounting:
	bash ./scripts/deliver-sql-row-binary-codec-accounting.sh commit

.PHONY: push-sql-row-binary-codec-accounting
push-sql-row-binary-codec-accounting:
	bash ./scripts/deliver-sql-row-binary-codec-accounting.sh push

.PHONY: benchmark-sql-row-binary-codec-accounting
benchmark-sql-row-binary-codec-accounting:
	bash ./scripts/benchmark-sql-row-binary-codec-accounting.sh

.PHONY: deliver-sql-row-binary-codec-accounting
deliver-sql-row-binary-codec-accounting:
	bash ./scripts/deliver-sql-row-binary-codec-accounting.sh preview

.PHONY: commit-sql-row-binary-codec-accounting
commit-sql-row-binary-codec-accounting:
	bash ./scripts/deliver-sql-row-binary-codec-accounting.sh commit

.PHONY: push-sql-row-binary-codec-accounting
push-sql-row-binary-codec-accounting:
	bash ./scripts/deliver-sql-row-binary-codec-accounting.sh push

.PHONY: inspect-row-binary-adaptive
inspect-row-binary-adaptive:
	bash ./scripts/inspect-row-binary-adaptive.sh

.PHONY: test-sql-row-binary-adaptive-sampled
test-sql-row-binary-adaptive-sampled:
	bash ./scripts/test-sql-row-binary-adaptive-sampled.sh

.PHONY: test-race-sql-row-binary-adaptive-sampled
test-race-sql-row-binary-adaptive-sampled:
	bash ./scripts/test-race-sql-row-binary-adaptive-sampled.sh

.PHONY: format-sql-row-binary-adaptive-sampled
format-sql-row-binary-adaptive-sampled:
	bash ./scripts/format-sql-row-binary-adaptive-sampled.sh

.PHONY: benchmark-sql-row-binary-adaptive-sampled
benchmark-sql-row-binary-adaptive-sampled:
	bash ./scripts/benchmark-sql-row-binary-adaptive-sampled.sh

.PHONY: deliver-sql-row-binary-adaptive-sampled
deliver-sql-row-binary-adaptive-sampled:
	bash ./scripts/deliver-sql-row-binary-adaptive-sampled.sh preview

.PHONY: commit-sql-row-binary-adaptive-sampled
commit-sql-row-binary-adaptive-sampled:
	bash ./scripts/deliver-sql-row-binary-adaptive-sampled.sh commit

.PHONY: push-sql-row-binary-adaptive-sampled
push-sql-row-binary-adaptive-sampled:
	bash ./scripts/deliver-sql-row-binary-adaptive-sampled.sh push

.PHONY: inspect-replication-checksum
inspect-replication-checksum:
	bash ./scripts/inspect-replication-checksum.sh

.PHONY: test-part-checksum
test-part-checksum:
	bash ./scripts/test-part-checksum.sh

.PHONY: test-race-part-checksum
test-race-part-checksum:
	bash ./scripts/test-race-part-checksum.sh

.PHONY: format-part-checksum
format-part-checksum:
	bash ./scripts/format-part-checksum.sh

.PHONY: benchmark-part-checksum
benchmark-part-checksum:
	bash ./scripts/benchmark-part-checksum.sh

.PHONY: deliver-part-checksum
deliver-part-checksum:
	bash ./scripts/deliver-part-checksum.sh preview

.PHONY: commit-part-checksum
commit-part-checksum:
	bash ./scripts/deliver-part-checksum.sh commit

.PHONY: push-part-checksum
push-part-checksum:
	bash ./scripts/deliver-part-checksum.sh push

.PHONY: test-sql-replacing-merge
test-sql-replacing-merge:
	bash ./scripts/test-sql-replacing-merge.sh

.PHONY: test-race-sql-replacing-merge
test-race-sql-replacing-merge:
	bash ./scripts/test-race-sql-replacing-merge.sh

.PHONY: format-sql-replacing-merge
format-sql-replacing-merge:
	bash ./scripts/format-sql-replacing-merge.sh

.PHONY: benchmark-sql-replacing-merge
benchmark-sql-replacing-merge:
	bash ./scripts/benchmark-sql-replacing-merge.sh

.PHONY: deliver-sql-replacing-merge
deliver-sql-replacing-merge:
	bash ./scripts/deliver-sql-replacing-merge.sh preview

.PHONY: commit-sql-replacing-merge
commit-sql-replacing-merge:
	bash ./scripts/deliver-sql-replacing-merge.sh commit

.PHONY: push-sql-replacing-merge
push-sql-replacing-merge:
	bash ./scripts/deliver-sql-replacing-merge.sh push

.PHONY: test-sql-summing-merge
test-sql-summing-merge:
	bash ./scripts/test-sql-summing-merge.sh

.PHONY: test-race-sql-summing-merge
test-race-sql-summing-merge:
	bash ./scripts/test-race-sql-summing-merge.sh

.PHONY: format-sql-summing-merge
format-sql-summing-merge:
	bash ./scripts/format-sql-summing-merge.sh

.PHONY: benchmark-sql-summing-merge
benchmark-sql-summing-merge:
	bash ./scripts/benchmark-sql-summing-merge.sh

.PHONY: deliver-sql-summing-merge
deliver-sql-summing-merge:
	bash ./scripts/deliver-sql-summing-merge.sh preview

.PHONY: commit-sql-summing-merge
commit-sql-summing-merge:
	bash ./scripts/deliver-sql-summing-merge.sh commit

.PHONY: push-sql-summing-merge
push-sql-summing-merge:
	bash ./scripts/deliver-sql-summing-merge.sh push

.PHONY: test-sql-collapsing-merge
test-sql-collapsing-merge:
	bash ./scripts/test-sql-collapsing-merge.sh

.PHONY: test-race-sql-collapsing-merge
test-race-sql-collapsing-merge:
	bash ./scripts/test-race-sql-collapsing-merge.sh

.PHONY: format-sql-collapsing-merge
format-sql-collapsing-merge:
	bash ./scripts/format-sql-collapsing-merge.sh

.PHONY: benchmark-sql-collapsing-merge
benchmark-sql-collapsing-merge:
	bash ./scripts/benchmark-sql-collapsing-merge.sh

.PHONY: deliver-sql-collapsing-merge
deliver-sql-collapsing-merge:
	bash ./scripts/deliver-sql-collapsing-merge.sh preview

.PHONY: commit-sql-collapsing-merge
commit-sql-collapsing-merge:
	bash ./scripts/deliver-sql-collapsing-merge.sh commit

.PHONY: push-sql-collapsing-merge
push-sql-collapsing-merge:
	bash ./scripts/deliver-sql-collapsing-merge.sh push

.PHONY: test-sql-deterministic-sample
test-sql-deterministic-sample:
	bash ./scripts/test-sql-deterministic-sample.sh

.PHONY: test-race-sql-deterministic-sample
test-race-sql-deterministic-sample:
	bash ./scripts/test-race-sql-deterministic-sample.sh

.PHONY: format-sql-deterministic-sample
format-sql-deterministic-sample:
	bash ./scripts/format-sql-deterministic-sample.sh

.PHONY: benchmark-sql-deterministic-sample
benchmark-sql-deterministic-sample:
	bash ./scripts/benchmark-sql-deterministic-sample.sh

.PHONY: deliver-sql-deterministic-sample
deliver-sql-deterministic-sample:
	bash ./scripts/deliver-sql-deterministic-sample.sh preview

.PHONY: commit-sql-deterministic-sample
commit-sql-deterministic-sample:
	bash ./scripts/deliver-sql-deterministic-sample.sh commit

.PHONY: push-sql-deterministic-sample
push-sql-deterministic-sample:
	bash ./scripts/deliver-sql-deterministic-sample.sh push

.PHONY: test-sql-differential-distinct
test-sql-differential-distinct:
	bash ./scripts/test-sql-differential-distinct.sh

.PHONY: test-race-sql-differential-distinct
test-race-sql-differential-distinct:
	bash ./scripts/test-race-sql-differential-distinct.sh

.PHONY: format-sql-differential-distinct
format-sql-differential-distinct:
	bash ./scripts/format-sql-differential-distinct.sh

.PHONY: benchmark-sql-differential-distinct
benchmark-sql-differential-distinct:
	bash ./scripts/benchmark-sql-differential-distinct.sh

.PHONY: deliver-sql-differential-distinct
deliver-sql-differential-distinct:
	bash ./scripts/deliver-sql-differential-distinct.sh preview

.PHONY: commit-sql-differential-distinct
commit-sql-differential-distinct:
	bash ./scripts/deliver-sql-differential-distinct.sh commit

.PHONY: push-sql-differential-distinct
push-sql-differential-distinct:
	bash ./scripts/deliver-sql-differential-distinct.sh push

.PHONY: test-sql-differential-group-by
test-sql-differential-group-by:
	bash ./scripts/test-sql-differential-group-by.sh

.PHONY: test-race-sql-differential-group-by
test-race-sql-differential-group-by:
	bash ./scripts/test-race-sql-differential-group-by.sh

.PHONY: format-sql-differential-group-by
format-sql-differential-group-by:
	bash ./scripts/format-sql-differential-group-by.sh

.PHONY: benchmark-sql-differential-group-by
benchmark-sql-differential-group-by:
	bash ./scripts/benchmark-sql-differential-group-by.sh

.PHONY: deliver-sql-differential-group-by
deliver-sql-differential-group-by:
	bash ./scripts/deliver-sql-differential-group-by.sh preview

.PHONY: commit-sql-differential-group-by
commit-sql-differential-group-by:
	bash ./scripts/deliver-sql-differential-group-by.sh commit

.PHONY: push-sql-differential-group-by
push-sql-differential-group-by:
	bash ./scripts/deliver-sql-differential-group-by.sh push

.PHONY: test-sql-typed-table-monotone
test-sql-typed-table-monotone:
	bash ./scripts/test-sql-typed-table-monotone.sh

.PHONY: test-race-sql-typed-table-monotone
test-race-sql-typed-table-monotone:
	bash ./scripts/test-race-sql-typed-table-monotone.sh

.PHONY: format-sql-typed-table-monotone
format-sql-typed-table-monotone:
	bash ./scripts/format-sql-typed-table-monotone.sh

.PHONY: benchmark-sql-typed-table-monotone
benchmark-sql-typed-table-monotone:
	bash ./scripts/benchmark-sql-typed-table-monotone.sh

.PHONY: deliver-sql-typed-table-monotone
deliver-sql-typed-table-monotone:
	bash ./scripts/deliver-sql-typed-table-monotone.sh preview

.PHONY: commit-sql-typed-table-monotone
commit-sql-typed-table-monotone:
	bash ./scripts/deliver-sql-typed-table-monotone.sh commit

.PHONY: push-sql-typed-table-monotone
push-sql-typed-table-monotone:
	bash ./scripts/deliver-sql-typed-table-monotone.sh push

.PHONY: test-sql-differential-late-data
test-sql-differential-late-data:
	bash ./scripts/test-sql-differential-late-data.sh

.PHONY: test-race-sql-differential-late-data
test-race-sql-differential-late-data:
	bash ./scripts/test-race-sql-differential-late-data.sh

.PHONY: format-sql-differential-late-data
format-sql-differential-late-data:
	bash ./scripts/format-sql-differential-late-data.sh

.PHONY: benchmark-sql-differential-late-data
benchmark-sql-differential-late-data:
	bash ./scripts/benchmark-sql-differential-late-data.sh

.PHONY: deliver-sql-differential-late-data
deliver-sql-differential-late-data:
	bash ./scripts/deliver-sql-differential-late-data.sh preview

.PHONY: commit-sql-differential-late-data
commit-sql-differential-late-data:
	bash ./scripts/deliver-sql-differential-late-data.sh commit

.PHONY: push-sql-differential-late-data
push-sql-differential-late-data:
	bash ./scripts/deliver-sql-differential-late-data.sh push

.PHONY: test-sql-differential-watermark
test-sql-differential-watermark:
	bash ./scripts/test-sql-differential-watermark.sh

.PHONY: test-race-sql-differential-watermark
test-race-sql-differential-watermark:
	bash ./scripts/test-race-sql-differential-watermark.sh

.PHONY: format-sql-differential-watermark
format-sql-differential-watermark:
	bash ./scripts/format-sql-differential-watermark.sh

.PHONY: benchmark-sql-differential-watermark
benchmark-sql-differential-watermark:
	bash ./scripts/benchmark-sql-differential-watermark.sh

.PHONY: deliver-sql-differential-watermark
deliver-sql-differential-watermark:
	bash ./scripts/deliver-sql-differential-watermark.sh preview

.PHONY: commit-sql-differential-watermark
commit-sql-differential-watermark:
	bash ./scripts/deliver-sql-differential-watermark.sh commit

.PHONY: push-sql-differential-watermark
push-sql-differential-watermark:
	bash ./scripts/deliver-sql-differential-watermark.sh push

.PHONY: test-sql-differential-temporal-join
test-sql-differential-temporal-join:
	bash ./scripts/test-sql-differential-temporal-join.sh

.PHONY: test-race-sql-differential-temporal-join
test-race-sql-differential-temporal-join:
	bash ./scripts/test-race-sql-differential-temporal-join.sh

.PHONY: format-sql-differential-temporal-join
format-sql-differential-temporal-join:
	bash ./scripts/format-sql-differential-temporal-join.sh

.PHONY: benchmark-sql-differential-temporal-join
benchmark-sql-differential-temporal-join:
	bash ./scripts/benchmark-sql-differential-temporal-join.sh

.PHONY: deliver-sql-differential-temporal-join
deliver-sql-differential-temporal-join:
	bash ./scripts/deliver-sql-differential-temporal-join.sh preview

.PHONY: commit-sql-differential-temporal-join
commit-sql-differential-temporal-join:
	bash ./scripts/deliver-sql-differential-temporal-join.sh commit

.PHONY: push-sql-differential-temporal-join
push-sql-differential-temporal-join:
	bash ./scripts/deliver-sql-differential-temporal-join.sh push

.PHONY: deliver-sql-differential-interval-join
deliver-sql-differential-interval-join:
	bash ./scripts/deliver-sql-differential-interval-join.sh preview

.PHONY: commit-sql-differential-interval-join
commit-sql-differential-interval-join:
	bash ./scripts/deliver-sql-differential-interval-join.sh commit

.PHONY: push-sql-differential-interval-join
push-sql-differential-interval-join:
	bash ./scripts/deliver-sql-differential-interval-join.sh push

.PHONY: format-source-frontier
format-source-frontier:
	bash ./scripts/format-source-frontier.sh

.PHONY: test-source-frontier
test-source-frontier:
	bash ./scripts/test-source-frontier.sh

.PHONY: test-race-source-frontier
test-race-source-frontier:
	bash ./scripts/test-race-source-frontier.sh

.PHONY: benchmark-source-frontier
benchmark-source-frontier:
	bash ./scripts/benchmark-source-frontier.sh

.PHONY: deliver-source-frontier
deliver-source-frontier:
	bash ./scripts/deliver-source-frontier.sh preview

.PHONY: commit-source-frontier
commit-source-frontier:
	bash ./scripts/deliver-source-frontier.sh commit

.PHONY: push-source-frontier
push-source-frontier:
	bash ./scripts/deliver-source-frontier.sh push

.PHONY: format-operator-memory
format-operator-memory:
	bash ./scripts/format-operator-memory.sh

.PHONY: test-operator-memory
test-operator-memory:
	bash ./scripts/test-operator-memory.sh

.PHONY: test-race-operator-memory
test-race-operator-memory:
	bash ./scripts/test-race-operator-memory.sh

.PHONY: benchmark-operator-memory
benchmark-operator-memory:
	bash ./scripts/benchmark-operator-memory.sh

.PHONY: deliver-operator-memory
deliver-operator-memory:
	bash ./scripts/deliver-operator-memory.sh preview

.PHONY: commit-operator-memory
commit-operator-memory:
	bash ./scripts/deliver-operator-memory.sh commit

.PHONY: push-operator-memory
push-operator-memory:
	bash ./scripts/deliver-operator-memory.sh push

.PHONY: format-collection-metrics
format-collection-metrics:
	bash ./scripts/format-collection-metrics.sh

.PHONY: test-collection-metrics
test-collection-metrics:
	bash ./scripts/test-collection-metrics.sh

.PHONY: test-race-collection-metrics
test-race-collection-metrics:
	bash ./scripts/test-race-collection-metrics.sh

.PHONY: benchmark-collection-metrics
benchmark-collection-metrics:
	bash ./scripts/benchmark-collection-metrics.sh

.PHONY: deliver-collection-metrics
deliver-collection-metrics:
	bash ./scripts/deliver-collection-metrics.sh preview

.PHONY: commit-collection-metrics
commit-collection-metrics:
	bash ./scripts/deliver-collection-metrics.sh commit

.PHONY: push-collection-metrics
push-collection-metrics:
	bash ./scripts/deliver-collection-metrics.sh push

.PHONY: format-read-replica-policy
format-read-replica-policy:
	bash ./scripts/format-read-replica-policy.sh

.PHONY: test-read-replica-policy
test-read-replica-policy:
	bash ./scripts/test-read-replica-policy.sh

.PHONY: test-race-read-replica-policy
test-race-read-replica-policy:
	bash ./scripts/test-race-read-replica-policy.sh

.PHONY: benchmark-read-replica-policy
benchmark-read-replica-policy:
	bash ./scripts/benchmark-read-replica-policy.sh

.PHONY: deliver-read-replica-policy
deliver-read-replica-policy:
	bash ./scripts/deliver-read-replica-policy.sh preview

.PHONY: commit-read-replica-policy
commit-read-replica-policy:
	bash ./scripts/deliver-read-replica-policy.sh commit

.PHONY: push-read-replica-policy
push-read-replica-policy:
	bash ./scripts/deliver-read-replica-policy.sh push

.PHONY: format-replay-digest
format-replay-digest:
	bash ./scripts/format-replay-digest.sh

.PHONY: test-replay-digest
test-replay-digest:
	bash ./scripts/test-replay-digest.sh

.PHONY: test-race-replay-digest
test-race-replay-digest:
	bash ./scripts/test-race-replay-digest.sh

.PHONY: benchmark-replay-digest
benchmark-replay-digest:
	bash ./scripts/benchmark-replay-digest.sh

.PHONY: deliver-replay-digest
deliver-replay-digest:
	bash ./scripts/deliver-replay-digest.sh preview

.PHONY: commit-replay-digest
commit-replay-digest:
	bash ./scripts/deliver-replay-digest.sh commit

.PHONY: push-replay-digest
push-replay-digest:
	bash ./scripts/deliver-replay-digest.sh push

.PHONY: format-quorum-policy
format-quorum-policy:
	bash ./scripts/format-quorum-policy.sh

.PHONY: test-quorum-policy
test-quorum-policy:
	bash ./scripts/test-quorum-policy.sh

.PHONY: test-race-quorum-policy
test-race-quorum-policy:
	bash ./scripts/test-race-quorum-policy.sh

.PHONY: benchmark-quorum-policy
benchmark-quorum-policy:
	bash ./scripts/benchmark-quorum-policy.sh

.PHONY: deliver-quorum-policy
deliver-quorum-policy:
	bash ./scripts/deliver-quorum-policy.sh preview

.PHONY: commit-quorum-policy
commit-quorum-policy:
	bash ./scripts/deliver-quorum-policy.sh commit

.PHONY: push-quorum-policy
push-quorum-policy:
	bash ./scripts/deliver-quorum-policy.sh push

.PHONY: format-conflict-resolution
format-conflict-resolution:
	bash ./scripts/format-conflict-resolution.sh

.PHONY: test-conflict-resolution
test-conflict-resolution:
	bash ./scripts/test-conflict-resolution.sh

.PHONY: test-race-conflict-resolution
test-race-conflict-resolution:
	bash ./scripts/test-race-conflict-resolution.sh

.PHONY: benchmark-conflict-resolution
benchmark-conflict-resolution:
	bash ./scripts/benchmark-conflict-resolution.sh

.PHONY: deliver-conflict-resolution
deliver-conflict-resolution:
	bash ./scripts/deliver-conflict-resolution.sh preview

.PHONY: commit-conflict-resolution
commit-conflict-resolution:
	bash ./scripts/deliver-conflict-resolution.sh commit

.PHONY: push-conflict-resolution
push-conflict-resolution:
	bash ./scripts/deliver-conflict-resolution.sh push

.PHONY: format-codec-metrics
format-codec-metrics:
	bash ./scripts/format-codec-metrics.sh

.PHONY: test-codec-metrics
test-codec-metrics:
	bash ./scripts/test-codec-metrics.sh

.PHONY: test-race-codec-metrics
test-race-codec-metrics:
	bash ./scripts/test-race-codec-metrics.sh

.PHONY: benchmark-codec-metrics
benchmark-codec-metrics:
	bash ./scripts/benchmark-codec-metrics.sh

.PHONY: deliver-codec-metrics
deliver-codec-metrics:
	bash ./scripts/deliver-codec-metrics.sh preview

.PHONY: commit-codec-metrics
commit-codec-metrics:
	bash ./scripts/deliver-codec-metrics.sh commit

.PHONY: push-codec-metrics
push-codec-metrics:
	bash ./scripts/deliver-codec-metrics.sh push

.PHONY: format-late-data-policy
format-late-data-policy:
	bash ./scripts/format-late-data-policy.sh

.PHONY: test-late-data-policy
test-late-data-policy:
	bash ./scripts/test-late-data-policy.sh

.PHONY: test-race-late-data-policy
test-race-late-data-policy:
	bash ./scripts/test-race-late-data-policy.sh

.PHONY: benchmark-late-data-policy
benchmark-late-data-policy:
	bash ./scripts/benchmark-late-data-policy.sh

.PHONY: deliver-late-data-policy
deliver-late-data-policy:
	bash ./scripts/deliver-late-data-policy.sh preview

.PHONY: commit-late-data-policy
commit-late-data-policy:
	bash ./scripts/deliver-late-data-policy.sh commit

.PHONY: push-late-data-policy
push-late-data-policy:
	bash ./scripts/deliver-late-data-policy.sh push

.PHONY: format-watermark
format-watermark:
	bash ./scripts/format-watermark.sh

.PHONY: test-watermark
test-watermark:
	bash ./scripts/test-watermark.sh

.PHONY: test-race-watermark
test-race-watermark:
	bash ./scripts/test-race-watermark.sh

.PHONY: benchmark-watermark
benchmark-watermark:
	bash ./scripts/benchmark-watermark.sh

.PHONY: deliver-watermark
deliver-watermark:
	bash ./scripts/deliver-watermark.sh preview

.PHONY: commit-watermark
commit-watermark:
	bash ./scripts/deliver-watermark.sh commit

.PHONY: push-watermark
push-watermark:
	bash ./scripts/deliver-watermark.sh push

.PHONY: format-nullable-bitmap
format-nullable-bitmap:
	bash ./scripts/format-nullable-bitmap.sh

.PHONY: test-nullable-bitmap
test-nullable-bitmap:
	bash ./scripts/test-nullable-bitmap.sh

.PHONY: test-race-nullable-bitmap
test-race-nullable-bitmap:
	bash ./scripts/test-race-nullable-bitmap.sh

.PHONY: benchmark-nullable-bitmap
benchmark-nullable-bitmap:
	bash ./scripts/benchmark-nullable-bitmap.sh

.PHONY: deliver-nullable-bitmap
deliver-nullable-bitmap:
	bash ./scripts/deliver-nullable-bitmap.sh preview

.PHONY: commit-nullable-bitmap
commit-nullable-bitmap:
	bash ./scripts/deliver-nullable-bitmap.sh commit

.PHONY: push-nullable-bitmap
push-nullable-bitmap:
	bash ./scripts/deliver-nullable-bitmap.sh push

.PHONY: format-gorilla-float
format-gorilla-float:
	bash ./scripts/format-gorilla-float.sh

.PHONY: test-gorilla-float
test-gorilla-float:
	bash ./scripts/test-gorilla-float.sh

.PHONY: test-race-gorilla-float
test-race-gorilla-float:
	bash ./scripts/test-race-gorilla-float.sh

.PHONY: benchmark-gorilla-float
benchmark-gorilla-float:
	bash ./scripts/benchmark-gorilla-float.sh

.PHONY: deliver-gorilla-float
deliver-gorilla-float:
	bash ./scripts/deliver-gorilla-float.sh preview

.PHONY: commit-gorilla-float
commit-gorilla-float:
	bash ./scripts/deliver-gorilla-float.sh commit

.PHONY: push-gorilla-float
push-gorilla-float:
	bash ./scripts/deliver-gorilla-float.sh push

.PHONY: format-codec-selection
format-codec-selection:
	bash ./scripts/format-codec-selection.sh

.PHONY: test-codec-selection
test-codec-selection:
	bash ./scripts/test-codec-selection.sh

.PHONY: test-race-codec-selection
test-race-codec-selection:
	bash ./scripts/test-race-codec-selection.sh

.PHONY: benchmark-codec-selection
benchmark-codec-selection:
	bash ./scripts/benchmark-codec-selection.sh

.PHONY: deliver-codec-selection
deliver-codec-selection:
	bash ./scripts/deliver-codec-selection.sh preview

.PHONY: commit-codec-selection
commit-codec-selection:
	bash ./scripts/deliver-codec-selection.sh commit

.PHONY: push-codec-selection
push-codec-selection:
	bash ./scripts/deliver-codec-selection.sh push

.PHONY: format-differential-multiset
format-differential-multiset:
	bash ./scripts/format-differential-multiset.sh

.PHONY: test-differential-multiset
test-differential-multiset:
	bash ./scripts/test-differential-multiset.sh

.PHONY: test-race-differential-multiset
test-race-differential-multiset:
	bash ./scripts/test-race-differential-multiset.sh

.PHONY: benchmark-differential-multiset
benchmark-differential-multiset:
	bash ./scripts/benchmark-differential-multiset.sh

.PHONY: deliver-differential-multiset
deliver-differential-multiset:
	bash ./scripts/deliver-differential-multiset.sh preview

.PHONY: commit-differential-multiset
commit-differential-multiset:
	bash ./scripts/deliver-differential-multiset.sh commit

.PHONY: push-differential-multiset
push-differential-multiset:
	bash ./scripts/deliver-differential-multiset.sh push

.PHONY: format-bitpacked-numeric
format-bitpacked-numeric:
	bash ./scripts/format-bitpacked-numeric.sh

.PHONY: test-bitpacked-numeric
test-bitpacked-numeric:
	bash ./scripts/test-bitpacked-numeric.sh

.PHONY: test-race-bitpacked-numeric
test-race-bitpacked-numeric:
	bash ./scripts/test-race-bitpacked-numeric.sh

.PHONY: benchmark-bitpacked-numeric
benchmark-bitpacked-numeric:
	bash ./scripts/benchmark-bitpacked-numeric.sh

.PHONY: deliver-bitpacked-numeric
deliver-bitpacked-numeric:
	bash ./scripts/deliver-bitpacked-numeric.sh preview

.PHONY: commit-bitpacked-numeric
commit-bitpacked-numeric:
	bash ./scripts/deliver-bitpacked-numeric.sh commit

.PHONY: push-bitpacked-numeric
push-bitpacked-numeric:
	bash ./scripts/deliver-bitpacked-numeric.sh push

.PHONY: format-disk-placement test-disk-placement test-race-disk-placement benchmark-disk-placement deliver-disk-placement commit-disk-placement push-disk-placement
format-disk-placement:
	bash ./scripts/format-disk-placement.sh
test-disk-placement:
	bash ./scripts/test-disk-placement.sh
test-race-disk-placement:
	bash ./scripts/test-race-disk-placement.sh
benchmark-disk-placement:
	bash ./scripts/benchmark-disk-placement.sh
deliver-disk-placement:
	bash ./scripts/deliver-disk-placement.sh preview
commit-disk-placement:
	bash ./scripts/deliver-disk-placement.sh commit
push-disk-placement:
	bash ./scripts/deliver-disk-placement.sh push

.PHONY: format-read-amplification test-read-amplification test-race-read-amplification benchmark-read-amplification deliver-read-amplification commit-read-amplification push-read-amplification
format-read-amplification:
	bash ./scripts/format-read-amplification.sh
test-read-amplification:
	bash ./scripts/test-read-amplification.sh
test-race-read-amplification:
	bash ./scripts/test-race-read-amplification.sh
benchmark-read-amplification:
	bash ./scripts/benchmark-read-amplification.sh
deliver-read-amplification:
	bash ./scripts/deliver-read-amplification.sh preview
commit-read-amplification:
	bash ./scripts/deliver-read-amplification.sh commit
push-read-amplification:
	bash ./scripts/deliver-read-amplification.sh push

.PHONY: format-storage-tier test-storage-tier test-race-storage-tier benchmark-storage-tier deliver-storage-tier commit-storage-tier push-storage-tier
format-storage-tier:
	bash ./scripts/format-storage-tier.sh
test-storage-tier:
	bash ./scripts/test-storage-tier.sh
test-race-storage-tier:
	bash ./scripts/test-race-storage-tier.sh
benchmark-storage-tier:
	bash ./scripts/benchmark-storage-tier.sh
deliver-storage-tier:
	bash ./scripts/deliver-storage-tier.sh preview
commit-storage-tier:
	bash ./scripts/deliver-storage-tier.sh commit
push-storage-tier:
	bash ./scripts/deliver-storage-tier.sh push

.PHONY: format-part-cache-policy test-part-cache-policy test-race-part-cache-policy benchmark-part-cache-policy deliver-part-cache-policy commit-part-cache-policy push-part-cache-policy
format-part-cache-policy:
	bash ./scripts/format-part-cache-policy.sh
test-part-cache-policy:
	bash ./scripts/test-part-cache-policy.sh
test-race-part-cache-policy:
	bash ./scripts/test-race-part-cache-policy.sh
benchmark-part-cache-policy:
	bash ./scripts/benchmark-part-cache-policy.sh
deliver-part-cache-policy:
	bash ./scripts/deliver-part-cache-policy.sh preview
commit-part-cache-policy:
	bash ./scripts/deliver-part-cache-policy.sh commit
push-part-cache-policy:
	bash ./scripts/deliver-part-cache-policy.sh push

.PHONY: format-granule-sizing test-granule-sizing test-race-granule-sizing benchmark-granule-sizing deliver-granule-sizing commit-granule-sizing push-granule-sizing
format-granule-sizing:
	bash ./scripts/format-granule-sizing.sh
test-granule-sizing:
	bash ./scripts/test-granule-sizing.sh
test-race-granule-sizing:
	bash ./scripts/test-race-granule-sizing.sh
benchmark-granule-sizing:
	bash ./scripts/benchmark-granule-sizing.sh
deliver-granule-sizing:
	bash ./scripts/deliver-granule-sizing.sh preview
commit-granule-sizing:
	bash ./scripts/deliver-granule-sizing.sh commit
push-granule-sizing:
	bash ./scripts/deliver-granule-sizing.sh push

.PHONY: format-remote-part test-remote-part test-race-remote-part benchmark-remote-part deliver-remote-part commit-remote-part push-remote-part
format-remote-part:
	bash ./scripts/format-remote-part.sh
test-remote-part:
	bash ./scripts/test-remote-part.sh
test-race-remote-part:
	bash ./scripts/test-race-remote-part.sh
benchmark-remote-part:
	bash ./scripts/benchmark-remote-part.sh
deliver-remote-part:
	bash ./scripts/deliver-remote-part.sh preview
commit-remote-part:
	bash ./scripts/deliver-remote-part.sh commit
push-remote-part:
	bash ./scripts/deliver-remote-part.sh push

.PHONY: format-json-subcolumns test-json-subcolumns test-race-json-subcolumns benchmark-json-subcolumns deliver-json-subcolumns commit-json-subcolumns push-json-subcolumns
format-json-subcolumns:
	bash ./scripts/format-json-subcolumns.sh
test-json-subcolumns:
	bash ./scripts/test-json-subcolumns.sh
test-race-json-subcolumns:
	bash ./scripts/test-race-json-subcolumns.sh
benchmark-json-subcolumns:
	bash ./scripts/benchmark-json-subcolumns.sh
deliver-json-subcolumns:
	bash ./scripts/deliver-json-subcolumns.sh preview
commit-json-subcolumns:
	bash ./scripts/deliver-json-subcolumns.sh commit
push-json-subcolumns:
	bash ./scripts/deliver-json-subcolumns.sh push

.PHONY: format-compression-negotiation test-compression-negotiation test-race-compression-negotiation benchmark-compression-negotiation deliver-compression-negotiation commit-compression-negotiation push-compression-negotiation
format-compression-negotiation:
	bash ./scripts/format-compression-negotiation.sh
test-compression-negotiation:
	bash ./scripts/test-compression-negotiation.sh
test-race-compression-negotiation:
	bash ./scripts/test-race-compression-negotiation.sh
benchmark-compression-negotiation:
	bash ./scripts/benchmark-compression-negotiation.sh
deliver-compression-negotiation:
	bash ./scripts/deliver-compression-negotiation.sh preview
commit-compression-negotiation:
	bash ./scripts/deliver-compression-negotiation.sh commit
push-compression-negotiation:
	bash ./scripts/deliver-compression-negotiation.sh push

# pipeline-stage-targets
format-pipeline-stages:
	bash ./scripts/format-pipeline-stages.sh

test-pipeline-stages:
	bash ./scripts/test-pipeline-stages.sh

test-race-pipeline-stages:
	bash ./scripts/test-race-pipeline-stages.sh

benchmark-pipeline-stages:
	bash ./scripts/benchmark-pipeline-stages.sh

deliver-pipeline-stages:
	bash ./scripts/deliver-pipeline-stages.sh preview

commit-pipeline-stages:
	bash ./scripts/deliver-pipeline-stages.sh commit

push-pipeline-stages:
	bash ./scripts/deliver-pipeline-stages.sh push

# columnar-layout-selection-targets
format-columnar-layout-selection:
	bash ./scripts/format-columnar-layout-selection.sh

test-columnar-layout-selection:
	bash ./scripts/test-columnar-layout-selection.sh

test-race-columnar-layout-selection:
	bash ./scripts/test-race-columnar-layout-selection.sh

benchmark-columnar-layout-selection:
	bash ./scripts/benchmark-columnar-layout-selection.sh

deliver-columnar-layout-selection:
	bash ./scripts/deliver-columnar-layout-selection.sh preview

commit-columnar-layout-selection:
	bash ./scripts/deliver-columnar-layout-selection.sh commit

push-columnar-layout-selection:
	bash ./scripts/deliver-columnar-layout-selection.sh push

# columnar-vertical-merge-targets
format-columnar-vertical-merge:
	bash ./scripts/format-columnar-vertical-merge.sh

test-columnar-vertical-merge:
	bash ./scripts/test-columnar-vertical-merge.sh

test-race-columnar-vertical-merge:
	bash ./scripts/test-race-columnar-vertical-merge.sh

benchmark-columnar-vertical-merge:
	bash ./scripts/benchmark-columnar-vertical-merge.sh

deliver-columnar-vertical-merge:
	bash ./scripts/deliver-columnar-vertical-merge.sh preview

commit-columnar-vertical-merge:
	bash ./scripts/deliver-columnar-vertical-merge.sh commit

push-columnar-vertical-merge:
	bash ./scripts/deliver-columnar-vertical-merge.sh push

# rollup-ttl-targets
format-rollup-ttl:
	bash ./scripts/format-rollup-ttl.sh

test-rollup-ttl:
	bash ./scripts/test-rollup-ttl.sh

test-race-rollup-ttl:
	bash ./scripts/test-race-rollup-ttl.sh

benchmark-rollup-ttl:
	bash ./scripts/benchmark-rollup-ttl.sh

deliver-rollup-ttl:
	bash ./scripts/deliver-rollup-ttl.sh preview

commit-rollup-ttl:
	bash ./scripts/deliver-rollup-ttl.sh commit

push-rollup-ttl:
	bash ./scripts/deliver-rollup-ttl.sh push

# aggregate-collections-targets
format-aggregate-collections:
	bash ./scripts/format-aggregate-collections.sh

test-aggregate-collections:
	bash ./scripts/test-aggregate-collections.sh

test-race-aggregate-collections:
	bash ./scripts/test-race-aggregate-collections.sh

benchmark-aggregate-collections:
	bash ./scripts/benchmark-aggregate-collections.sh

deliver-aggregate-collections:
	bash ./scripts/deliver-aggregate-collections.sh preview

commit-aggregate-collections:
	bash ./scripts/deliver-aggregate-collections.sh commit

push-aggregate-collections:
	bash ./scripts/deliver-aggregate-collections.sh push

# array-join-targets
format-array-join:
	bash ./scripts/format-array-join.sh

test-array-join:
	bash ./scripts/test-array-join.sh

test-race-array-join:
	bash ./scripts/test-race-array-join.sh

benchmark-array-join:
	bash ./scripts/benchmark-array-join.sh

deliver-array-join:
	bash ./scripts/deliver-array-join.sh preview

commit-array-join:
	bash ./scripts/deliver-array-join.sh commit

push-array-join:
	bash ./scripts/deliver-array-join.sh push

# with-fill-query-targets
format-with-fill-query:
	bash ./scripts/format-with-fill-query.sh

test-with-fill-query:
	bash ./scripts/test-with-fill-query.sh

test-with-fill-package:
	bash ./scripts/test-with-fill-package.sh

race-with-fill-query:
	bash ./scripts/race-with-fill-query.sh

benchmark-with-fill-query:
	bash ./scripts/benchmark-with-fill-query.sh

deliver-with-fill-query:
	bash ./scripts/deliver-with-fill-query.sh preview

commit-with-fill-query:
	bash ./scripts/deliver-with-fill-query.sh commit

push-with-fill-query:
	bash ./scripts/deliver-with-fill-query.sh push

# c015-simd-targets
format-c015:
	bash ./scripts/format-c015.sh

test-c015:
	bash ./scripts/test-c015.sh

race-c015:
	bash ./scripts/race-c015.sh

benchmark-c015:
	bash ./scripts/benchmark-c015.sh

deliver-c015:
	bash ./scripts/deliver-c015.sh preview

commit-c015:
	bash ./scripts/deliver-c015.sh commit

push-c015:
	bash ./scripts/deliver-c015.sh push

format-rtree:
	bash ./scripts/format-rtree.sh

test-rtree:
	bash ./scripts/test-rtree.sh

race-rtree:
	bash ./scripts/race-rtree.sh

benchmark-rtree:
	bash ./scripts/benchmark-rtree.sh

deliver-rtree:
	bash ./scripts/deliver-rtree.sh preview

commit-rtree:
	bash ./scripts/deliver-rtree.sh commit

push-rtree:
	bash ./scripts/deliver-rtree.sh push

verify-c016:
	bash ./scripts/verify-c016.sh

deliver-c016-fix:
	bash ./scripts/deliver-c016-fix.sh preview

commit-c016-fix:
	bash ./scripts/deliver-c016-fix.sh commit

push-c016-fix:
	bash ./scripts/deliver-c016-fix.sh push

deliver-c016-checklist:
	bash ./scripts/deliver-c016-checklist.sh preview

commit-c016-checklist:
	bash ./scripts/deliver-c016-checklist.sh commit

push-c016-checklist:
	bash ./scripts/deliver-c016-checklist.sh push

verify-c057:
	bash ./scripts/verify-c057.sh

deliver-c057-checklist:
	bash ./scripts/deliver-c057-checklist.sh preview

commit-c057-checklist:
	bash ./scripts/deliver-c057-checklist.sh commit

push-c057-checklist:
	bash ./scripts/deliver-c057-checklist.sh push

verify-c059:
	sh ./scripts/verify-c059.sh

deliver-c059-checklist:
	sh ./scripts/deliver-c059-checklist.sh

verify-c060:
	sh ./scripts/verify-c060.sh

deliver-c060-checklist:
	sh ./scripts/deliver-c060-checklist.sh

verify-c123:
	sh ./scripts/verify-c123.sh

deliver-c123-checklist:
	sh ./scripts/deliver-c123-checklist.sh

verify-c128:
	sh ./scripts/verify-c128.sh

deliver-c128-checklist:
	sh ./scripts/deliver-c128-checklist.sh

verify-c152:
	sh ./scripts/verify-c152.sh

deliver-c152-checklist:
	sh ./scripts/deliver-c152-checklist.sh

verify-c158:
	sh ./scripts/verify-c158.sh

deliver-c158-checklist:
	sh ./scripts/deliver-c158-checklist.sh

verify-c156:
	sh ./scripts/verify-c156.sh

deliver-c156-checklist:
	sh ./scripts/deliver-c156-checklist.sh

verify-c159:
	sh ./scripts/verify-c159.sh

deliver-c159-checklist:
	sh ./scripts/deliver-c159-checklist.sh

verify-m084:
	sh ./scripts/verify-m084.sh

deliver-m084-checklist:
	sh ./scripts/deliver-m084-checklist.sh

verify-m085:
	sh ./scripts/verify-m085.sh

deliver-m085-checklist:
	sh ./scripts/deliver-m085-checklist.sh

verify-m087:
	sh ./scripts/verify-m087.sh

deliver-m087-checklist:
	sh ./scripts/deliver-m087-checklist.sh

verify-m088:
	sh ./scripts/verify-m088.sh

deliver-m088-checklist:
	sh ./scripts/deliver-m088-checklist.sh

format-t058:
	sh ./scripts/format-t058.sh

test-t058-stage:
	sh ./scripts/test-t058-stage.sh

test-t058-edge:
	sh ./scripts/test-t058-edge.sh

verify-t058:
	sh ./scripts/verify-t058.sh

deliver-t058:
	sh ./scripts/deliver-t058.sh

audit-inspiration-remote:
	sh ./scripts/audit-inspiration-remote.sh

deliver-inspiration-audit:
	sh ./scripts/deliver-inspiration-audit.sh

inspect-object-backup:
	sh ./scripts/inspect-object-backup.sh

test-object-store-verify-stage:
	sh ./scripts/test-object-store-verify-stage.sh

format-object-store-verify:
	sh ./scripts/format-object-store-verify.sh

verify-object-store-verify:
	sh ./scripts/verify-object-store-verify.sh

deliver-object-store-verify:
	sh ./scripts/deliver-object-store-verify.sh

list-inspiration-open:
	sh ./scripts/list-inspiration-open.sh

deliver-inspiration-list:
	sh ./scripts/deliver-inspiration-list.sh

inspect-frontier-metrics:
	sh ./scripts/inspect-frontier-metrics.sh

test-source-frontier-monitoring:
	sh ./scripts/test-source-frontier-monitoring.sh

format-source-frontier-monitoring:
	sh ./scripts/format-source-frontier-monitoring.sh

verify-source-frontier-monitoring:
	sh ./scripts/verify-source-frontier-monitoring.sh

deliver-source-frontier-monitoring:
	sh ./scripts/deliver-source-frontier-monitoring.sh

test-operator-memory-monitoring:
	sh ./scripts/test-operator-memory-monitoring.sh

format-operator-memory-monitoring:
	sh ./scripts/format-operator-memory-monitoring.sh

verify-operator-memory-monitoring:
	sh ./scripts/verify-operator-memory-monitoring.sh

deliver-operator-memory-monitoring:
	sh ./scripts/deliver-operator-memory-monitoring.sh

inspect-m080:
	sh ./scripts/inspect-m080.sh

test-m080:
	sh ./scripts/test-m080.sh

format-m080:
	sh ./scripts/format-m080.sh

test-m080-full:
	sh ./scripts/test-m080-full.sh

test-m080-race:
	sh ./scripts/test-m080-race.sh

.PHONY: test-limit-with-ties-local-clean format-limit-with-ties-local-clean test-limit-with-ties-package-local-clean test-limit-with-ties-cache-sql-local-clean test-limit-with-ties-regression-local-clean test-limit-with-ties-race-local-clean vet-limit-with-ties-local-clean benchmark-limit-with-ties-local-clean verify-limit-with-ties-doc-local-clean
test-limit-with-ties-local-clean:
	bash ./scripts/test-limit-with-ties-local-clean.sh

format-limit-with-ties-local-clean:
	bash ./scripts/format-limit-with-ties-local-clean.sh

test-limit-with-ties-package-local-clean:
	bash ./scripts/test-limit-with-ties-package-local-clean.sh

test-limit-with-ties-cache-sql-local-clean:
	bash ./scripts/test-limit-with-ties-cache-sql-local-clean.sh

test-limit-with-ties-regression-local-clean:
	bash ./scripts/test-limit-with-ties-regression-local-clean.sh

test-limit-with-ties-race-local-clean:
	bash ./scripts/test-limit-with-ties-race-local-clean.sh

vet-limit-with-ties-local-clean:
	bash ./scripts/vet-limit-with-ties-local-clean.sh

benchmark-limit-with-ties-local-clean:
	bash ./scripts/benchmark-limit-with-ties-local-clean.sh

verify-limit-with-ties-doc-local-clean:
	bash ./scripts/verify-limit-with-ties-doc-local-clean.sh

.PHONY: test-partition-resize benchmark-partition-resize
test-partition-resize:
	bash ./scripts/test-partition-resize.sh
benchmark-partition-resize:
	bash ./scripts/benchmark-partition-resize.sh
.PHONY: generate-inspiration-audit
generate-inspiration-audit:
	bash ./scripts/generate-inspiration-audit.sh
	bash ./scripts/verify-inspiration-audit.sh

.PHONY: verify-inspiration-audit
verify-inspiration-audit:
	bash ./scripts/verify-inspiration-audit.sh

.PHONY: commit-inspiration-audit
commit-inspiration-audit:
	bash ./scripts/commit-inspiration-audit.sh

.PHONY: benchmark-c154-rollout
benchmark-c154-rollout:
	bash ./scripts/benchmark-c154-rollout.sh

.PHONY: test-c154-rollout
test-c154-rollout:
	bash ./scripts/test-c154-rollout.sh

.PHONY: test-c154-race
test-c154-race:
	bash ./scripts/test-c154-race.sh

.PHONY: vet-c154-rollout
vet-c154-rollout:
	bash ./scripts/vet-c154-rollout.sh

.PHONY: test-c154-regression
test-c154-regression:
	bash ./scripts/test-c154-regression.sh

.PHONY: test-c154-all
test-c154-all:
	bash ./scripts/test-c154-all.sh

.PHONY: format-c154
format-c154:
	bash ./scripts/format-c154.sh

.PHONY: commit-c154-rollout
commit-c154-rollout:
	bash ./scripts/commit-c154-rollout.sh
.PHONY: benchmark-c153
benchmark-c153:
	bash ./scripts/benchmark-c153.sh

.PHONY: test-c153
test-c153:
	bash ./scripts/test-c153.sh

.PHONY: format-c153
format-c153:
	bash ./scripts/format-c153.sh

.PHONY: race-c153
race-c153:
	bash ./scripts/race-c153.sh

.PHONY: vet-c153
vet-c153:
	bash ./scripts/vet-c153.sh

.PHONY: review-c153
review-c153:
	bash ./scripts/review-c153.sh

.PHONY: commit-c153
commit-c153:
	bash ./scripts/commit-c153.sh

.PHONY: benchmark-c154d
benchmark-c154d:
	bash ./scripts/benchmark-c154d.sh

.PHONY: test-c154d
test-c154d:
	bash ./scripts/test-c154d.sh

.PHONY: format-c154d
format-c154d:
	bash ./scripts/format-c154d.sh

.PHONY: race-c154d
race-c154d:
	bash ./scripts/race-c154d.sh

.PHONY: vet-c154d
vet-c154d:
	bash ./scripts/vet-c154d.sh

.PHONY: review-c154d
review-c154d:
	bash ./scripts/review-c154d.sh

.PHONY: commit-c154d
commit-c154d:
	bash ./scripts/commit-c154d.sh


.PHONY: inspect-timestamp-next
inspect-timestamp-next:
	bash ./scripts/inspect-timestamp-next.sh
.PHONY: benchmark-sql-snapshot-provider
benchmark-sql-snapshot-provider:
	bash ./scripts/benchmark-sql-snapshot-provider.sh
.PHONY: test-sql-snapshot-provider
test-sql-snapshot-provider:
	bash ./scripts/test-sql-snapshot-provider.sh
.PHONY: format-sql-snapshot-provider
format-sql-snapshot-provider:
	bash ./scripts/format-sql-snapshot-provider.sh
.PHONY: review-sql-snapshot-provider
review-sql-snapshot-provider:
	bash ./scripts/review-sql-snapshot-provider.sh
.PHONY: commit-sql-snapshot-provider
commit-sql-snapshot-provider:
	bash ./scripts/commit-sql-snapshot-provider.sh

.PHONY: push-sql-snapshot-provider
push-sql-snapshot-provider:
	bash ./scripts/push-sql-snapshot-provider.sh
.PHONY: benchmark-m065-rank-window
benchmark-m065-rank-window:
	bash ./scripts/benchmark-m065-rank-window.sh
.PHONY: test-m065-rank-window
test-m065-rank-window:
	bash ./scripts/test-m065-rank-window.sh
.PHONY: format-m065-rank-window
format-m065-rank-window:
	bash ./scripts/format-m065-rank-window.sh
.PHONY: review-m065-rank-window
review-m065-rank-window:
	bash ./scripts/review-m065-rank-window.sh
.PHONY: commit-m065-rank-window
commit-m065-rank-window:
	bash ./scripts/commit-m065-rank-window.sh

.PHONY: push-m065-rank-window
push-m065-rank-window:
	bash ./scripts/push-m065-rank-window.sh
.PHONY: benchmark-m064-recursive-reachability
benchmark-m064-recursive-reachability:
	bash ./scripts/benchmark-m064-recursive-reachability.sh
.PHONY: test-m064-recursive-reachability
test-m064-recursive-reachability:
	bash ./scripts/test-m064-recursive-reachability.sh
.PHONY: format-m064-recursive-reachability
format-m064-recursive-reachability:
	bash ./scripts/format-m064-recursive-reachability.sh

.PHONY: test-race-m064-recursive-reachability
test-race-m064-recursive-reachability:
	bash ./scripts/test-race-m064-recursive-reachability.sh
.PHONY: review-m064-recursive-reachability
review-m064-recursive-reachability:
	bash ./scripts/review-m064-recursive-reachability.sh
.PHONY: commit-m064-recursive-reachability
commit-m064-recursive-reachability:
	bash ./scripts/commit-m064-recursive-reachability.sh

.PHONY: push-m064-recursive-reachability
push-m064-recursive-reachability:
	bash ./scripts/push-m064-recursive-reachability.sh
.PHONY: commit-t042-rejection
commit-t042-rejection:
	bash ./scripts/commit-t042-rejection.sh

.PHONY: push-t042-rejection
push-t042-rejection:
	bash ./scripts/push-t042-rejection.sh
.PHONY: benchmark-m032-frontier
benchmark-m032-frontier:
	bash ./scripts/benchmark-m032-frontier.sh
.PHONY: test-m032-frontier
test-m032-frontier:
	bash ./scripts/test-m032-frontier.sh
.PHONY: format-m032-frontier
format-m032-frontier:
	bash ./scripts/format-m032-frontier.sh

.PHONY: test-race-m032-frontier
test-race-m032-frontier:
	bash ./scripts/test-race-m032-frontier.sh

.PHONY: review-m032-frontier
review-m032-frontier:
	bash ./scripts/review-m032-frontier.sh
.PHONY: commit-pipeline-cancellation
commit-pipeline-cancellation:
	bash ./scripts/commit-pipeline-cancellation.sh

.PHONY: push-pipeline-cancellation
push-pipeline-cancellation:
	bash ./scripts/push-pipeline-cancellation.sh
.PHONY: commit-m032-frontier
commit-m032-frontier:
	bash ./scripts/commit-m032-frontier.sh

.PHONY: push-m032-frontier
push-m032-frontier:
	bash ./scripts/push-m032-frontier.sh
.PHONY: benchmark-m033-global-timestamps
benchmark-m033-global-timestamps:
	bash ./scripts/benchmark-m033-global-timestamps.sh
.PHONY: test-m033-global-timestamps
test-m033-global-timestamps:
	bash ./scripts/test-m033-global-timestamps.sh
.PHONY: format-m033-global-timestamps
format-m033-global-timestamps:
	bash ./scripts/format-m033-global-timestamps.sh
.PHONY: test-race-m033-global-timestamps
test-race-m033-global-timestamps:
	bash ./scripts/test-race-m033-global-timestamps.sh
.PHONY: review-m033-global-timestamps
review-m033-global-timestamps:
	bash ./scripts/review-m033-global-timestamps.sh
.PHONY: commit-m033-global-timestamps
commit-m033-global-timestamps:
	bash ./scripts/commit-m033-global-timestamps.sh

.PHONY: push-m033-global-timestamps
push-m033-global-timestamps:
	bash ./scripts/push-m033-global-timestamps.sh
.PHONY: benchmark-m032c-frontier
benchmark-m032c-frontier:
	bash ./scripts/benchmark-m032c-frontier.sh
.PHONY: test-m032c-frontier
test-m032c-frontier:
	bash ./scripts/test-m032c-frontier.sh
.PHONY: format-m032c-frontier
format-m032c-frontier:
	bash ./scripts/format-m032c-frontier.sh
.PHONY: test-race-m032c-frontier
test-race-m032c-frontier:
	bash ./scripts/test-race-m032c-frontier.sh
.PHONY: review-m032c-frontier
review-m032c-frontier:
	bash ./scripts/review-m032c-frontier.sh
.PHONY: commit-m032c-frontier
commit-m032c-frontier:
	bash ./scripts/commit-m032c-frontier.sh
.PHONY: push-m032c-frontier
push-m032c-frontier:
	bash ./scripts/push-m032c-frontier.sh
.PHONY: benchmark-m032d-frontier-snapshot
benchmark-m032d-frontier-snapshot:
	bash ./scripts/benchmark-m032d-frontier-snapshot.sh
.PHONY: test-m032d-frontier-snapshot
test-m032d-frontier-snapshot:
	bash ./scripts/test-m032d-frontier-snapshot.sh
.PHONY: format-m032d-frontier-snapshot
format-m032d-frontier-snapshot:
	bash ./scripts/format-m032d-frontier-snapshot.sh
.PHONY: test-race-m032d-frontier-snapshot
test-race-m032d-frontier-snapshot:
	bash ./scripts/test-race-m032d-frontier-snapshot.sh
.PHONY: review-m032d-frontier-snapshot
review-m032d-frontier-snapshot:
	bash ./scripts/review-m032d-frontier-snapshot.sh
.PHONY: commit-m032d-frontier-snapshot
commit-m032d-frontier-snapshot:
	bash ./scripts/commit-m032d-frontier-snapshot.sh
.PHONY: push-m032d-frontier-snapshot
push-m032d-frontier-snapshot:
	bash ./scripts/push-m032d-frontier-snapshot.sh
.PHONY: test-m065-rank-window-mutations
test-m065-rank-window-mutations:
	bash ./scripts/test-m065-rank-window-mutations.sh
.PHONY: format-m065-rank-window-mutations
format-m065-rank-window-mutations:
	bash ./scripts/format-m065-rank-window-mutations.sh
.PHONY: benchmark-m065-rank-window-mutations
benchmark-m065-rank-window-mutations:
	bash ./scripts/benchmark-m065-rank-window-mutations.sh
.PHONY: test-m065-rank-window-all
test-m065-rank-window-all:
	bash ./scripts/test-m065-rank-window-all.sh

.PHONY: test-race-m065-rank-window-mutations
test-race-m065-rank-window-mutations:
	bash ./scripts/test-race-m065-rank-window-mutations.sh

.PHONY: vet-m065-rank-window-mutations
vet-m065-rank-window-mutations:
	bash ./scripts/vet-m065-rank-window-mutations.sh
.PHONY: review-m065-rank-window-mutations
review-m065-rank-window-mutations:
	bash ./scripts/review-m065-rank-window-mutations.sh
.PHONY: commit-m065-rank-window-mutations
commit-m065-rank-window-mutations:
	bash ./scripts/commit-m065-rank-window-mutations.sh

.PHONY: push-m065-rank-window-mutations
push-m065-rank-window-mutations:
	bash ./scripts/push-m065-rank-window-mutations.sh
.PHONY: test-m065c-incremental-offset-window
test-m065c-incremental-offset-window:
	bash ./scripts/test-m065c-incremental-offset-window.sh
.PHONY: format-m065c-incremental-offset-window
format-m065c-incremental-offset-window:
	bash ./scripts/format-m065c-incremental-offset-window.sh
.PHONY: test-race-m065c-incremental-offset-window
test-race-m065c-incremental-offset-window:
	bash ./scripts/test-race-m065c-incremental-offset-window.sh
.PHONY: vet-m065c-incremental-offset-window
vet-m065c-incremental-offset-window:
	bash ./scripts/vet-m065c-incremental-offset-window.sh
.PHONY: benchmark-m065c-incremental-offset-window
benchmark-m065c-incremental-offset-window:
	bash ./scripts/benchmark-m065c-incremental-offset-window.sh
.PHONY: review-m065c-incremental-offset-window
review-m065c-incremental-offset-window:
	bash ./scripts/review-m065c-incremental-offset-window.sh
.PHONY: commit-m065c-incremental-offset-window
commit-m065c-incremental-offset-window:
	bash ./scripts/commit-m065c-incremental-offset-window.sh
.PHONY: push-m065c-incremental-offset-window
push-m065c-incremental-offset-window:
	bash ./scripts/push-m065c-incremental-offset-window.sh
.PHONY: test-m065d-incremental-frame-window
test-m065d-incremental-frame-window:
	bash ./scripts/test-m065d-incremental-frame-window.sh

.PHONY: format-m065d-incremental-frame-window
format-m065d-incremental-frame-window:
	bash ./scripts/format-m065d-incremental-frame-window.sh

.PHONY: test-race-m065d-incremental-frame-window
test-race-m065d-incremental-frame-window:
	bash ./scripts/test-race-m065d-incremental-frame-window.sh

.PHONY: vet-m065d-incremental-frame-window
vet-m065d-incremental-frame-window:
	bash ./scripts/vet-m065d-incremental-frame-window.sh

.PHONY: benchmark-m065d-incremental-frame-window
benchmark-m065d-incremental-frame-window:
	bash ./scripts/benchmark-m065d-incremental-frame-window.sh

.PHONY: review-m065d-incremental-frame-window
review-m065d-incremental-frame-window:
	bash ./scripts/review-m065d-incremental-frame-window.sh

.PHONY: commit-m065d-incremental-frame-window
commit-m065d-incremental-frame-window:
	bash ./scripts/commit-m065d-incremental-frame-window.sh

.PHONY: push-m065d-incremental-frame-window
push-m065d-incremental-frame-window:
	bash ./scripts/push-m065d-incremental-frame-window.sh

.PHONY: test-m065e-incremental-boundary-window
test-m065e-incremental-boundary-window:
	bash ./scripts/test-m065e-incremental-boundary-window.sh

.PHONY: benchmark-m065e-incremental-boundary-window
benchmark-m065e-incremental-boundary-window:
	bash ./scripts/benchmark-m065e-incremental-boundary-window.sh

.PHONY: format-m065e-incremental-boundary-window
format-m065e-incremental-boundary-window:
	bash ./scripts/format-m065e-incremental-boundary-window.sh

.PHONY: test-race-m065e-incremental-boundary-window
test-race-m065e-incremental-boundary-window:
	bash ./scripts/test-race-m065e-incremental-boundary-window.sh

.PHONY: vet-m065e-incremental-boundary-window
vet-m065e-incremental-boundary-window:
	bash ./scripts/vet-m065e-incremental-boundary-window.sh

.PHONY: review-m065e-incremental-boundary-window
review-m065e-incremental-boundary-window:
	bash ./scripts/review-m065e-incremental-boundary-window.sh

.PHONY: commit-m065e-incremental-boundary-window
commit-m065e-incremental-boundary-window:
	bash ./scripts/commit-m065e-incremental-boundary-window.sh

.PHONY: push-m065e-incremental-boundary-window
push-m065e-incremental-boundary-window:
	bash ./scripts/push-m065e-incremental-boundary-window.sh

.PHONY: test-m065f-incremental-nth-value-window
test-m065f-incremental-nth-value-window:
	bash ./scripts/test-m065f-incremental-nth-value-window.sh

.PHONY: benchmark-m065f-incremental-nth-value-window
benchmark-m065f-incremental-nth-value-window:
	bash ./scripts/benchmark-m065f-incremental-nth-value-window.sh

.PHONY: format-m065f-incremental-nth-value-window
format-m065f-incremental-nth-value-window:
	bash ./scripts/format-m065f-incremental-nth-value-window.sh

.PHONY: test-race-m065f-incremental-nth-value-window
test-race-m065f-incremental-nth-value-window:
	bash ./scripts/test-race-m065f-incremental-nth-value-window.sh

.PHONY: vet-m065f-incremental-nth-value-window
vet-m065f-incremental-nth-value-window:
	bash ./scripts/vet-m065f-incremental-nth-value-window.sh

.PHONY: review-m065f-incremental-nth-value-window
review-m065f-incremental-nth-value-window:
	bash ./scripts/review-m065f-incremental-nth-value-window.sh

.PHONY: commit-m065f-incremental-nth-value-window
commit-m065f-incremental-nth-value-window:
	bash ./scripts/commit-m065f-incremental-nth-value-window.sh

.PHONY: push-m065f-incremental-nth-value-window
push-m065f-incremental-nth-value-window:
	bash ./scripts/push-m065f-incremental-nth-value-window.sh

.PHONY: test-m065g-incremental-extrema-frame-window
test-m065g-incremental-extrema-frame-window:
	bash ./scripts/test-m065g-incremental-extrema-frame-window.sh

.PHONY: review-m065g-incremental-extrema-frame-window
review-m065g-incremental-extrema-frame-window:
	bash ./scripts/review-m065g-incremental-extrema-frame-window.sh

.PHONY: commit-m065g-incremental-extrema-frame-window
commit-m065g-incremental-extrema-frame-window:
	bash ./scripts/commit-m065g-incremental-extrema-frame-window.sh

.PHONY: push-m065g-incremental-extrema-frame-window
push-m065g-incremental-extrema-frame-window:
	bash ./scripts/push-m065g-incremental-extrema-frame-window.sh

.PHONY: format-m065g-incremental-extrema-frame-window
format-m065g-incremental-extrema-frame-window:
	bash ./scripts/format-m065g-incremental-extrema-frame-window.sh

.PHONY: test-race-m065g-incremental-extrema-frame-window
test-race-m065g-incremental-extrema-frame-window:
	bash ./scripts/test-race-m065g-incremental-extrema-frame-window.sh

.PHONY: vet-m065g-incremental-extrema-frame-window
vet-m065g-incremental-extrema-frame-window:
	bash ./scripts/vet-m065g-incremental-extrema-frame-window.sh

.PHONY: benchmark-m065g-incremental-extrema-frame-window
benchmark-m065g-incremental-extrema-frame-window:
	bash ./scripts/benchmark-m065g-incremental-extrema-frame-window.sh

.PHONY: test-m065h-incremental-average-frame-window
test-m065h-incremental-average-frame-window:
	bash ./scripts/test-m065h-incremental-average-frame-window.sh

.PHONY: format-m065h-incremental-average-frame-window
format-m065h-incremental-average-frame-window:
	bash ./scripts/format-m065h-incremental-average-frame-window.sh

.PHONY: benchmark-m065h-incremental-average-frame-window
benchmark-m065h-incremental-average-frame-window:
	bash ./scripts/benchmark-m065h-incremental-average-frame-window.sh


.PHONY: test-m064-mutable-recursive-reachability
test-m064-mutable-recursive-reachability:
	bash ./scripts/test-m064-mutable-recursive-reachability.sh
.PHONY: format-m064-mutable-recursive-reachability
format-m064-mutable-recursive-reachability:
	bash ./scripts/format-m064-mutable-recursive-reachability.sh
.PHONY: benchmark-m064-mutable-recursive-reachability
benchmark-m064-mutable-recursive-reachability:
	bash ./scripts/benchmark-m064-mutable-recursive-reachability.sh
.PHONY: test-race-m064-mutable-recursive-reachability
test-race-m064-mutable-recursive-reachability:
	bash ./scripts/test-race-m064-mutable-recursive-reachability.sh

.PHONY: test-m064-reachability-all
test-m064-reachability-all:
	bash ./scripts/test-m064-reachability-all.sh

.PHONY: vet-m064-mutable-recursive-reachability
vet-m064-mutable-recursive-reachability:
	bash ./scripts/vet-m064-mutable-recursive-reachability.sh
.PHONY: review-m064-mutable-recursive-reachability
review-m064-mutable-recursive-reachability:
	bash ./scripts/review-m064-mutable-recursive-reachability.sh
.PHONY: commit-m064-mutable-recursive-reachability
commit-m064-mutable-recursive-reachability:
	bash ./scripts/commit-m064-mutable-recursive-reachability.sh
.PHONY: push-m064-mutable-recursive-reachability
push-m064-mutable-recursive-reachability:
	bash ./scripts/push-m064-mutable-recursive-reachability.sh
.PHONY: test-race-m065h-incremental-average-frame-window
test-race-m065h-incremental-average-frame-window:
	bash ./scripts/test-race-m065h-incremental-average-frame-window.sh

.PHONY: vet-m065h-incremental-average-frame-window
vet-m065h-incremental-average-frame-window:
	bash ./scripts/vet-m065h-incremental-average-frame-window.sh

.PHONY: review-m065h-incremental-average-frame-window
review-m065h-incremental-average-frame-window:
	bash ./scripts/review-m065h-incremental-average-frame-window.sh

.PHONY: commit-m065h-incremental-average-frame-window
commit-m065h-incremental-average-frame-window:
	bash ./scripts/commit-m065h-incremental-average-frame-window.sh

.PHONY: push-m065h-incremental-average-frame-window
push-m065h-incremental-average-frame-window:
	bash ./scripts/push-m065h-incremental-average-frame-window.sh
.PHONY: benchmark-m065i-incremental-distinct-frame-window
benchmark-m065i-incremental-distinct-frame-window:
	bash ./scripts/benchmark-m065i-incremental-distinct-frame-window.sh

.PHONY: format-m065i-incremental-distinct-frame-window
format-m065i-incremental-distinct-frame-window:
	bash ./scripts/format-m065i-incremental-distinct-frame-window.sh
.PHONY: test-m065i-incremental-distinct-frame-window
test-m065i-incremental-distinct-frame-window:
	bash ./scripts/test-m065i-incremental-distinct-frame-window.sh
.PHONY: test-race-m065i-incremental-distinct-frame-window
test-race-m065i-incremental-distinct-frame-window:
	bash ./scripts/test-race-m065i-incremental-distinct-frame-window.sh

.PHONY: vet-m065i-incremental-distinct-frame-window
vet-m065i-incremental-distinct-frame-window:
	bash ./scripts/vet-m065i-incremental-distinct-frame-window.sh

.PHONY: review-m065i-incremental-distinct-frame-window
review-m065i-incremental-distinct-frame-window:
	bash ./scripts/review-m065i-incremental-distinct-frame-window.sh

.PHONY: commit-m065i-incremental-distinct-frame-window
commit-m065i-incremental-distinct-frame-window:
	bash ./scripts/commit-m065i-incremental-distinct-frame-window.sh

.PHONY: push-m065i-incremental-distinct-frame-window
push-m065i-incremental-distinct-frame-window:
	bash ./scripts/push-m065i-incremental-distinct-frame-window.sh
.PHONY: review-m065j-rejected-small-distinct
review-m065j-rejected-small-distinct:
	bash ./scripts/review-m065j-rejected-small-distinct.sh

.PHONY: commit-m065j-rejected-small-distinct
commit-m065j-rejected-small-distinct:
	bash ./scripts/commit-m065j-rejected-small-distinct.sh

.PHONY: push-m065j-rejected-small-distinct
push-m065j-rejected-small-distinct:
	bash ./scripts/push-m065j-rejected-small-distinct.sh
.PHONY: test-m065k-distinct-snapshot-atomicity
test-m065k-distinct-snapshot-atomicity:
	bash ./scripts/test-m065k-distinct-snapshot-atomicity.sh
.PHONY: format-m065k-distinct-snapshot-atomicity
format-m065k-distinct-snapshot-atomicity:
	bash ./scripts/format-m065k-distinct-snapshot-atomicity.sh
.PHONY: test-race-m065k-distinct-snapshot-atomicity
test-race-m065k-distinct-snapshot-atomicity:
	bash ./scripts/test-race-m065k-distinct-snapshot-atomicity.sh

.PHONY: vet-m065k-distinct-snapshot-atomicity
vet-m065k-distinct-snapshot-atomicity:
	bash ./scripts/vet-m065k-distinct-snapshot-atomicity.sh

.PHONY: review-m065k-distinct-snapshot-atomicity
review-m065k-distinct-snapshot-atomicity:
	bash ./scripts/review-m065k-distinct-snapshot-atomicity.sh

.PHONY: commit-m065k-distinct-snapshot-atomicity
commit-m065k-distinct-snapshot-atomicity:
	bash ./scripts/commit-m065k-distinct-snapshot-atomicity.sh

.PHONY: push-m065k-distinct-snapshot-atomicity
push-m065k-distinct-snapshot-atomicity:
	bash ./scripts/push-m065k-distinct-snapshot-atomicity.sh
.PHONY: test-m052b-dataflow-fragments
test-m052b-dataflow-fragments:
	bash ./scripts/test-m052b-dataflow-fragments.sh

.PHONY: format-m052b-dataflow-fragments
format-m052b-dataflow-fragments:
	bash ./scripts/format-m052b-dataflow-fragments.sh

.PHONY: benchmark-m052b-dataflow-fragments
benchmark-m052b-dataflow-fragments:
	bash ./scripts/benchmark-m052b-dataflow-fragments.sh

.PHONY: test-race-m052b-dataflow-fragments
test-race-m052b-dataflow-fragments:
	bash ./scripts/test-race-m052b-dataflow-fragments.sh

.PHONY: vet-m052b-dataflow-fragments
vet-m052b-dataflow-fragments:
	bash ./scripts/vet-m052b-dataflow-fragments.sh

.PHONY: review-m052b-dataflow-fragments
review-m052b-dataflow-fragments:
	bash ./scripts/review-m052b-dataflow-fragments.sh

.PHONY: commit-m052b-dataflow-fragments
commit-m052b-dataflow-fragments:
	bash ./scripts/commit-m052b-dataflow-fragments.sh

.PHONY: push-m052b-dataflow-fragments
push-m052b-dataflow-fragments:
	bash ./scripts/push-m052b-dataflow-fragments.sh

.PHONY: format-m065l-mutable-frame
format-m065l-mutable-frame:
	bash ./scripts/format-m065l-mutable-frame.sh

.PHONY: test-m065l-mutable-frame
test-m065l-mutable-frame:
	bash ./scripts/test-m065l-mutable-frame.sh

.PHONY: benchmark-m065l-mutable-frame
benchmark-m065l-mutable-frame:
	bash ./scripts/benchmark-m065l-mutable-frame.sh

.PHONY: test-race-m065l-mutable-frame
test-race-m065l-mutable-frame:
	bash ./scripts/test-race-m065l-mutable-frame.sh

.PHONY: vet-m065l-mutable-frame
vet-m065l-mutable-frame:
	bash ./scripts/vet-m065l-mutable-frame.sh

.PHONY: review-m065l-mutable-frame
review-m065l-mutable-frame:
	bash ./scripts/review-m065l-mutable-frame.sh

.PHONY: commit-m065l-mutable-frame
commit-m065l-mutable-frame:
	bash ./scripts/commit-m065l-mutable-frame.sh

.PHONY: push-m065l-mutable-frame
push-m065l-mutable-frame:
	bash ./scripts/push-m065l-mutable-frame.sh

.PHONY: test-m071-compiled-template-reuse
test-m071-compiled-template-reuse:
	bash ./scripts/test-m071-compiled-template-reuse.sh

.PHONY: benchmark-m071-compiled-template-reuse
benchmark-m071-compiled-template-reuse:
	bash ./scripts/benchmark-m071-compiled-template-reuse.sh

.PHONY: format-m071-compiled-template-reuse
format-m071-compiled-template-reuse:
	bash ./scripts/format-m071-compiled-template-reuse.sh

.PHONY: test-race-m071-compiled-template-reuse
test-race-m071-compiled-template-reuse:
	bash ./scripts/test-race-m071-compiled-template-reuse.sh

.PHONY: vet-m071-compiled-template-reuse
vet-m071-compiled-template-reuse:
	bash ./scripts/vet-m071-compiled-template-reuse.sh

.PHONY: review-m071-compiled-template-reuse
review-m071-compiled-template-reuse:
	sh ./scripts/review-m071-compiled-template-reuse.sh

.PHONY: commit-m071-compiled-template-reuse
commit-m071-compiled-template-reuse:
	bash ./scripts/commit-m071-compiled-template-reuse.sh

.PHONY: amend-m071-compiled-template-reuse
amend-m071-compiled-template-reuse:
	M071_AMEND=1 bash ./scripts/commit-m071-compiled-template-reuse.sh

.PHONY: push-m071-compiled-template-reuse
push-m071-compiled-template-reuse:
	bash ./scripts/push-m071-compiled-template-reuse.sh

.PHONY: audit-inspiration-now
audit-inspiration-now:
	bash ./scripts/audit-inspiration-now.sh

.PHONY: benchmark-m065m-incremental-range-window
benchmark-m065m-incremental-range-window:
	bash ./scripts/benchmark-m065m-incremental-range-window.sh

.PHONY: test-m065m-incremental-range-window
test-m065m-incremental-range-window:
	bash ./scripts/test-m065m-incremental-range-window.sh

.PHONY: format-m065m-incremental-range-window
format-m065m-incremental-range-window:
	bash ./scripts/format-m065m-incremental-range-window.sh

.PHONY: test-race-m065m-incremental-range-window
test-race-m065m-incremental-range-window:
	bash ./scripts/test-race-m065m-incremental-range-window.sh

.PHONY: vet-m065m-incremental-range-window
vet-m065m-incremental-range-window:
	bash ./scripts/vet-m065m-incremental-range-window.sh

.PHONY: review-m065m-incremental-range-window
review-m065m-incremental-range-window:
	bash ./scripts/review-m065m-incremental-range-window.sh

.PHONY: commit-m065m-incremental-range-window
commit-m065m-incremental-range-window:
	bash ./scripts/commit-m065m-incremental-range-window.sh

.PHONY: push-m065m-incremental-range-window
push-m065m-incremental-range-window:
	bash ./scripts/push-m065m-incremental-range-window.sh

.PHONY: test-m065n-incremental-range-window
test-m065n-incremental-range-window:
	bash ./scripts/test-m065n-incremental-range-window.sh

.PHONY: benchmark-m065n-incremental-range-window
benchmark-m065n-incremental-range-window:
	bash ./scripts/benchmark-m065n-incremental-range-window.sh

.PHONY: format-m065n-incremental-range-window
format-m065n-incremental-range-window:
	bash ./scripts/format-m065n-incremental-range-window.sh

.PHONY: test-race-m065n-incremental-range-window
test-race-m065n-incremental-range-window:
	bash ./scripts/test-race-m065n-incremental-range-window.sh

.PHONY: vet-m065n-incremental-range-window
vet-m065n-incremental-range-window:
	bash ./scripts/vet-m065n-incremental-range-window.sh

.PHONY: review-m065n-incremental-range-window
review-m065n-incremental-range-window:
	bash ./scripts/review-m065n-incremental-range-window.sh

.PHONY: commit-m065n-incremental-range-window
commit-m065n-incremental-range-window:
	bash ./scripts/commit-m065n-incremental-range-window.sh

.PHONY: push-m065n-incremental-range-window
push-m065n-incremental-range-window:
	bash ./scripts/push-m065n-incremental-range-window.sh

.PHONY: inspect-sql-window-ideas-now
inspect-sql-window-ideas-now:
	bash ./scripts/inspect-sql-window-ideas-now.sh

.PHONY: benchmark-m065o-incremental-range-distinct
benchmark-m065o-incremental-range-distinct:
	bash ./scripts/benchmark-m065o-incremental-range-distinct.sh

.PHONY: test-m065o-incremental-range-distinct
test-m065o-incremental-range-distinct:
	bash ./scripts/test-m065o-incremental-range-distinct.sh

.PHONY: format-m065o-incremental-range-distinct
format-m065o-incremental-range-distinct:
	bash ./scripts/format-m065o-incremental-range-distinct.sh

.PHONY: test-race-m065o-incremental-range-distinct
test-race-m065o-incremental-range-distinct:
	bash ./scripts/test-race-m065o-incremental-range-distinct.sh

.PHONY: vet-m065o-incremental-range-distinct
vet-m065o-incremental-range-distinct:
	bash ./scripts/vet-m065o-incremental-range-distinct.sh

.PHONY: review-m065o-incremental-range-distinct
review-m065o-incremental-range-distinct:
	bash ./scripts/review-m065o-incremental-range-distinct.sh

.PHONY: commit-m065o-incremental-range-distinct
commit-m065o-incremental-range-distinct:
	bash ./scripts/commit-m065o-incremental-range-distinct.sh

.PHONY: push-m065o-incremental-range-distinct
push-m065o-incremental-range-distinct:
	bash ./scripts/push-m065o-incremental-range-distinct.sh

.PHONY: benchmark-m065p-incremental-range-average
benchmark-m065p-incremental-range-average:
	bash ./scripts/benchmark-m065p-incremental-range-average.sh

.PHONY: test-m065p-incremental-range-average
test-m065p-incremental-range-average:
	bash ./scripts/test-m065p-incremental-range-average.sh

.PHONY: format-m065p-incremental-range-average
format-m065p-incremental-range-average:
	bash ./scripts/format-m065p-incremental-range-average.sh

.PHONY: test-race-m065p-incremental-range-average
test-race-m065p-incremental-range-average:
	bash ./scripts/test-race-m065p-incremental-range-average.sh

.PHONY: vet-m065p-incremental-range-average
vet-m065p-incremental-range-average:
	bash ./scripts/vet-m065p-incremental-range-average.sh

.PHONY: review-m065p-incremental-range-average
review-m065p-incremental-range-average:
	bash ./scripts/review-m065p-incremental-range-average.sh

.PHONY: commit-m065p-incremental-range-average
commit-m065p-incremental-range-average:
	bash ./scripts/commit-m065p-incremental-range-average.sh

.PHONY: push-m065p-incremental-range-average
push-m065p-incremental-range-average:
	bash ./scripts/push-m065p-incremental-range-average.sh

.PHONY: benchmark-m065q-incremental-range-boundary
benchmark-m065q-incremental-range-boundary:
	bash ./scripts/benchmark-m065q-incremental-range-boundary.sh

.PHONY: test-m065q-incremental-range-boundary
test-m065q-incremental-range-boundary:
	bash ./scripts/test-m065q-incremental-range-boundary.sh

.PHONY: format-m065q-incremental-range-boundary
format-m065q-incremental-range-boundary:
	bash ./scripts/format-m065q-incremental-range-boundary.sh

.PHONY: test-race-m065q-incremental-range-boundary
test-race-m065q-incremental-range-boundary:
	bash ./scripts/test-race-m065q-incremental-range-boundary.sh

.PHONY: vet-m065q-incremental-range-boundary
vet-m065q-incremental-range-boundary:
	bash ./scripts/vet-m065q-incremental-range-boundary.sh

.PHONY: review-m065q-incremental-range-boundary
review-m065q-incremental-range-boundary:
	bash ./scripts/review-m065q-incremental-range-boundary.sh

.PHONY: commit-m065q-incremental-range-boundary
commit-m065q-incremental-range-boundary:
	bash ./scripts/commit-m065q-incremental-range-boundary.sh

.PHONY: push-m065q-incremental-range-boundary
push-m065q-incremental-range-boundary:
	bash ./scripts/push-m065q-incremental-range-boundary.sh
.PHONY: benchmark-m065r-incremental-range-nth-value
benchmark-m065r-incremental-range-nth-value:
	bash ./scripts/benchmark-m065r-incremental-range-nth-value.sh

.PHONY: test-m065r-incremental-range-nth-value
test-m065r-incremental-range-nth-value:
	bash ./scripts/test-m065r-incremental-range-nth-value.sh

.PHONY: format-m065r-incremental-range-nth-value
format-m065r-incremental-range-nth-value:
	bash ./scripts/format-m065r-incremental-range-nth-value.sh

.PHONY: test-race-m065r-incremental-range-nth-value
test-race-m065r-incremental-range-nth-value:
	bash ./scripts/test-race-m065r-incremental-range-nth-value.sh

.PHONY: vet-m065r-incremental-range-nth-value
vet-m065r-incremental-range-nth-value:
	bash ./scripts/vet-m065r-incremental-range-nth-value.sh

.PHONY: review-m065r-incremental-range-nth-value
review-m065r-incremental-range-nth-value:
	bash ./scripts/review-m065r-incremental-range-nth-value.sh

.PHONY: commit-m065r-incremental-range-nth-value
commit-m065r-incremental-range-nth-value:
	bash ./scripts/commit-m065r-incremental-range-nth-value.sh

.PHONY: push-m065r-incremental-range-nth-value
push-m065r-incremental-range-nth-value:
	bash ./scripts/push-m065r-incremental-range-nth-value.sh

.PHONY: benchmark-differential-except
benchmark-differential-except:
	bash ./scripts/benchmark-differential-except.sh

.PHONY: test-differential-except
test-differential-except:
	bash ./scripts/test-differential-except.sh

.PHONY: format-differential-except
format-differential-except:
	bash ./scripts/format-differential-except.sh

.PHONY: verify-differential-except
verify-differential-except:
	bash ./scripts/verify-differential-except.sh

.PHONY: commit-differential-except
commit-differential-except:
	bash ./scripts/commit-differential-except.sh

.PHONY: benchmark-differential-intersect
benchmark-differential-intersect:
	bash ./scripts/benchmark-differential-intersect.sh

.PHONY: test-differential-intersect
test-differential-intersect:
	bash ./scripts/test-differential-intersect.sh

.PHONY: format-differential-intersect
format-differential-intersect:
	bash ./scripts/format-differential-intersect.sh

.PHONY: verify-differential-intersect
verify-differential-intersect:
	bash ./scripts/verify-differential-intersect.sh

.PHONY: commit-differential-intersect
commit-differential-intersect:
	bash ./scripts/commit-differential-intersect.sh

.PHONY: benchmark-sql-set-operation-all
benchmark-sql-set-operation-all:
	bash ./scripts/benchmark-sql-set-operation-all.sh

.PHONY: commit-sql-set-operation-all
commit-sql-set-operation-all:
	bash ./scripts/commit-sql-set-operation-all.sh

.PHONY: benchmark-differential-group-min-max
benchmark-differential-group-min-max:
	bash ./scripts/benchmark-differential-group-min-max.sh

.PHONY: test-differential-group-min-max
test-differential-group-min-max:
	bash ./scripts/test-differential-group-min-max.sh

.PHONY: format-differential-group-min-max
format-differential-group-min-max:
	bash ./scripts/format-differential-group-min-max.sh

.PHONY: verify-differential-group-min-max
verify-differential-group-min-max:
	bash ./scripts/verify-differential-group-min-max.sh

.PHONY: commit-differential-group-min-max
commit-differential-group-min-max:
	bash ./scripts/commit-differential-group-min-max.sh

.PHONY: benchmark-partition-ownership-consensus
benchmark-partition-ownership-consensus:
	bash ./scripts/benchmark-partition-ownership-consensus.sh

.PHONY: test-partition-ownership-consensus
test-partition-ownership-consensus:
	bash ./scripts/test-partition-ownership-consensus.sh

.PHONY: format-partition-ownership-consensus
format-partition-ownership-consensus:
	bash ./scripts/format-partition-ownership-consensus.sh

.PHONY: verify-partition-ownership-consensus
verify-partition-ownership-consensus:
	bash ./scripts/verify-partition-ownership-consensus.sh

.PHONY: commit-partition-ownership-consensus
commit-partition-ownership-consensus:
	bash ./scripts/commit-partition-ownership-consensus.sh



.PHONY: test-sql-set-operation-all
test-sql-set-operation-all:
	bash ./scripts/test-sql-set-operation-all.sh


.PHONY: format-sql-set-operation-all
format-sql-set-operation-all:
	bash ./scripts/format-sql-set-operation-all.sh

.PHONY: verify-sql-set-operation-all
verify-sql-set-operation-all:
	bash ./scripts/verify-sql-set-operation-all.sh
.PHONY: inspect-goal-state
inspect-goal-state:
	bash ./scripts/inspect-goal-state.sh
.PHONY: test-m052c-native-dataflow
test-m052c-native-dataflow:
	bash ./scripts/test-m052c-native-dataflow.sh

.PHONY: benchmark-m052c-native-dataflow
benchmark-m052c-native-dataflow:
	bash ./scripts/benchmark-m052c-native-dataflow.sh

.PHONY: format-m052c-native-dataflow
format-m052c-native-dataflow:
	bash ./scripts/format-m052c-native-dataflow.sh

.PHONY: test-race-m052c-native-dataflow
test-race-m052c-native-dataflow:
	bash ./scripts/test-race-m052c-native-dataflow.sh

.PHONY: vet-m052c-native-dataflow
vet-m052c-native-dataflow:
	bash ./scripts/vet-m052c-native-dataflow.sh

.PHONY: review-m052c-native-dataflow
review-m052c-native-dataflow:
	bash ./scripts/review-m052c-native-dataflow.sh

.PHONY: commit-m052c-native-dataflow
commit-m052c-native-dataflow:
	bash ./scripts/commit-m052c-native-dataflow.sh

.PHONY: push-m052c-native-dataflow
push-m052c-native-dataflow:
	bash ./scripts/push-m052c-native-dataflow.sh

.PHONY: test-m052d-native-aggregate
test-m052d-native-aggregate:
	bash ./scripts/test-m052d-native-aggregate.sh

.PHONY: benchmark-m052d-native-aggregate
benchmark-m052d-native-aggregate:
	bash ./scripts/benchmark-m052d-native-aggregate.sh

.PHONY: format-m052d-native-aggregate
format-m052d-native-aggregate:
	bash ./scripts/format-m052d-native-aggregate.sh
.PHONY: test-race-m052d-native-aggregate
test-race-m052d-native-aggregate:
	bash ./scripts/test-race-m052d-native-aggregate.sh

.PHONY: vet-m052d-native-aggregate
vet-m052d-native-aggregate:
	bash ./scripts/vet-m052d-native-aggregate.sh

.PHONY: review-m052d-native-aggregate
review-m052d-native-aggregate:
	bash ./scripts/review-m052d-native-aggregate.sh

.PHONY: commit-m052d-native-aggregate
commit-m052d-native-aggregate:
	bash ./scripts/commit-m052d-native-aggregate.sh

.PHONY: push-m052d-native-aggregate
push-m052d-native-aggregate:
	bash ./scripts/push-m052d-native-aggregate.sh

.PHONY: test-m052e-native-group
test-m052e-native-group:
	bash ./scripts/test-m052e-native-group.sh

.PHONY: benchmark-m052e-native-group
benchmark-m052e-native-group:
	bash ./scripts/benchmark-m052e-native-group.sh

.PHONY: format-m052e-native-group
format-m052e-native-group:
	bash ./scripts/format-m052e-native-group.sh

.PHONY: test-race-m052e-native-group
test-race-m052e-native-group:
	bash ./scripts/test-race-m052e-native-group.sh

.PHONY: vet-m052e-native-group
vet-m052e-native-group:
	bash ./scripts/vet-m052e-native-group.sh

.PHONY: review-m052e-native-group
review-m052e-native-group:
	bash ./scripts/review-m052e-native-group.sh

.PHONY: commit-m052e-native-group
commit-m052e-native-group:
	bash ./scripts/commit-m052e-native-group.sh

.PHONY: push-m052e-native-group
push-m052e-native-group:
	bash ./scripts/push-m052e-native-group.sh

.PHONY: test-m052f-native-distinct
test-m052f-native-distinct:
	bash ./scripts/test-m052f-native-distinct.sh

.PHONY: benchmark-m052f-native-distinct
benchmark-m052f-native-distinct:
	bash ./scripts/benchmark-m052f-native-distinct.sh

.PHONY: format-m052f-native-distinct
format-m052f-native-distinct:
	bash ./scripts/format-m052f-native-distinct.sh

.PHONY: test-race-m052f-native-distinct
test-race-m052f-native-distinct:
	bash ./scripts/test-race-m052f-native-distinct.sh

.PHONY: vet-m052f-native-distinct
vet-m052f-native-distinct:
	bash ./scripts/vet-m052f-native-distinct.sh

.PHONY: review-m052f-native-distinct
review-m052f-native-distinct:
	bash ./scripts/review-m052f-native-distinct.sh

.PHONY: commit-m052f-native-distinct
commit-m052f-native-distinct:
	bash ./scripts/commit-m052f-native-distinct.sh

.PHONY: push-m052f-native-distinct
push-m052f-native-distinct:
	bash ./scripts/push-m052f-native-distinct.sh

.PHONY: test-m052g-native-limit
test-m052g-native-limit:
	bash ./scripts/test-m052g-native-limit.sh

.PHONY: benchmark-m052g-native-limit
benchmark-m052g-native-limit:
	bash ./scripts/benchmark-m052g-native-limit.sh

.PHONY: format-m052g-native-limit
format-m052g-native-limit:
	bash ./scripts/format-m052g-native-limit.sh

.PHONY: test-race-m052g-native-limit
test-race-m052g-native-limit:
	bash ./scripts/test-race-m052g-native-limit.sh

.PHONY: vet-m052g-native-limit
vet-m052g-native-limit:
	bash ./scripts/vet-m052g-native-limit.sh

.PHONY: review-m052g-native-limit
review-m052g-native-limit:
	bash ./scripts/review-m052g-native-limit.sh

.PHONY: commit-m052g-native-limit
commit-m052g-native-limit:
	bash ./scripts/commit-m052g-native-limit.sh

.PHONY: push-m052g-native-limit
push-m052g-native-limit:
	bash ./scripts/push-m052g-native-limit.sh

.PHONY: test-m052h-native-ordered-limit
test-m052h-native-ordered-limit:
	bash ./scripts/test-m052h-native-ordered-limit.sh

.PHONY: benchmark-m052h-native-ordered-limit
benchmark-m052h-native-ordered-limit:
	bash ./scripts/benchmark-m052h-native-ordered-limit.sh

.PHONY: format-m052h-native-ordered-limit
format-m052h-native-ordered-limit:
	bash ./scripts/format-m052h-native-ordered-limit.sh

.PHONY: test-race-m052h-native-ordered-limit
test-race-m052h-native-ordered-limit:
	bash ./scripts/test-race-m052h-native-ordered-limit.sh

.PHONY: vet-m052h-native-ordered-limit
vet-m052h-native-ordered-limit:
	bash ./scripts/vet-m052h-native-ordered-limit.sh

.PHONY: review-m052h-native-ordered-limit
review-m052h-native-ordered-limit:
	bash ./scripts/review-m052h-native-ordered-limit.sh

.PHONY: commit-m052h-native-ordered-limit
commit-m052h-native-ordered-limit:
	bash ./scripts/commit-m052h-native-ordered-limit.sh

.PHONY: push-m052h-native-ordered-limit
push-m052h-native-ordered-limit:
	bash ./scripts/push-m052h-native-ordered-limit.sh

.PHONY: test-m052i-native-composite-ordered-limit
test-m052i-native-composite-ordered-limit:
	bash ./scripts/test-m052i-native-composite-ordered-limit.sh

.PHONY: benchmark-m052i-native-composite-ordered-limit
benchmark-m052i-native-composite-ordered-limit:
	bash ./scripts/benchmark-m052i-native-composite-ordered-limit.sh

.PHONY: format-m052i-native-composite-ordered-limit
format-m052i-native-composite-ordered-limit:
	bash ./scripts/format-m052i-native-composite-ordered-limit.sh

.PHONY: test-race-m052i-native-composite-ordered-limit
test-race-m052i-native-composite-ordered-limit:
	bash ./scripts/test-race-m052i-native-composite-ordered-limit.sh

.PHONY: vet-m052i-native-composite-ordered-limit
vet-m052i-native-composite-ordered-limit:
	bash ./scripts/vet-m052i-native-composite-ordered-limit.sh

.PHONY: review-m052i-native-composite-ordered-limit
review-m052i-native-composite-ordered-limit:
	bash ./scripts/review-m052i-native-composite-ordered-limit.sh

.PHONY: commit-m052i-native-composite-ordered-limit
commit-m052i-native-composite-ordered-limit:
	bash ./scripts/commit-m052i-native-composite-ordered-limit.sh

.PHONY: push-m052i-native-composite-ordered-limit
push-m052i-native-composite-ordered-limit:
	bash ./scripts/push-m052i-native-composite-ordered-limit.sh

.PHONY: inspect-query-engine-backlog
inspect-query-engine-backlog:
	bash ./scripts/inspect-query-engine-backlog.sh

.PHONY: test-m052j-native-grouped-ordered-limit
test-m052j-native-grouped-ordered-limit:
	bash ./scripts/test-m052j-native-grouped-ordered-limit.sh

.PHONY: benchmark-m052j-native-grouped-ordered-limit
benchmark-m052j-native-grouped-ordered-limit:
	bash ./scripts/benchmark-m052j-native-grouped-ordered-limit.sh

.PHONY: format-m052j-native-grouped-ordered-limit
format-m052j-native-grouped-ordered-limit:
	bash ./scripts/format-m052j-native-grouped-ordered-limit.sh

.PHONY: test-race-m052j-native-grouped-ordered-limit
test-race-m052j-native-grouped-ordered-limit:
	bash ./scripts/test-race-m052j-native-grouped-ordered-limit.sh

.PHONY: vet-m052j-native-grouped-ordered-limit
vet-m052j-native-grouped-ordered-limit:
	bash ./scripts/vet-m052j-native-grouped-ordered-limit.sh

.PHONY: review-m052j-native-grouped-ordered-limit
review-m052j-native-grouped-ordered-limit:
	bash ./scripts/review-m052j-native-grouped-ordered-limit.sh

.PHONY: commit-m052j-native-grouped-ordered-limit
commit-m052j-native-grouped-ordered-limit:
	bash ./scripts/commit-m052j-native-grouped-ordered-limit.sh

.PHONY: push-m052j-native-grouped-ordered-limit
push-m052j-native-grouped-ordered-limit:
	bash ./scripts/push-m052j-native-grouped-ordered-limit.sh

.PHONY: test-m052k-native-grouped-having
test-m052k-native-grouped-having:
	bash ./scripts/test-m052k-native-grouped-having.sh

.PHONY: benchmark-m052k-native-grouped-having
benchmark-m052k-native-grouped-having:
	bash ./scripts/benchmark-m052k-native-grouped-having.sh

.PHONY: format-m052k-native-grouped-having
format-m052k-native-grouped-having:
	bash ./scripts/format-m052k-native-grouped-having.sh

.PHONY: test-race-m052k-native-grouped-having
test-race-m052k-native-grouped-having:
	bash ./scripts/test-race-m052k-native-grouped-having.sh

.PHONY: vet-m052k-native-grouped-having
vet-m052k-native-grouped-having:
	bash ./scripts/vet-m052k-native-grouped-having.sh

.PHONY: review-m052k-native-grouped-having
review-m052k-native-grouped-having:
	bash ./scripts/review-m052k-native-grouped-having.sh

.PHONY: commit-m052k-native-grouped-having
commit-m052k-native-grouped-having:
	bash ./scripts/commit-m052k-native-grouped-having.sh

.PHONY: push-m052k-native-grouped-having
push-m052k-native-grouped-having:
	bash ./scripts/push-m052k-native-grouped-having.sh
.PHONY: test-m052l-native-string-group benchmark-m052l-native-string-group benchmark-m052l-native-string-group-baseline format-m052l-native-string-group test-race-m052l-native-string-group vet-m052l-native-string-group review-m052l-native-string-group commit-m052l-native-string-group push-m052l-native-string-group
test-m052l-native-string-group:
	bash ./scripts/test-m052l-native-string-group.sh

benchmark-m052l-native-string-group:
	bash ./scripts/benchmark-m052l-native-string-group.sh

benchmark-m052l-native-string-group-baseline:
	bash ./scripts/benchmark-m052l-native-string-group-baseline.sh

format-m052l-native-string-group:
	bash ./scripts/format-m052l-native-string-group.sh

test-race-m052l-native-string-group:
	bash ./scripts/test-race-m052l-native-string-group.sh

vet-m052l-native-string-group:
	bash ./scripts/vet-m052l-native-string-group.sh

review-m052l-native-string-group:
	bash ./scripts/review-m052l-native-string-group.sh

commit-m052l-native-string-group:
	bash ./scripts/commit-m052l-native-string-group.sh

push-m052l-native-string-group:
	bash ./scripts/push-m052l-native-string-group.sh
.PHONY: test-m052m-native-string-distinct benchmark-m052m-native-string-distinct benchmark-m052m-native-string-distinct-baseline format-m052m-native-string-distinct
test-m052m-native-string-distinct:
	bash ./scripts/test-m052m-native-string-distinct.sh

benchmark-m052m-native-string-distinct:
	bash ./scripts/benchmark-m052m-native-string-distinct.sh

benchmark-m052m-native-string-distinct-baseline:
	bash ./scripts/benchmark-m052m-native-string-distinct-baseline.sh

format-m052m-native-string-distinct:
	bash ./scripts/format-m052m-native-string-distinct.sh
.PHONY: test-race-m052m-native-string-distinct vet-m052m-native-string-distinct review-m052m-native-string-distinct commit-m052m-native-string-distinct push-m052m-native-string-distinct
test-race-m052m-native-string-distinct:
	bash ./scripts/test-race-m052m-native-string-distinct.sh

vet-m052m-native-string-distinct:
	bash ./scripts/vet-m052m-native-string-distinct.sh

review-m052m-native-string-distinct:
	bash ./scripts/review-m052m-native-string-distinct.sh

commit-m052m-native-string-distinct:
	bash ./scripts/commit-m052m-native-string-distinct.sh

push-m052m-native-string-distinct:
	bash ./scripts/push-m052m-native-string-distinct.sh
.PHONY: test-m052n-native-composite-distinct benchmark-m052n-native-composite-distinct benchmark-m052n-native-composite-distinct-baseline format-m052n-native-composite-distinct test-race-m052n-native-composite-distinct vet-m052n-native-composite-distinct review-m052n-native-composite-distinct commit-m052n-native-composite-distinct push-m052n-native-composite-distinct
test-m052n-native-composite-distinct:
	bash ./scripts/test-m052n-native-composite-distinct.sh

benchmark-m052n-native-composite-distinct:
	bash ./scripts/benchmark-m052n-native-composite-distinct.sh

benchmark-m052n-native-composite-distinct-baseline:
	bash ./scripts/benchmark-m052n-native-composite-distinct-baseline.sh

format-m052n-native-composite-distinct:
	bash ./scripts/format-m052n-native-composite-distinct.sh

test-race-m052n-native-composite-distinct:
	bash ./scripts/test-race-m052n-native-composite-distinct.sh

vet-m052n-native-composite-distinct:
	bash ./scripts/vet-m052n-native-composite-distinct.sh

review-m052n-native-composite-distinct:
	bash ./scripts/review-m052n-native-composite-distinct.sh

commit-m052n-native-composite-distinct:
	bash ./scripts/commit-m052n-native-composite-distinct.sh

push-m052n-native-composite-distinct:
	bash ./scripts/push-m052n-native-composite-distinct.sh
.PHONY: test-m052o-native-composite-group benchmark-m052o-native-composite-group benchmark-m052o-native-composite-group-baseline format-m052o-native-composite-group test-race-m052o-native-composite-group vet-m052o-native-composite-group review-m052o-native-composite-group commit-m052o-native-composite-group push-m052o-native-composite-group
test-m052o-native-composite-group:
	bash ./scripts/test-m052o-native-composite-group.sh

benchmark-m052o-native-composite-group:
	bash ./scripts/benchmark-m052o-native-composite-group.sh

benchmark-m052o-native-composite-group-baseline:
	bash ./scripts/benchmark-m052o-native-composite-group-baseline.sh

format-m052o-native-composite-group:
	bash ./scripts/format-m052o-native-composite-group.sh

test-race-m052o-native-composite-group:
	bash ./scripts/test-race-m052o-native-composite-group.sh

vet-m052o-native-composite-group:
	bash ./scripts/vet-m052o-native-composite-group.sh

review-m052o-native-composite-group:
	bash ./scripts/review-m052o-native-composite-group.sh

commit-m052o-native-composite-group:
	bash ./scripts/commit-m052o-native-composite-group.sh

push-m052o-native-composite-group:
	bash ./scripts/push-m052o-native-composite-group.sh
.PHONY: review-rejected-t042-parallel-replay
review-rejected-t042-parallel-replay:
	bash ./scripts/review-rejected-t042-parallel-replay.sh

.PHONY: commit-rejected-t042-parallel-replay
commit-rejected-t042-parallel-replay:
	bash ./scripts/commit-rejected-t042-parallel-replay.sh

.PHONY: push-rejected-t042-parallel-replay
push-rejected-t042-parallel-replay:
	bash ./scripts/push-rejected-t042-parallel-replay.sh

.PHONY: inspect-native-dataflow
inspect-native-dataflow:
	@bash ./scripts/inspect-native-dataflow.sh

.PHONY: test-m052p-auto-native-dataflow
test-m052p-auto-native-dataflow:
	bash ./scripts/test-m052p-auto-native-dataflow.sh

.PHONY: benchmark-m052p-auto-native-dataflow
benchmark-m052p-auto-native-dataflow:
	bash ./scripts/benchmark-m052p-auto-native-dataflow.sh

.PHONY: format-m052p-auto-native-dataflow
format-m052p-auto-native-dataflow:
	bash ./scripts/format-m052p-auto-native-dataflow.sh

.PHONY: test-race-m052p-auto-native-dataflow
test-race-m052p-auto-native-dataflow:
	bash ./scripts/test-race-m052p-auto-native-dataflow.sh

.PHONY: vet-m052p-auto-native-dataflow
vet-m052p-auto-native-dataflow:
	bash ./scripts/vet-m052p-auto-native-dataflow.sh

.PHONY: review-m052p-auto-native-dataflow
review-m052p-auto-native-dataflow:
	bash ./scripts/review-m052p-auto-native-dataflow.sh

.PHONY: commit-m052p-auto-native-dataflow
commit-m052p-auto-native-dataflow:
	bash ./scripts/commit-m052p-auto-native-dataflow.sh

.PHONY: push-m052p-auto-native-dataflow
push-m052p-auto-native-dataflow:
	bash ./scripts/push-m052p-auto-native-dataflow.sh

.PHONY: test-m052q-auto-native-operators
test-m052q-auto-native-operators:
	bash ./scripts/test-m052q-auto-native-operators.sh

.PHONY: benchmark-m052q-auto-native-operators
benchmark-m052q-auto-native-operators:
	bash ./scripts/benchmark-m052q-auto-native-operators.sh

.PHONY: format-m052q-auto-native-operators
format-m052q-auto-native-operators:
	bash ./scripts/format-m052q-auto-native-operators.sh

.PHONY: test-race-m052q-auto-native-operators
test-race-m052q-auto-native-operators:
	bash ./scripts/test-race-m052q-auto-native-operators.sh

.PHONY: vet-m052q-auto-native-operators
vet-m052q-auto-native-operators:
	bash ./scripts/vet-m052q-auto-native-operators.sh

.PHONY: review-m052q-auto-native-operators
review-m052q-auto-native-operators:
	bash ./scripts/review-m052q-auto-native-operators.sh

.PHONY: commit-m052q-auto-native-operators
commit-m052q-auto-native-operators:
	bash ./scripts/commit-m052q-auto-native-operators.sh

.PHONY: push-m052q-auto-native-operators
push-m052q-auto-native-operators:
	bash ./scripts/push-m052q-auto-native-operators.sh

.PHONY: test-m052r-auto-native-ordered
test-m052r-auto-native-ordered:
	bash ./scripts/test-m052r-auto-native-ordered.sh

.PHONY: benchmark-m052r-auto-native-ordered
benchmark-m052r-auto-native-ordered:
	bash ./scripts/benchmark-m052r-auto-native-ordered.sh

.PHONY: format-m052r-auto-native-ordered
format-m052r-auto-native-ordered:
	bash ./scripts/format-m052r-auto-native-ordered.sh

.PHONY: test-race-m052r-auto-native-ordered
test-race-m052r-auto-native-ordered:
	bash ./scripts/test-race-m052r-auto-native-ordered.sh

.PHONY: vet-m052r-auto-native-ordered
vet-m052r-auto-native-ordered:
	bash ./scripts/vet-m052r-auto-native-ordered.sh

.PHONY: review-m052r-auto-native-ordered
review-m052r-auto-native-ordered:
	bash ./scripts/review-m052r-auto-native-ordered.sh

.PHONY: commit-m052r-auto-native-ordered
commit-m052r-auto-native-ordered:
	bash ./scripts/commit-m052r-auto-native-ordered.sh

.PHONY: push-m052r-auto-native-ordered
push-m052r-auto-native-ordered:
	bash ./scripts/push-m052r-auto-native-ordered.sh
.PHONY: test-m052s-auto-native-grouped benchmark-m052s-auto-native-grouped format-m052s-auto-native-grouped test-race-m052s-auto-native-grouped vet-m052s-auto-native-grouped review-m052s-auto-native-grouped commit-m052s-auto-native-grouped push-m052s-auto-native-grouped
test-m052s-auto-native-grouped:
	bash scripts/test-m052s-auto-native-grouped.sh

benchmark-m052s-auto-native-grouped:
	bash scripts/benchmark-m052s-auto-native-grouped.sh

format-m052s-auto-native-grouped:
	bash scripts/format-m052s-auto-native-grouped.sh

test-race-m052s-auto-native-grouped:
	bash scripts/test-race-m052s-auto-native-grouped.sh

vet-m052s-auto-native-grouped:
	bash scripts/vet-m052s-auto-native-grouped.sh

review-m052s-auto-native-grouped:
	bash scripts/review-m052s-auto-native-grouped.sh

commit-m052s-auto-native-grouped:
	bash scripts/commit-m052s-auto-native-grouped.sh

push-m052s-auto-native-grouped:
	bash scripts/push-m052s-auto-native-grouped.sh
.PHONY: test-m052t-auto-native-grouped-ordered benchmark-m052t-auto-native-grouped-ordered format-m052t-auto-native-grouped-ordered test-race-m052t-auto-native-grouped-ordered vet-m052t-auto-native-grouped-ordered review-m052t-auto-native-grouped-ordered commit-m052t-auto-native-grouped-ordered push-m052t-auto-native-grouped-ordered
test-m052t-auto-native-grouped-ordered:
	bash scripts/test-m052t-auto-native-grouped-ordered.sh

benchmark-m052t-auto-native-grouped-ordered:
	bash scripts/benchmark-m052t-auto-native-grouped-ordered.sh

format-m052t-auto-native-grouped-ordered:
	bash scripts/format-m052t-auto-native-grouped-ordered.sh

test-race-m052t-auto-native-grouped-ordered:
	bash scripts/test-race-m052t-auto-native-grouped-ordered.sh

vet-m052t-auto-native-grouped-ordered:
	bash scripts/vet-m052t-auto-native-grouped-ordered.sh

review-m052t-auto-native-grouped-ordered:
	bash scripts/review-m052t-auto-native-grouped-ordered.sh

commit-m052t-auto-native-grouped-ordered:
	bash scripts/commit-m052t-auto-native-grouped-ordered.sh

push-m052t-auto-native-grouped-ordered:
	bash scripts/push-m052t-auto-native-grouped-ordered.sh
.PHONY: test-m052v-auto-native-scalar-limit benchmark-m052v-auto-native-scalar-limit format-m052v-auto-native-scalar-limit test-race-m052v-auto-native-scalar-limit vet-m052v-auto-native-scalar-limit review-m052v-auto-native-scalar-limit commit-m052v-auto-native-scalar-limit push-m052v-auto-native-scalar-limit
test-m052v-auto-native-scalar-limit:
	bash scripts/test-m052v-auto-native-scalar-limit.sh

benchmark-m052v-auto-native-scalar-limit:
	bash scripts/benchmark-m052v-auto-native-scalar-limit.sh

format-m052v-auto-native-scalar-limit:
	bash scripts/format-m052v-auto-native-scalar-limit.sh

test-race-m052v-auto-native-scalar-limit:
	bash scripts/test-race-m052v-auto-native-scalar-limit.sh

vet-m052v-auto-native-scalar-limit:
	bash scripts/vet-m052v-auto-native-scalar-limit.sh

review-m052v-auto-native-scalar-limit:
	bash scripts/review-m052v-auto-native-scalar-limit.sh

commit-m052v-auto-native-scalar-limit:
	bash scripts/commit-m052v-auto-native-scalar-limit.sh

push-m052v-auto-native-scalar-limit:
	bash scripts/push-m052v-auto-native-scalar-limit.sh
.PHONY: test-m052w-auto-native-distinct-limit benchmark-m052w-auto-native-distinct-limit format-m052w-auto-native-distinct-limit test-race-m052w-auto-native-distinct-limit vet-m052w-auto-native-distinct-limit review-m052w-auto-native-distinct-limit commit-m052w-auto-native-distinct-limit push-m052w-auto-native-distinct-limit
test-m052w-auto-native-distinct-limit:
	bash scripts/test-m052w-auto-native-distinct-limit.sh

benchmark-m052w-auto-native-distinct-limit:
	bash scripts/benchmark-m052w-auto-native-distinct-limit.sh

format-m052w-auto-native-distinct-limit:
	bash scripts/format-m052w-auto-native-distinct-limit.sh

test-race-m052w-auto-native-distinct-limit:
	bash scripts/test-race-m052w-auto-native-distinct-limit.sh

vet-m052w-auto-native-distinct-limit:
	bash scripts/vet-m052w-auto-native-distinct-limit.sh

review-m052w-auto-native-distinct-limit:
	bash scripts/review-m052w-auto-native-distinct-limit.sh

commit-m052w-auto-native-distinct-limit:
	bash scripts/commit-m052w-auto-native-distinct-limit.sh

push-m052w-auto-native-distinct-limit:
	bash scripts/push-m052w-auto-native-distinct-limit.sh
.PHONY: test-m052x-auto-native-aggregate-limit benchmark-m052x-auto-native-aggregate-limit format-m052x-auto-native-aggregate-limit test-race-m052x-auto-native-aggregate-limit vet-m052x-auto-native-aggregate-limit review-m052x-auto-native-aggregate-limit commit-m052x-auto-native-aggregate-limit push-m052x-auto-native-aggregate-limit
test-m052x-auto-native-aggregate-limit:
	bash scripts/test-m052x-auto-native-aggregate-limit.sh

benchmark-m052x-auto-native-aggregate-limit:
	bash scripts/benchmark-m052x-auto-native-aggregate-limit.sh

format-m052x-auto-native-aggregate-limit:
	bash scripts/format-m052x-auto-native-aggregate-limit.sh

test-race-m052x-auto-native-aggregate-limit:
	bash scripts/test-race-m052x-auto-native-aggregate-limit.sh

vet-m052x-auto-native-aggregate-limit:
	bash scripts/vet-m052x-auto-native-aggregate-limit.sh

review-m052x-auto-native-aggregate-limit:
	bash scripts/review-m052x-auto-native-aggregate-limit.sh

commit-m052x-auto-native-aggregate-limit:
	bash scripts/commit-m052x-auto-native-aggregate-limit.sh

push-m052x-auto-native-aggregate-limit:
	bash scripts/push-m052x-auto-native-aggregate-limit.sh
.PHONY: test-m052y-auto-native-composite-grouped-ordered benchmark-m052y-auto-native-composite-grouped-ordered format-m052y-auto-native-composite-grouped-ordered test-race-m052y-auto-native-composite-grouped-ordered vet-m052y-auto-native-composite-grouped-ordered review-m052y-auto-native-composite-grouped-ordered commit-m052y-auto-native-composite-grouped-ordered push-m052y-auto-native-composite-grouped-ordered
test-m052y-auto-native-composite-grouped-ordered:
	bash scripts/test-m052y-auto-native-composite-grouped-ordered.sh

benchmark-m052y-auto-native-composite-grouped-ordered:
	bash scripts/benchmark-m052y-auto-native-composite-grouped-ordered.sh

format-m052y-auto-native-composite-grouped-ordered:
	bash scripts/format-m052y-auto-native-composite-grouped-ordered.sh

test-race-m052y-auto-native-composite-grouped-ordered:
	bash scripts/test-race-m052y-auto-native-composite-grouped-ordered.sh

vet-m052y-auto-native-composite-grouped-ordered:
	bash scripts/vet-m052y-auto-native-composite-grouped-ordered.sh

review-m052y-auto-native-composite-grouped-ordered:
	bash scripts/review-m052y-auto-native-composite-grouped-ordered.sh

commit-m052y-auto-native-composite-grouped-ordered:
	bash scripts/commit-m052y-auto-native-composite-grouped-ordered.sh

push-m052y-auto-native-composite-grouped-ordered:
	bash scripts/push-m052y-auto-native-composite-grouped-ordered.sh

.PHONY: inspect-engine-ideas
inspect-engine-ideas:
	bash scripts/inspect-engine-ideas.sh

.PHONY: inspect-sql-prewhere test-ch001-prewhere benchmark-ch001-prewhere format-ch001-prewhere test-race-ch001-prewhere vet-ch001-prewhere
inspect-sql-prewhere:
	bash scripts/inspect-sql-prewhere.sh

test-ch001-prewhere:
	bash scripts/test-ch001-prewhere.sh

benchmark-ch001-prewhere:
	bash scripts/benchmark-ch001-prewhere.sh

format-ch001-prewhere:
	bash scripts/format-ch001-prewhere.sh

test-race-ch001-prewhere:
	bash scripts/test-race-ch001-prewhere.sh

vet-ch001-prewhere:
	bash scripts/vet-ch001-prewhere.sh

.PHONY: review-ch001-prewhere
review-ch001-prewhere:
	bash scripts/review-ch001-prewhere.sh

.PHONY: commit-ch001-prewhere push-ch001-prewhere
commit-ch001-prewhere:
	bash scripts/commit-ch001-prewhere.sh

push-ch001-prewhere:
	bash scripts/push-ch001-prewhere.sh

.PHONY: list-engine-ideas
list-engine-ideas:
	bash scripts/list-engine-ideas.sh

test-ch031-persistent-query-log:
	bash scripts/test-ch031-persistent-query-log.sh

benchmark-ch031-persistent-query-log:
	bash scripts/benchmark-ch031-persistent-query-log.sh

benchmark-ch031-baseline:
	bash scripts/benchmark-ch031-baseline.sh

format-ch031-persistent-query-log:
	bash scripts/format-ch031-persistent-query-log.sh

test-race-ch031-persistent-query-log:
	bash scripts/test-race-ch031-persistent-query-log.sh

vet-ch031-persistent-query-log:
	bash scripts/vet-ch031-persistent-query-log.sh

verify-ch031-docs:
	bash scripts/verify-ch031-docs.sh

review-ch031-persistent-query-log:
	bash scripts/review-ch031-persistent-query-log.sh

commit-ch031-persistent-query-log:
	bash scripts/commit-ch031-persistent-query-log.sh

push-ch031-persistent-query-log:
	bash scripts/push-ch031-persistent-query-log.sh

format-mz027-arrangement-stats:
	bash scripts/format-mz027-arrangement-stats.sh

test-mz027-arrangement-stats:
	bash scripts/test-mz027-arrangement-stats.sh

benchmark-mz027-baseline:
	bash scripts/benchmark-mz027-baseline.sh

benchmark-mz027-arrangement-stats:
	bash scripts/benchmark-mz027-arrangement-stats.sh

test-race-mz027-arrangement-stats:
	bash scripts/test-race-mz027-arrangement-stats.sh

vet-mz027-arrangement-stats:
	bash scripts/vet-mz027-arrangement-stats.sh

verify-mz027-docs:
	bash scripts/verify-mz027-docs.sh

review-mz027-arrangement-stats:
	bash scripts/review-mz027-arrangement-stats.sh

commit-mz027-arrangement-stats:
	bash scripts/commit-mz027-arrangement-stats.sh

push-mz027-arrangement-stats:
	bash scripts/push-mz027-arrangement-stats.sh

test-tt036-scheduler-stats:
	bash scripts/test-tt036-scheduler-stats.sh

format-tt036-scheduler-stats:
	bash scripts/format-tt036-scheduler-stats.sh

benchmark-tt036-baseline:
	bash scripts/benchmark-tt036-baseline.sh

benchmark-tt036-scheduler-stats:
	bash scripts/benchmark-tt036-scheduler-stats.sh

test-race-tt036-scheduler-stats:
	bash scripts/test-race-tt036-scheduler-stats.sh

vet-tt036-scheduler-stats:
	bash scripts/vet-tt036-scheduler-stats.sh

verify-tt036-docs:
	bash scripts/verify-tt036-docs.sh

review-tt036-scheduler-stats:
	bash scripts/review-tt036-scheduler-stats.sh

commit-tt036-scheduler-stats:
	bash scripts/commit-tt036-scheduler-stats.sh

push-tt036-scheduler-stats:
	bash scripts/push-tt036-scheduler-stats.sh

test-ch048-numeric-predicate-kernel:
	bash scripts/test-ch048-numeric-predicate-kernel.sh

benchmark-ch048-numeric-predicate-kernel-baseline:
	bash scripts/benchmark-ch048-numeric-predicate-kernel-baseline.sh

benchmark-ch048-numeric-predicate-kernel:
	bash scripts/benchmark-ch048-numeric-predicate-kernel.sh

format-ch048-numeric-predicate-kernel:
	bash scripts/format-ch048-numeric-predicate-kernel.sh

test-ch048-numeric-predicate-kernel-full:
	bash scripts/test-ch048-numeric-predicate-kernel-full.sh

race-ch048-numeric-predicate-kernel:
	bash scripts/race-ch048-numeric-predicate-kernel.sh

vet-ch048-numeric-predicate-kernel:
	bash scripts/vet-ch048-numeric-predicate-kernel.sh

verify-ch048-numeric-predicate-kernel-docs:
	bash scripts/verify-ch048-numeric-predicate-kernel-docs.sh

review-ch048-numeric-predicate-kernel:
	bash scripts/review-ch048-numeric-predicate-kernel.sh

commit-ch048-numeric-predicate-kernel:
	bash scripts/commit-ch048-numeric-predicate-kernel.sh

push-ch048-numeric-predicate-kernel:
	bash scripts/push-ch048-numeric-predicate-kernel.sh

audit-next-engine-idea:
	bash scripts/audit-next-engine-idea.sh

test-tt024-text-prefix:
	bash scripts/test-tt024-text-prefix.sh

format-tt024-text-prefix:
	bash scripts/format-tt024-text-prefix.sh

benchmark-tt024-text-prefix:
	bash scripts/benchmark-tt024-text-prefix.sh

review-tt024-text-prefix:
	bash scripts/review-tt024-text-prefix.sh

commit-tt024-text-prefix:
	COMMIT_MESSAGE='$(COMMIT_MESSAGE)' bash scripts/commit-tt024-text-prefix.sh

push-tt024-text-prefix:
	bash scripts/push-tt024-text-prefix.sh

test-ch038-aggregate-if:
	bash scripts/test-ch038-aggregate-if.sh

format-ch038-aggregate-if:
	bash scripts/format-ch038-aggregate-if.sh

benchmark-ch038-aggregate-if:
	bash scripts/benchmark-ch038-aggregate-if.sh

review-ch038-aggregate-if:
	bash scripts/review-ch038-aggregate-if.sh

commit-ch038-aggregate-if:
	bash scripts/commit-ch038-aggregate-if.sh

push-ch038-aggregate-if:
	bash scripts/push-ch038-aggregate-if.sh

test-ch039-approx-stream:
	bash scripts/test-ch039-approx-stream.sh

format-ch039-approx-stream:
	bash scripts/format-ch039-approx-stream.sh

benchmark-ch039-approx-stream:
	bash scripts/benchmark-ch039-approx-stream.sh

review-ch039-approx-stream:
	bash scripts/review-ch039-approx-stream.sh

commit-ch039-approx-stream:
	bash scripts/commit-ch039-approx-stream.sh

push-ch039-approx-stream:
	bash scripts/push-ch039-approx-stream.sh

test-ch041-grouping-identifiers:
	bash scripts/test-ch041-grouping-identifiers.sh


format-ch041-grouping-identifiers:
	bash scripts/format-ch041-grouping-identifiers.sh

benchmark-ch041-grouping-identifiers:
	bash scripts/benchmark-ch041-grouping-identifiers.sh

benchmark-ch041-baseline:
	bash scripts/benchmark-ch041-baseline.sh

review-ch041-grouping-identifiers:
	bash scripts/review-ch041-grouping-identifiers.sh

commit-ch041-grouping-identifiers:
	bash scripts/commit-ch041-grouping-identifiers.sh

push-ch041-grouping-identifiers:
	bash scripts/push-ch041-grouping-identifiers.sh





inspect-text-index-implementation:
	bash scripts/inspect-text-index-implementation.sh

test-ch036-asof-join:
	bash scripts/test-ch036-asof-join.sh

format-ch036-asof-join:
	bash scripts/format-ch036-asof-join.sh

benchmark-ch036-asof-join-baseline:
	bash scripts/benchmark-ch036-asof-join.sh baseline

benchmark-ch036-asof-join-current:
	bash scripts/benchmark-ch036-asof-join.sh current

benchmark-ch036-asof-join: format-ch036-asof-join
	bash scripts/benchmark-ch036-asof-join.sh

review-ch036-asof-join:
	bash scripts/review-ch036-asof-join.sh

commit-ch036-asof-join:
	bash scripts/commit-ch036-asof-join.sh

push-ch036-asof-join:
	bash scripts/push-ch036-asof-join.sh

deliver-ch036-asof-join: test-ch036-asof-join benchmark-ch036-asof-join review-ch036-asof-join commit-ch036-asof-join push-ch036-asof-join

test-ch030-group-key-budget:
	bash scripts/test-ch030-group-key-budget.sh

format-ch030-group-key-budget:
	bash scripts/format-ch030-group-key-budget.sh

benchmark-ch030-group-key-budget-baseline:
	bash scripts/benchmark-ch030-group-key-budget.sh baseline

benchmark-ch030-group-key-budget: format-ch030-group-key-budget
	bash scripts/benchmark-ch030-group-key-budget.sh

review-ch030-group-key-budget:
	bash scripts/review-ch030-group-key-budget.sh

commit-ch030-group-key-budget:
	bash scripts/commit-ch030-group-key-budget.sh

push-ch030-group-key-budget:
	bash scripts/push-ch030-group-key-budget.sh

deliver-ch030-group-key-budget: test-ch030-group-key-budget benchmark-ch030-group-key-budget review-ch030-group-key-budget commit-ch030-group-key-budget push-ch030-group-key-budget
test-m065t-boolean-predicate-kernel:
	bash scripts/test-m065t-boolean-predicate-kernel.sh

benchmark-m065t-boolean-predicate-kernel-baseline:
	bash scripts/benchmark-m065t-boolean-predicate-kernel-baseline.sh

benchmark-m065t-boolean-predicate-kernel:
	bash scripts/benchmark-m065t-boolean-predicate-kernel.sh

format-m065t-boolean-predicate-kernel:
	bash scripts/format-m065t-boolean-predicate-kernel.sh

test-m065t-boolean-predicate-kernel-full:
	bash scripts/test-m065t-boolean-predicate-kernel-full.sh

race-m065t-boolean-predicate-kernel:
	bash scripts/race-m065t-boolean-predicate-kernel.sh

vet-m065t-boolean-predicate-kernel:
	bash scripts/vet-m065t-boolean-predicate-kernel.sh

verify-m065t-boolean-predicate-kernel-docs:
	bash scripts/verify-m065t-boolean-predicate-kernel-docs.sh

review-m065t-boolean-predicate-kernel:
	bash scripts/review-m065t-boolean-predicate-kernel.sh

commit-m065t-boolean-predicate-kernel:
	bash scripts/commit-m065t-boolean-predicate-kernel.sh

push-m065t-boolean-predicate-kernel:
	bash scripts/push-m065t-boolean-predicate-kernel.sh

test-m065u-parallel-row-binary:
	bash scripts/test-m065u-parallel-row-binary.sh

benchmark-m065u-parallel-row-binary-baseline:
	bash scripts/benchmark-m065u-parallel-row-binary-baseline.sh

benchmark-m065u-parallel-row-binary:
	bash scripts/benchmark-m065u-parallel-row-binary.sh

format-m065u-parallel-row-binary:
	bash scripts/format-m065u-parallel-row-binary.sh

test-m065u-parallel-row-binary-full:
	bash scripts/test-m065u-parallel-row-binary-full.sh

race-m065u-parallel-row-binary:
	bash scripts/race-m065u-parallel-row-binary.sh

vet-m065u-parallel-row-binary:
	bash scripts/vet-m065u-parallel-row-binary.sh

verify-m065u-parallel-row-binary-docs:
	bash scripts/verify-m065u-parallel-row-binary-docs.sh

review-m065u-parallel-row-binary:
	bash scripts/review-m065u-parallel-row-binary.sh

commit-m065u-parallel-row-binary:
	bash scripts/commit-m065u-parallel-row-binary.sh

push-m065u-parallel-row-binary:
	bash scripts/push-m065u-parallel-row-binary.sh

.PHONY: test-ch002-primary-mark-pruning benchmark-ch002-primary-mark-pruning format-ch002-primary-mark-pruning
test-ch002-primary-mark-pruning:
	bash scripts/test-ch002-primary-mark-pruning.sh

.PHONY: test-race-ch002-primary-mark-pruning vet-ch002-primary-mark-pruning
test-race-ch002-primary-mark-pruning:
	bash scripts/test-race-ch002-primary-mark-pruning.sh

vet-ch002-primary-mark-pruning:
	bash scripts/vet-ch002-primary-mark-pruning.sh

benchmark-ch002-primary-mark-pruning:
	bash scripts/benchmark-ch002-primary-mark-pruning.sh

format-ch002-primary-mark-pruning:
	bash scripts/format-ch002-primary-mark-pruning.sh

.PHONY: test-ch002-real-index
test-ch002-real-index:
	bash scripts/test-ch002-real-index.sh

.PHONY: benchmark-ch002-stream-sparse
benchmark-ch002-stream-sparse:
	bash scripts/benchmark-ch002-stream-sparse.sh

.PHONY: benchmark-ch002-hattrie
benchmark-ch002-hattrie:
	bash scripts/benchmark-ch002-hattrie.sh

.PHONY: benchmark-ch002-hattrie-materialized benchmark-ch002-hattrie-stream
benchmark-ch002-hattrie-materialized:
	bash scripts/benchmark-ch002-hattrie-materialized.sh

benchmark-ch002-hattrie-stream:
	bash scripts/benchmark-ch002-hattrie-stream.sh

.PHONY: verify-ch002-docs
verify-ch002-docs:
	bash scripts/verify-ch002-docs.sh

.PHONY: review-ch002
review-ch002:
	bash scripts/review-ch002.sh

.PHONY: review-ch002-code
review-ch002-code:
	bash scripts/review-ch002-code.sh

.PHONY: commit-ch002 push-ch002
commit-ch002:
	bash scripts/commit-ch002.sh

push-ch002:
	bash scripts/push-ch002.sh

.PHONY: commit-engine-idea-catalog push-engine-idea-catalog
commit-engine-idea-catalog:
	bash scripts/commit-engine-idea-catalog.sh

push-engine-idea-catalog:
	bash scripts/push-engine-idea-catalog.sh
.PHONY: test-ch040-arg-extreme
test-ch040-arg-extreme:
	bash ./scripts/test-ch040-arg-extreme.sh

.PHONY: benchmark-ch040-arg-extreme
benchmark-ch040-arg-extreme:
	bash ./scripts/benchmark-ch040-arg-extreme.sh

.PHONY: format-ch040-arg-extreme
format-ch040-arg-extreme:
	bash ./scripts/format-ch040-arg-extreme.sh

.PHONY: test-ch040-sql-package
test-ch040-sql-package:
	bash ./scripts/test-ch040-sql-package.sh

.PHONY: test-race-ch040-arg-extreme
test-race-ch040-arg-extreme:
	bash ./scripts/test-race-ch040-arg-extreme.sh

.PHONY: vet-ch040-arg-extreme
vet-ch040-arg-extreme:
	bash ./scripts/vet-ch040-arg-extreme.sh
.PHONY: verify-ch040-docs
verify-ch040-docs:
	@bash ./scripts/verify-ch040-docs.sh
.PHONY: review-ch040-arg-extreme
review-ch040-arg-extreme:
	@bash ./scripts/review-ch040-arg-extreme.sh
.PHONY: commit-ch040-arg-extreme
commit-ch040-arg-extreme:
	@bash ./scripts/commit-ch040-arg-extreme.sh
.PHONY: push-ch040-arg-extreme
push-ch040-arg-extreme:
	@bash ./scripts/push-ch040-arg-extreme.sh
.PHONY: benchmark-tt016-disk-reserve
benchmark-tt016-disk-reserve:
	@bash ./scripts/benchmark-tt016-disk-reserve.sh
.PHONY: test-tt016-disk-reserve
test-tt016-disk-reserve:
	@bash ./scripts/test-tt016-disk-reserve.sh
.PHONY: test-tt016-cli-config
test-tt016-cli-config:
	@bash ./scripts/test-tt016-cli-config.sh
.PHONY: format-tt016-disk-reserve
format-tt016-disk-reserve:
	@bash ./scripts/format-tt016-disk-reserve.sh

.PHONY: verify-tt016-docs
verify-tt016-docs:
	@bash ./scripts/verify-tt016-docs.sh

.PHONY: test-race-tt016-disk-reserve
test-race-tt016-disk-reserve:
	@bash ./scripts/test-race-tt016-disk-reserve.sh

.PHONY: vet-tt016-disk-reserve
vet-tt016-disk-reserve:
	@bash ./scripts/vet-tt016-disk-reserve.sh

.PHONY: review-tt016-disk-reserve
review-tt016-disk-reserve:
	@bash ./scripts/review-tt016-disk-reserve.sh

.PHONY: commit-tt016-disk-reserve
commit-tt016-disk-reserve:
	@bash ./scripts/commit-tt016-disk-reserve.sh

.PHONY: push-tt016-disk-reserve
push-tt016-disk-reserve:
	@bash ./scripts/push-tt016-disk-reserve.sh

.PHONY: test-tt044-migration-preview
test-tt044-migration-preview:
	@bash ./scripts/test-tt044-migration-preview.sh

.PHONY: format-tt044-migration-preview
format-tt044-migration-preview:
	@bash ./scripts/format-tt044-migration-preview.sh

.PHONY: benchmark-tt044-migration-preview
benchmark-tt044-migration-preview:
	@bash ./scripts/benchmark-tt044-migration-preview.sh

.PHONY: verify-tt044-docs
verify-tt044-docs:
	@bash ./scripts/verify-tt044-docs.sh

.PHONY: test-race-tt044-migration-preview
test-race-tt044-migration-preview:
	@bash ./scripts/test-race-tt044-migration-preview.sh

.PHONY: vet-tt044-migration-preview
vet-tt044-migration-preview:
	@bash ./scripts/vet-tt044-migration-preview.sh

.PHONY: review-tt044-migration-preview
review-tt044-migration-preview:
	@bash ./scripts/review-tt044-migration-preview.sh

.PHONY: commit-tt044-migration-preview
commit-tt044-migration-preview:
	@bash ./scripts/commit-tt044-migration-preview.sh

.PHONY: push-tt044-migration-preview
push-tt044-migration-preview:
	@bash ./scripts/push-tt044-migration-preview.sh

.PHONY: test-mz041-refresh-freshness
test-mz041-refresh-freshness:
	@bash ./scripts/test-mz041-refresh-freshness.sh

.PHONY: format-mz041-refresh-freshness
format-mz041-refresh-freshness:
	@bash ./scripts/format-mz041-refresh-freshness.sh

.PHONY: benchmark-mz041-refresh-freshness
benchmark-mz041-refresh-freshness:
	@bash ./scripts/benchmark-mz041-refresh-freshness.sh

.PHONY: verify-mz041-docs
verify-mz041-docs:
	@bash ./scripts/verify-mz041-docs.sh

.PHONY: test-race-mz041-refresh-freshness
test-race-mz041-refresh-freshness:
	@bash ./scripts/test-race-mz041-refresh-freshness.sh

.PHONY: vet-mz041-refresh-freshness
vet-mz041-refresh-freshness:
	@bash ./scripts/vet-mz041-refresh-freshness.sh

.PHONY: review-mz041-refresh-freshness
review-mz041-refresh-freshness:
	@bash ./scripts/review-mz041-refresh-freshness.sh

.PHONY: commit-mz041-refresh-freshness
commit-mz041-refresh-freshness:
	@bash ./scripts/commit-mz041-refresh-freshness.sh

.PHONY: push-mz041-refresh-freshness
push-mz041-refresh-freshness:
	@bash ./scripts/push-mz041-refresh-freshness.sh

.PHONY: test-tt017
test-tt017:
	@bash ./scripts/test-tt017.sh

.PHONY: format-tt017
format-tt017:
	@bash ./scripts/format-tt017.sh

.PHONY: benchmark-tt017
benchmark-tt017:
	@bash ./scripts/benchmark-tt017.sh

.PHONY: test-race-tt017
test-race-tt017:
	@bash ./scripts/test-race-tt017.sh

.PHONY: vet-tt017
vet-tt017:
	@bash ./scripts/vet-tt017.sh

.PHONY: verify-tt017-docs
verify-tt017-docs:
	@bash ./scripts/verify-tt017-docs.sh

.PHONY: review-tt017
review-tt017:
	@bash ./scripts/review-tt017.sh

.PHONY: commit-tt017
commit-tt017:
	@bash ./scripts/commit-tt017.sh

.PHONY: push-tt017
push-tt017:
	@bash ./scripts/push-tt017.sh

.PHONY: test-mz042-dependency-graph
test-mz042-dependency-graph:
	@bash ./scripts/test-mz042-dependency-graph.sh

.PHONY: format-mz042-dependency-graph
format-mz042-dependency-graph:
	@bash ./scripts/format-mz042-dependency-graph.sh

.PHONY: benchmark-mz042-dependency-graph
benchmark-mz042-dependency-graph:
	@bash ./scripts/benchmark-mz042-dependency-graph.sh

.PHONY: benchmark-mz042-baseline
benchmark-mz042-baseline:
	@bash ./scripts/benchmark-mz042-baseline.sh

.PHONY: test-race-mz042-dependency-graph
test-race-mz042-dependency-graph:
	@bash ./scripts/test-race-mz042-dependency-graph.sh

.PHONY: vet-mz042-dependency-graph
vet-mz042-dependency-graph:
	@bash ./scripts/vet-mz042-dependency-graph.sh

.PHONY: verify-mz042-docs
verify-mz042-docs:
	@bash ./scripts/verify-mz042-docs.sh

.PHONY: review-mz042-dependency-graph
review-mz042-dependency-graph:
	@bash ./scripts/review-mz042-dependency-graph.sh

.PHONY: commit-mz042-dependency-graph
commit-mz042-dependency-graph:
	@bash ./scripts/commit-mz042-dependency-graph.sh

.PHONY: push-mz042-dependency-graph
push-mz042-dependency-graph:
	@bash ./scripts/push-mz042-dependency-graph.sh
.PHONY: test-tt050-sql-planner-statistics
test-tt050-sql-planner-statistics:
	bash scripts/test-tt050-sql-planner-statistics.sh

.PHONY: format-tt050-sql-planner-statistics
format-tt050-sql-planner-statistics:
	bash scripts/format-tt050-sql-planner-statistics.sh

.PHONY: benchmark-tt050-sql-planner-statistics
benchmark-tt050-sql-planner-statistics:
	bash scripts/benchmark-tt050-sql-planner-statistics.sh

.PHONY: test-race-tt050-sql-planner-statistics
test-race-tt050-sql-planner-statistics:
	bash scripts/test-race-tt050-sql-planner-statistics.sh

.PHONY: verify-tt050-sql-planner-statistics-docs
verify-tt050-sql-planner-statistics-docs:
	bash scripts/verify-tt050-sql-planner-statistics-docs.sh

.PHONY: review-tt050-sql-planner-statistics
review-tt050-sql-planner-statistics:
	bash scripts/review-tt050-sql-planner-statistics.sh

.PHONY: commit-tt050-sql-planner-statistics
commit-tt050-sql-planner-statistics:
	bash scripts/commit-tt050-sql-planner-statistics.sh

.PHONY: push-tt050-sql-planner-statistics
.PHONY: test-ch049-external-dictionary
test-ch049-external-dictionary:
	bash scripts/test-ch049-external-dictionary.sh

.PHONY: test-ch032-query-profiler
test-ch032-query-profiler:
	bash scripts/test-ch032-query-profiler.sh

.PHONY: benchmark-ch032-query-profiler
benchmark-ch032-query-profiler:
	bash scripts/benchmark-ch032-query-profiler.sh

.PHONY: format-ch032-query-profiler
format-ch032-query-profiler:
	bash scripts/format-ch032-query-profiler.sh

.PHONY: test-race-ch032-query-profiler
test-race-ch032-query-profiler:
	bash scripts/test-race-ch032-query-profiler.sh

.PHONY: verify-ch032-query-profiler-docs
verify-ch032-query-profiler-docs:
	bash scripts/verify-ch032-query-profiler-docs.sh

.PHONY: review-ch032-query-profiler
review-ch032-query-profiler:
	bash scripts/review-ch032-query-profiler.sh

.PHONY: commit-ch032-query-profiler
commit-ch032-query-profiler:
	bash scripts/commit-ch032-query-profiler.sh

.PHONY: push-ch032-query-profiler
push-ch032-query-profiler:
	bash scripts/push-ch032-query-profiler.sh

.PHONY: test-ch050-named-settings
test-ch050-named-settings:
	bash scripts/test-ch050-named-settings.sh

.PHONY: benchmark-ch050-named-settings
benchmark-ch050-named-settings:
	bash scripts/benchmark-ch050-named-settings.sh

.PHONY: format-ch050-named-settings
format-ch050-named-settings:
	bash scripts/format-ch050-named-settings.sh

.PHONY: test-race-ch050-named-settings
test-race-ch050-named-settings:
	bash scripts/test-race-ch050-named-settings.sh

.PHONY: verify-ch050-named-settings-docs
verify-ch050-named-settings-docs:
	bash scripts/verify-ch050-named-settings-docs.sh

.PHONY: review-ch050-named-settings
review-ch050-named-settings:
	bash scripts/review-ch050-named-settings.sh

.PHONY: commit-ch050-named-settings
commit-ch050-named-settings:
	bash scripts/commit-ch050-named-settings.sh

.PHONY: push-ch050-named-settings
push-ch050-named-settings:
	bash scripts/push-ch050-named-settings.sh

.PHONY: inspect-ch029-query-contracts
.PHONY: format-ch049-external-dictionary
format-ch049-external-dictionary:
	bash scripts/format-ch049-external-dictionary.sh

.PHONY: benchmark-ch049-external-dictionary
benchmark-ch049-external-dictionary:
	bash scripts/benchmark-ch049-external-dictionary.sh

.PHONY: test-race-ch049-external-dictionary
test-race-ch049-external-dictionary:
	bash scripts/test-race-ch049-external-dictionary.sh

.PHONY: verify-ch049-external-dictionary-docs
verify-ch049-external-dictionary-docs:
	bash scripts/verify-ch049-external-dictionary-docs.sh

.PHONY: review-ch049-external-dictionary
review-ch049-external-dictionary:
	bash scripts/review-ch049-external-dictionary.sh

.PHONY: commit-ch049-external-dictionary
commit-ch049-external-dictionary:
	bash scripts/commit-ch049-external-dictionary.sh

.PHONY: push-ch049-external-dictionary
push-ch049-external-dictionary:
	bash scripts/push-ch049-external-dictionary.sh

push-tt050-sql-planner-statistics:
	bash scripts/push-tt050-sql-planner-statistics.sh
.PHONY: inspect-query-frontier-contracts
inspect-query-frontier-contracts:
	bash scripts/inspect-query-frontier-contracts.sh
.PHONY: benchmark-mz007-frontier-rejection
benchmark-mz007-frontier-rejection:
	bash scripts/benchmark-mz007-frontier-rejection.sh
.PHONY: test-mz007-frontier-rejection
test-mz007-frontier-rejection:
	bash scripts/test-mz007-frontier-rejection.sh
.PHONY: format-mz007-frontier-rejection
format-mz007-frontier-rejection:
	bash scripts/format-mz007-frontier-rejection.sh
.PHONY: inspect-frontier-api
inspect-frontier-api:
	bash scripts/inspect-frontier-api.sh
.PHONY: test-race-mz007-frontier-rejection
test-race-mz007-frontier-rejection:
	bash scripts/test-race-mz007-frontier-rejection.sh
.PHONY: inspect-adopted-ledger
inspect-adopted-ledger:
	bash scripts/inspect-adopted-ledger.sh
.PHONY: verify-mz007-docs
verify-mz007-docs:
	bash scripts/verify-mz007-docs.sh
.PHONY: inspect-api-diff

review-mz007-frontier-rejection:
	bash scripts/review-mz007-frontier-rejection.sh

commit-mz007-frontier-rejection:
	bash scripts/commit-mz007-frontier-rejection.sh

push-mz007-frontier-rejection:
	bash scripts/push-mz007-frontier-rejection.sh

benchmark-mz008-asof:
	bash scripts/benchmark-mz008-asof.sh


test-mz009-temporal-validity:
	bash scripts/test-mz009-temporal-validity.sh

test-race-mz009-temporal-validity:
	bash scripts/test-race-mz009-temporal-validity.sh

benchmark-mz009-temporal-validity:
	bash scripts/benchmark-mz009-temporal-validity.sh

format-mz009-temporal-validity:
	bash scripts/format-mz009-temporal-validity.sh

test-mz009-broad:
	bash scripts/test-mz009-broad.sh

verify-mz009-docs:
	bash scripts/verify-mz009-docs.sh

review-mz009-temporal-validity:
	bash scripts/review-mz009-temporal-validity.sh

commit-mz009-temporal-validity:
	bash scripts/commit-mz009-temporal-validity.sh

push-mz009-temporal-validity:
	bash scripts/push-mz009-temporal-validity.sh

test-mz008-asof:
	bash scripts/test-mz008-asof.sh

format-mz008-asof:
	bash scripts/format-mz008-asof.sh

test-race-mz008-asof:
	bash scripts/test-race-mz008-asof.sh

test-mz008-broad:
	bash scripts/test-mz008-broad.sh

verify-mz008-docs:
	bash scripts/verify-mz008-docs.sh

review-mz008-asof:
	bash scripts/review-mz008-asof.sh

commit-mz008-asof:
	bash scripts/commit-mz008-asof.sh

push-mz008-asof:
	bash scripts/push-mz008-asof.sh
test-mz010-journal-subscription:
	bash scripts/test-mz010-journal-subscription.sh
format-mz010-journal-subscription:
	bash scripts/format-mz010-journal-subscription.sh
test-race-mz010-journal-subscription:
	bash scripts/test-race-mz010-journal-subscription.sh

test-mz010-broad:
	bash scripts/test-mz010-broad.sh
benchmark-mz010-journal-subscription:
	bash scripts/benchmark-mz010-journal-subscription.sh
test-tt040-space-changefeed:
	sh scripts/test-tt040-space-changefeed.sh
format-tt040-space-changefeed:
	sh scripts/format-tt040-space-changefeed.sh
test-race-tt040-space-changefeed:
	sh scripts/test-race-tt040-space-changefeed.sh
benchmark-tt040-space-changefeed:
	sh scripts/benchmark-tt040-space-changefeed.sh
vet-tt040-space-changefeed:
	sh scripts/vet-tt040-space-changefeed.sh
verify-tt040-docs:
	sh scripts/verify-tt040-docs.sh
review-tt040-space-changefeed:
	sh scripts/review-tt040-space-changefeed.sh
commit-tt040-space-changefeed:
	sh scripts/commit-tt040-space-changefeed.sh
push-tt040-space-changefeed:
	sh scripts/push-tt040-space-changefeed.sh
status-mz010:
	bash scripts/status-mz010.sh
verify-mz010-docs:
	bash scripts/verify-mz010-docs.sh
review-mz010:
	bash scripts/review-mz010.sh
commit-mz010:
	bash scripts/commit-mz010.sh
push-mz010:
	bash scripts/push-mz010.sh
benchmark-mz011-sink:
	bash scripts/benchmark-mz011-sink.sh
test-mz011-sink:
	bash scripts/test-mz011-sink.sh
format-mz011-sink:
	bash scripts/format-mz011-sink.sh
test-race-mz011-sink:
	bash scripts/test-race-mz011-sink.sh
test-mz011-broad:
	bash scripts/test-mz011-broad.sh
verify-mz011-docs:
	bash scripts/verify-mz011-docs.sh
review-mz011:
	bash scripts/review-mz011.sh
status-mz011:
	bash scripts/status-mz011.sh
commit-mz011:
	bash scripts/commit-mz011.sh
push-mz011:
	bash scripts/push-mz011.sh
benchmark-mz012-sink:
	bash scripts/benchmark-mz012-sink.sh
test-mz012-sink:
	bash scripts/test-mz012-sink.sh
format-mz012-sink:
	bash scripts/format-mz012-sink.sh
test-race-mz012-sink:
	bash scripts/test-race-mz012-sink.sh
test-mz012-broad:
	bash scripts/test-mz012-broad.sh
verify-mz012-docs:
	bash scripts/verify-mz012-docs.sh
review-mz012:
	bash scripts/review-mz012.sh
status-mz012:
	bash scripts/status-mz012.sh
commit-mz012:
	bash scripts/commit-mz012.sh
push-mz012:
	bash scripts/push-mz012.sh
benchmark-mz013-source:
	bash scripts/benchmark-mz013-source.sh
test-mz013-source:
	bash scripts/test-mz013-source.sh
format-mz013-source:
	bash scripts/format-mz013-source.sh
test-race-mz013-source:
	bash scripts/test-race-mz013-source.sh
test-mz013-broad:
	bash scripts/test-mz013-broad.sh
verify-mz013-docs:
	bash scripts/verify-mz013-docs.sh
review-mz013:
	bash scripts/review-mz013.sh
status-mz013:
	bash scripts/status-mz013.sh
commit-mz013:
	bash scripts/commit-mz013.sh
push-mz013:
	bash scripts/push-mz013.sh
benchmark-mz014-upsert:
	bash scripts/benchmark-mz014-upsert.sh
test-mz014-upsert:
	bash scripts/test-mz014-upsert.sh
format-mz014-upsert:
	bash scripts/format-mz014-upsert.sh
test-race-mz014-upsert:
	bash scripts/test-race-mz014-upsert.sh
test-mz014-broad:
	bash scripts/test-mz014-broad.sh
verify-mz014-docs:
	bash scripts/verify-mz014-docs.sh
review-mz014:
	bash scripts/review-mz014.sh
status-mz014:
	bash scripts/status-mz014.sh
commit-mz014:
	bash scripts/commit-mz014.sh
push-mz014:
	bash scripts/push-mz014.sh

benchmark-mz015-cdc:
	bash scripts/benchmark-mz015-cdc.sh
format-mz015-cdc:
	bash scripts/format-mz015-cdc.sh
test-mz015-cdc:
	bash scripts/test-mz015-cdc.sh
test-race-mz015-cdc:
	bash scripts/test-race-mz015-cdc.sh
test-mz015-broad:
	bash scripts/test-mz015-broad.sh
verify-mz015-docs:
	bash scripts/verify-mz015-docs.sh
review-mz015:
	bash scripts/review-mz015.sh
status-mz015:
	bash scripts/status-mz015.sh
commit-mz015:
	bash scripts/commit-mz015.sh
push-mz015:
	bash scripts/push-mz015.sh

benchmark-mz017-restore-workers:
	MZ017_RESTORE_KEYS='$(MZ017_RESTORE_KEYS)' MZ017_RESTORE_PARTITIONS='$(MZ017_RESTORE_PARTITIONS)' MZ017_RESTORE_CPUS='$(MZ017_RESTORE_CPUS)' BENCHTIME='$(BENCHTIME)' COUNT='3' BENCHMARK_ARTIFACT_DIR='$(BENCHMARK_ARTIFACT_DIR)' bash scripts/benchmark-mz017-restore-workers.sh

test-mz017-restore-workers:
	bash scripts/test-mz017-restore-workers.sh

format-mz017-restore-workers:
	bash scripts/format-mz017-restore-workers.sh

test-race-mz017-restore-workers:
	bash scripts/test-race-mz017-restore-workers.sh

test-mz017-broad:
	bash scripts/test-mz017-broad.sh

verify-mz017-docs:
	bash scripts/verify-mz017-docs.sh

review-mz017:
	bash scripts/review-mz017.sh

status-mz017:
	bash scripts/status-mz017.sh

commit-mz017:
	bash scripts/commit-mz017.sh

push-mz017:
	bash scripts/push-mz017.sh

.PHONY: benchmark-mz018-compute-pool
benchmark-mz018-compute-pool:
	bash scripts/benchmark-mz018-compute-pool.sh

.PHONY: test-mz018-compute-pool
test-mz018-compute-pool:
	bash scripts/test-mz018-compute-pool.sh

.PHONY: format-mz018-compute-pool
format-mz018-compute-pool:
	bash scripts/format-mz018-compute-pool.sh

.PHONY: test-race-mz018-compute-pool
test-race-mz018-compute-pool:
	bash scripts/test-race-mz018-compute-pool.sh

.PHONY: test-mz018-broad
test-mz018-broad:
	bash scripts/test-mz018-broad.sh

.PHONY: verify-mz018-compute-pool-docs
verify-mz018-compute-pool-docs:
	bash scripts/verify-mz018-compute-pool-docs.sh

.PHONY: review-mz018-compute-pool
review-mz018-compute-pool:
	bash scripts/review-mz018-compute-pool.sh

.PHONY: status-mz018-compute-pool
status-mz018-compute-pool:
	bash scripts/status-mz018-compute-pool.sh

.PHONY: commit-mz018-compute-pool
commit-mz018-compute-pool:
	bash scripts/commit-mz018-compute-pool.sh

.PHONY: push-mz018-compute-pool
push-mz018-compute-pool:
	bash scripts/push-mz018-compute-pool.sh

.PHONY: benchmark-mz019-resource-pools
benchmark-mz019-resource-pools:
	bash scripts/benchmark-mz019-resource-pools.sh

.PHONY: format-mz019-resource-pools
format-mz019-resource-pools:
	bash scripts/format-mz019-resource-pools.sh

.PHONY: test-mz019-resource-pools
test-mz019-resource-pools:
	bash scripts/test-mz019-resource-pools.sh

.PHONY: test-race-mz019-resource-pools
test-race-mz019-resource-pools:
	bash scripts/test-race-mz019-resource-pools.sh

.PHONY: test-mz019-broad
test-mz019-broad:
	bash scripts/test-mz019-broad.sh

.PHONY: verify-mz019-resource-pool-docs
verify-mz019-resource-pool-docs:
	bash scripts/verify-mz019-resource-pool-docs.sh

.PHONY: review-mz019-resource-pools
review-mz019-resource-pools:
	bash scripts/review-mz019-resource-pools.sh

.PHONY: status-mz019-resource-pools
status-mz019-resource-pools:
	bash scripts/status-mz019-resource-pools.sh

.PHONY: commit-mz019-resource-pools
commit-mz019-resource-pools:
	bash scripts/commit-mz019-resource-pools.sh

.PHONY: push-mz019-resource-pools
push-mz019-resource-pools:
	bash scripts/push-mz019-resource-pools.sh

.PHONY: test-mz022-index-readiness
test-mz022-index-readiness:
	bash scripts/test-mz022-index-readiness.sh

.PHONY: benchmark-mz022-index-readiness
benchmark-mz022-index-readiness:
	bash scripts/benchmark-mz022-index-readiness.sh

.PHONY: format-mz022-index-readiness
format-mz022-index-readiness:
	bash scripts/format-mz022-index-readiness.sh

.PHONY: test-race-mz022-index-readiness
test-race-mz022-index-readiness:
	bash scripts/test-race-mz022-index-readiness.sh

.PHONY: test-mz022-broad
test-mz022-broad:
	bash scripts/test-mz022-broad.sh

.PHONY: verify-mz022-index-readiness-docs
verify-mz022-index-readiness-docs:
	bash scripts/verify-mz022-index-readiness-docs.sh

.PHONY: review-mz022-index-readiness
review-mz022-index-readiness:
	bash scripts/review-mz022-index-readiness.sh

.PHONY: status-mz022-index-readiness
status-mz022-index-readiness:
	bash scripts/status-mz022-index-readiness.sh

.PHONY: commit-mz022-index-readiness
commit-mz022-index-readiness:
	bash scripts/commit-mz022-index-readiness.sh

.PHONY: push-mz022-index-readiness
push-mz022-index-readiness:
	bash scripts/push-mz022-index-readiness.sh

.PHONY: test-expiration-deadline-cleaner
test-expiration-deadline-cleaner:
	bash scripts/test-expiration-deadline-cleaner.sh

.PHONY: benchmark-expiration-deadline-cleaner
benchmark-expiration-deadline-cleaner:
	bash scripts/benchmark-expiration-deadline-cleaner.sh

.PHONY: format-expiration-deadline-cleaner
format-expiration-deadline-cleaner:
	bash scripts/format-expiration-deadline-cleaner.sh

.PHONY: test-race-expiration-deadline-cleaner
test-race-expiration-deadline-cleaner:
	bash scripts/test-race-expiration-deadline-cleaner.sh

.PHONY: test-expiration-deadline-cleaner-broad
test-expiration-deadline-cleaner-broad:
	bash scripts/test-expiration-deadline-cleaner-broad.sh

.PHONY: vet-expiration-deadline-cleaner
vet-expiration-deadline-cleaner:
	bash scripts/vet-expiration-deadline-cleaner.sh

.PHONY: verify-expiration-deadline-cleaner
verify-expiration-deadline-cleaner:
	bash scripts/verify-expiration-deadline-cleaner.sh

.PHONY: review-expiration-deadline-cleaner
review-expiration-deadline-cleaner:
	bash scripts/review-expiration-deadline-cleaner.sh

.PHONY: benchmark-selective-backup
benchmark-selective-backup:
	bash scripts/benchmark-selective-backup.sh

.PHONY: test-selective-backup
test-selective-backup:
	bash scripts/test-selective-backup.sh

.PHONY: format-selective-backup
format-selective-backup:
	bash scripts/format-selective-backup.sh

.PHONY: test-race-selective-backup
test-race-selective-backup:
	bash scripts/test-race-selective-backup.sh

.PHONY: test-selective-backup-broad
test-selective-backup-broad:
	bash scripts/test-selective-backup-broad.sh

.PHONY: vet-selective-backup
vet-selective-backup:
	bash scripts/vet-selective-backup.sh

.PHONY: verify-selective-backup
verify-selective-backup:
	bash scripts/verify-selective-backup.sh

.PHONY: review-selective-backup
review-selective-backup:
	bash scripts/review-selective-backup.sh

.PHONY: commit-selective-backup
commit-selective-backup:
	bash scripts/commit-selective-backup.sh

.PHONY: push-selective-backup
push-selective-backup:
	bash scripts/push-selective-backup.sh

.PHONY: test-restore-resume
test-restore-resume:
	bash scripts/test-restore-resume.sh

.PHONY: format-restore-resume
format-restore-resume:
	bash scripts/format-restore-resume.sh

.PHONY: benchmark-restore-resume
benchmark-restore-resume:
	bash scripts/benchmark-restore-resume.sh

.PHONY: test-restore-resume-cli
test-restore-resume-cli:
	bash scripts/test-restore-resume-cli.sh

.PHONY: test-race-restore-resume
test-race-restore-resume:
	bash scripts/test-race-restore-resume.sh

.PHONY: test-restore-resume-broad
test-restore-resume-broad:
	bash scripts/test-restore-resume-broad.sh

.PHONY: vet-restore-resume
vet-restore-resume:
	bash scripts/vet-restore-resume.sh

.PHONY: review-restore-resume
review-restore-resume:
	bash scripts/review-restore-resume.sh

.PHONY: verify-restore-resume
verify-restore-resume:
	bash scripts/verify-restore-resume.sh

.PHONY: commit-restore-resume
commit-restore-resume:
	bash scripts/commit-restore-resume.sh

.PHONY: push-restore-resume
push-restore-resume:
	bash scripts/push-restore-resume.sh

.PHONY: commit-expiration-deadline-cleaner
commit-expiration-deadline-cleaner:
	bash scripts/commit-expiration-deadline-cleaner.sh

.PHONY: push-expiration-deadline-cleaner
push-expiration-deadline-cleaner:
	bash scripts/push-expiration-deadline-cleaner.sh

.PHONY: commit-expiration-deadline-cleaner-push-fix
commit-expiration-deadline-cleaner-push-fix:
	bash scripts/commit-expiration-deadline-cleaner-push-fix.sh
test-tt032-multiplexing:
	sh scripts/test-tt032-multiplexing.sh
benchmark-tt032-multiplexing:
	sh scripts/benchmark-tt032-multiplexing.sh
format-tt032-multiplexing:
	sh scripts/format-tt032-multiplexing.sh
test-race-tt032-multiplexing:
	sh scripts/test-race-tt032-multiplexing.sh

vet-tt032-multiplexing:
	sh scripts/vet-tt032-multiplexing.sh
test-tt032-package:
	sh scripts/test-tt032-package.sh
verify-tt032-docs:
	sh scripts/verify-tt032-docs.sh
review-tt032-multiplexing:
	sh scripts/review-tt032-multiplexing.sh
commit-tt032-multiplexing:
	sh scripts/commit-tt032-multiplexing.sh

push-tt032-multiplexing:
	sh scripts/push-tt032-multiplexing.sh

.PHONY: test-tt043-maintenance-read-only
test-tt043-maintenance-read-only:
	sh scripts/test-tt043-maintenance-read-only.sh

.PHONY: format-tt043-maintenance-read-only
format-tt043-maintenance-read-only:
	sh scripts/format-tt043-maintenance-read-only.sh

.PHONY: benchmark-tt043-maintenance-read-only
benchmark-tt043-maintenance-read-only:
	sh scripts/benchmark-tt043-maintenance-read-only.sh

.PHONY: verify-tt043-docs
verify-tt043-docs:
	sh scripts/verify-tt043-docs.sh

.PHONY: test-tt043-package
test-tt043-package:
	sh scripts/test-tt043-package.sh

.PHONY: test-race-tt043-maintenance-read-only
test-race-tt043-maintenance-read-only:
	sh scripts/test-race-tt043-maintenance-read-only.sh

.PHONY: vet-tt043-maintenance-read-only
vet-tt043-maintenance-read-only:
	sh scripts/vet-tt043-maintenance-read-only.sh

.PHONY: review-tt043-maintenance-read-only
review-tt043-maintenance-read-only:
	sh scripts/review-tt043-maintenance-read-only.sh

.PHONY: commit-tt043-maintenance-read-only
commit-tt043-maintenance-read-only:
	sh scripts/commit-tt043-maintenance-read-only.sh

.PHONY: push-tt043-maintenance-read-only
push-tt043-maintenance-read-only:
	sh scripts/push-tt043-maintenance-read-only.sh

.PHONY: benchmark-tt046-memory-accounting
benchmark-tt046-memory-accounting:
	sh scripts/benchmark-tt046-memory-accounting.sh

.PHONY: test-tt046-memory-accounting
test-tt046-memory-accounting:
	sh scripts/test-tt046-memory-accounting.sh

.PHONY: format-tt046-memory-accounting
format-tt046-memory-accounting:
	sh scripts/format-tt046-memory-accounting.sh

.PHONY: verify-tt046-docs
verify-tt046-docs:
	sh scripts/verify-tt046-docs.sh

.PHONY: race-tt046-memory-accounting
race-tt046-memory-accounting:
	sh scripts/race-tt046-memory-accounting.sh

.PHONY: vet-tt046-memory-accounting
vet-tt046-memory-accounting:
	sh scripts/vet-tt046-memory-accounting.sh

.PHONY: review-tt046-memory-accounting
review-tt046-memory-accounting:
	sh scripts/review-tt046-memory-accounting.sh

.PHONY: commit-tt046-memory-accounting
commit-tt046-memory-accounting:
	sh scripts/commit-tt046-memory-accounting.sh

.PHONY: push-tt046-memory-accounting
push-tt046-memory-accounting:
	sh scripts/push-tt046-memory-accounting.sh

.PHONY: amend-tt046-memory-accounting
amend-tt046-memory-accounting:
	sh scripts/amend-tt046-memory-accounting.sh

.PHONY: benchmark-ch027-scheduler-observability
benchmark-ch027-scheduler-observability:
	sh scripts/benchmark-ch027-scheduler-observability.sh

.PHONY: test-ch027-scheduler-observability
test-ch027-scheduler-observability:
	sh scripts/test-ch027-scheduler-observability.sh

.PHONY: format-ch027-scheduler-observability
format-ch027-scheduler-observability:
	sh scripts/format-ch027-scheduler-observability.sh

.PHONY: verify-ch027-docs
verify-ch027-docs:
	sh scripts/verify-ch027-docs.sh

.PHONY: race-ch027-scheduler-observability
race-ch027-scheduler-observability:
	sh scripts/race-ch027-scheduler-observability.sh

.PHONY: vet-ch027-scheduler-observability
vet-ch027-scheduler-observability:
	sh scripts/vet-ch027-scheduler-observability.sh

.PHONY: review-ch027-scheduler-observability
review-ch027-scheduler-observability:
	sh scripts/review-ch027-scheduler-observability.sh

.PHONY: commit-ch027-scheduler-observability
commit-ch027-scheduler-observability:
	sh scripts/commit-ch027-scheduler-observability.sh

.PHONY: push-ch027-scheduler-observability
push-ch027-scheduler-observability:
	sh scripts/push-ch027-scheduler-observability.sh
.PHONY: test-ch028-max-threads
test-ch028-max-threads:
	sh scripts/test-ch028-max-threads.sh

.PHONY: benchmark-ch028-max-threads
benchmark-ch028-max-threads:
	sh scripts/benchmark-ch028-max-threads.sh
.PHONY: format-ch028-max-threads
format-ch028-max-threads:
	sh scripts/format-ch028-max-threads.sh
.PHONY: race-ch028-max-threads
race-ch028-max-threads:
	sh scripts/race-ch028-max-threads.sh

.PHONY: vet-ch028-max-threads
vet-ch028-max-threads:
	sh scripts/vet-ch028-max-threads.sh
.PHONY: verify-ch028-docs
verify-ch028-docs:
	sh scripts/verify-ch028-docs.sh

.PHONY: review-ch028-max-threads
review-ch028-max-threads:
	sh scripts/review-ch028-max-threads.sh

.PHONY: commit-ch028-max-threads
commit-ch028-max-threads:
	sh scripts/commit-ch028-max-threads.sh

.PHONY: push-ch028-max-threads
push-ch028-max-threads:
	sh scripts/push-ch028-max-threads.sh
.PHONY: test-ch029-sql-quotas
test-ch029-sql-quotas:
	sh scripts/test-ch029-sql-quotas.sh

.PHONY: benchmark-ch029-sql-quotas
benchmark-ch029-sql-quotas:
	sh scripts/benchmark-ch029-sql-quotas.sh
.PHONY: format-ch029-sql-quotas
format-ch029-sql-quotas:
	sh scripts/format-ch029-sql-quotas.sh

.PHONY: race-ch029-sql-quotas
race-ch029-sql-quotas:
	sh scripts/race-ch029-sql-quotas.sh

.PHONY: vet-ch029-sql-quotas
vet-ch029-sql-quotas:
	sh scripts/vet-ch029-sql-quotas.sh
.PHONY: verify-ch029-docs
verify-ch029-docs:
	sh scripts/verify-ch029-docs.sh
.PHONY: review-ch029-sql-quotas
review-ch029-sql-quotas:
	sh scripts/review-ch029-sql-quotas.sh
.PHONY: commit-ch029-sql-quotas
commit-ch029-sql-quotas:
	sh scripts/commit-ch029-sql-quotas.sh

.PHONY: push-ch029-sql-quotas
push-ch029-sql-quotas:
	sh scripts/push-ch029-sql-quotas.sh
test-tg42-key-watchers:
	sh scripts/test-tg42-key-watchers.sh

benchmark-tg42-key-watchers:
	sh scripts/benchmark-tg42-key-watchers.sh

format-tg42-key-watchers:
	sh scripts/format-tg42-key-watchers.sh

test-tg42-package:
	sh scripts/test-tg42-package.sh

race-tg42-key-watchers:
	sh scripts/race-tg42-key-watchers.sh

vet-tg42-key-watchers:
	sh scripts/vet-tg42-key-watchers.sh

review-tg42-key-watchers:
	sh scripts/review-tg42-key-watchers.sh

commit-tg42-key-watchers:
	sh scripts/commit-tg42-key-watchers.sh

push-tg42-key-watchers:
	sh scripts/push-tg42-key-watchers.sh

.PHONY: test-tg42-key-watchers benchmark-tg42-key-watchers format-tg42-key-watchers test-tg42-package race-tg42-key-watchers vet-tg42-key-watchers review-tg42-key-watchers commit-tg42-key-watchers push-tg42-key-watchers

.PHONY: benchmark-journal-replay
benchmark-journal-replay:
	sh scripts/benchmark-journal-replay.sh

.PHONY: test-journal-replay
test-journal-replay:
	sh scripts/test-journal-replay.sh

.PHONY: format-journal-replay
format-journal-replay:
	sh scripts/format-journal-replay.sh

.PHONY: review-journal-replay
review-journal-replay:
	sh scripts/review-journal-replay.sh

.PHONY: commit-journal-replay
commit-journal-replay:
	sh scripts/commit-journal-replay.sh

.PHONY: push-journal-replay
push-journal-replay:
	sh scripts/push-journal-replay.sh


.PHONY: benchmark-visibility-queue
benchmark-visibility-queue:
	sh scripts/benchmark-visibility-queue.sh

.PHONY: test-visibility-queue
test-visibility-queue:
	sh scripts/test-visibility-queue.sh

.PHONY: format-visibility-queue
format-visibility-queue:
	sh scripts/format-visibility-queue.sh

.PHONY: test-visibility-queue-package
test-visibility-queue-package:
	sh scripts/test-visibility-queue-package.sh

.PHONY: race-visibility-queue
race-visibility-queue:
	sh scripts/race-visibility-queue.sh

.PHONY: vet-visibility-queue
vet-visibility-queue:
	sh scripts/vet-visibility-queue.sh

.PHONY: review-visibility-queue
review-visibility-queue:
	sh scripts/review-visibility-queue.sh

.PHONY: commit-visibility-queue
commit-visibility-queue:
	sh scripts/commit-visibility-queue.sh

.PHONY: push-visibility-queue
push-visibility-queue:
	sh scripts/push-visibility-queue.sh

.PHONY: review-visibility-fencing
review-visibility-fencing:
	sh scripts/review-visibility-fencing.sh

.PHONY: commit-visibility-fencing
commit-visibility-fencing:
	sh scripts/commit-visibility-fencing.sh

.PHONY: push-visibility-fencing
push-visibility-fencing:
	sh scripts/push-visibility-fencing.sh



.PHONY: test-connection-pool
test-connection-pool:
	sh scripts/test-connection-pool.sh

.PHONY: benchmark-connection-pool
benchmark-connection-pool:
	sh scripts/benchmark-connection-pool.sh

.PHONY: format-connection-pool
format-connection-pool:
	sh scripts/format-connection-pool.sh

.PHONY: test-connection-pool-package
test-connection-pool-package:
	sh scripts/test-connection-pool-package.sh

.PHONY: race-connection-pool
race-connection-pool:
	sh scripts/race-connection-pool.sh

.PHONY: vet-connection-pool
vet-connection-pool:
	sh scripts/vet-connection-pool.sh

.PHONY: review-connection-pool
review-connection-pool:
	sh scripts/review-connection-pool.sh

.PHONY: commit-connection-pool
commit-connection-pool:
	sh scripts/commit-connection-pool.sh

.PHONY: push-connection-pool
push-connection-pool:
	sh scripts/push-connection-pool.sh
.PHONY: review-replay-rollback commit-replay-rollback push-replay-rollback

review-replay-rollback:
	sh scripts/review-replay-rollback.sh

commit-replay-rollback:
	sh scripts/commit-replay-rollback.sh

push-replay-rollback:
	sh scripts/push-replay-rollback.sh
.PHONY: inspect-open-inspiration-local-clean

inspect-open-inspiration-local-clean:
	sh scripts/inspect-open-inspiration-local-clean.sh

.PHONY: inspect-files

inspect-files:
	sh scripts/inspect-files.sh $(FILES)

.PHONY: search-files

search-files:
	sh scripts/search-files.sh "$(PATTERN)" $(FILES)

.PHONY: inspect-timestamp-ordering

inspect-timestamp-ordering:
	sh scripts/inspect-timestamp-ordering.sh

.PHONY: review-inspection-tools commit-inspection-tools push-inspection-tools

review-inspection-tools:
	sh scripts/review-inspection-tools.sh

commit-inspection-tools:
	sh scripts/commit-inspection-tools.sh

push-inspection-tools:
	sh scripts/push-inspection-tools.sh

.PHONY: test-write-quorum-fastpath benchmark-write-quorum-fastpath

test-write-quorum-fastpath:
	sh scripts/test-write-quorum-fastpath.sh

benchmark-write-quorum-fastpath:
	sh scripts/benchmark-write-quorum-fastpath.sh

.PHONY: format-write-quorum-fastpath race-write-quorum-fastpath vet-write-quorum-fastpath

format-write-quorum-fastpath:
	sh scripts/format-write-quorum-fastpath.sh

race-write-quorum-fastpath:
	sh scripts/race-write-quorum-fastpath.sh

vet-write-quorum-fastpath:
	sh scripts/vet-write-quorum-fastpath.sh

.PHONY: review-write-quorum-fastpath commit-write-quorum-fastpath push-write-quorum-fastpath

review-write-quorum-fastpath:
	sh scripts/review-write-quorum-fastpath.sh

commit-write-quorum-fastpath:
	sh scripts/commit-write-quorum-fastpath.sh

push-write-quorum-fastpath:
	sh scripts/push-write-quorum-fastpath.sh

.PHONY: remote-status

remote-status:
	sh scripts/remote-status.sh

.PHONY: test-visibility-queue-epoch

test-visibility-queue-epoch:
	sh scripts/test-visibility-queue-epoch.sh

.PHONY: benchmark-visibility-queue-epoch

benchmark-visibility-queue-epoch:
	sh scripts/benchmark-visibility-queue-epoch.sh

.PHONY: format-visibility-queue-epoch race-visibility-queue-epoch vet-visibility-queue-epoch

format-visibility-queue-epoch:
	sh scripts/format-visibility-queue-epoch.sh

race-visibility-queue-epoch:
	sh scripts/race-visibility-queue-epoch.sh

vet-visibility-queue-epoch:
	sh scripts/vet-visibility-queue-epoch.sh

.PHONY: review-visibility-queue-epoch commit-visibility-queue-epoch push-visibility-queue-epoch

review-visibility-queue-epoch:
	sh scripts/review-visibility-queue-epoch.sh

commit-visibility-queue-epoch:
	sh scripts/commit-visibility-queue-epoch.sh

push-visibility-queue-epoch:
	sh scripts/push-visibility-queue-epoch.sh

.PHONY: test-monitoring-command-client
test-monitoring-command-client:
	sh scripts/test-monitoring-command-client.sh

.PHONY: format-monitoring-command-client
format-monitoring-command-client:
	sh scripts/format-monitoring-command-client.sh

.PHONY: benchmark-monitoring-command-client
benchmark-monitoring-command-client:
	sh scripts/benchmark-monitoring-command-client.sh

.PHONY: vet-monitoring-command-client
vet-monitoring-command-client:
	sh scripts/vet-monitoring-command-client.sh

.PHONY: race-monitoring-command-client
race-monitoring-command-client:
	sh scripts/race-monitoring-command-client.sh

.PHONY: review-monitoring-command-client
review-monitoring-command-client:
	sh scripts/review-monitoring-command-client.sh

.PHONY: commit-monitoring-command-client
commit-monitoring-command-client:
	sh scripts/commit-monitoring-command-client.sh

.PHONY: push-monitoring-command-client
push-monitoring-command-client:
	sh scripts/push-monitoring-command-client.sh

.PHONY: review-monitoring-command-batch
review-monitoring-command-batch:
	sh scripts/review-monitoring-command-batch.sh

.PHONY: commit-monitoring-command-batch
commit-monitoring-command-batch:
	sh scripts/commit-monitoring-command-batch.sh

.PHONY: push-monitoring-command-batch
push-monitoring-command-batch:
	sh scripts/push-monitoring-command-batch.sh

.PHONY: review-t103-decision
review-t103-decision:
	sh scripts/review-t103-decision.sh

.PHONY: commit-t103-decision
commit-t103-decision:
	sh scripts/commit-t103-decision.sh

.PHONY: push-t103-decision
push-t103-decision:
	sh scripts/push-t103-decision.sh
.PHONY: review-monitoring-command-wire
review-monitoring-command-wire:
	sh scripts/review-monitoring-command-wire.sh

.PHONY: commit-monitoring-command-wire
commit-monitoring-command-wire:
	sh scripts/commit-monitoring-command-wire.sh

.PHONY: push-monitoring-command-wire
push-monitoring-command-wire:
	sh scripts/push-monitoring-command-wire.sh

.PHONY: audit-inspiration-inventory
audit-inspiration-inventory:
	sh scripts/audit-inspiration-inventory.sh

.PHONY: verify-inspiration-backlog
verify-inspiration-backlog:
	sh scripts/audit-inspiration-inventory.sh

.PHONY: review-inspiration-backlog
review-inspiration-backlog:
	sh scripts/review-inspiration-backlog.sh

.PHONY: commit-inspiration-backlog
commit-inspiration-backlog:
	sh scripts/commit-inspiration-backlog.sh

.PHONY: push-inspiration-backlog
push-inspiration-backlog:
	sh scripts/push-inspiration-backlog.sh

.PHONY: test-t-async-batcher
test-t-async-batcher:
	bash ./scripts/test-t-async-batcher.sh

.PHONY: format-t-async-batcher
format-t-async-batcher:
	bash ./scripts/format-t-async-batcher.sh

.PHONY: test-t-async-batcher-package
test-t-async-batcher-package:
	bash ./scripts/test-t-async-batcher-package.sh

.PHONY: race-t-async-batcher
race-t-async-batcher:
	bash ./scripts/race-t-async-batcher.sh

.PHONY: vet-t-async-batcher
vet-t-async-batcher:
	bash ./scripts/vet-t-async-batcher.sh

.PHONY: benchmark-t-async-batcher
benchmark-t-async-batcher:
	bash ./scripts/benchmark-t-async-batcher.sh

.PHONY: commit-t-async-batcher
commit-t-async-batcher:
	bash ./scripts/commit-t-async-batcher.sh

.PHONY: publish-t-async-batcher
publish-t-async-batcher:
	bash ./scripts/publish-t-async-batcher.sh

.PHONY: audit-product-idea-gaps
audit-product-idea-gaps:
	bash ./scripts/audit-product-idea-gaps.sh

.PHONY: test-t-peer-pool format-t-peer-pool test-t-peer-pool-package race-t-peer-pool vet-t-peer-pool benchmark-t-peer-pool publish-t-peer-pool
test-t-peer-pool:
	bash ./scripts/test-t-peer-pool.sh
format-t-peer-pool:
	bash ./scripts/format-t-peer-pool.sh
test-t-peer-pool-package:
	bash ./scripts/test-t-peer-pool-package.sh
race-t-peer-pool:
	bash ./scripts/race-t-peer-pool.sh
vet-t-peer-pool:
	bash ./scripts/vet-t-peer-pool.sh
benchmark-t-peer-pool:
	bash ./scripts/benchmark-t-peer-pool.sh
publish-t-peer-pool:
	bash ./scripts/publish-t-peer-pool.sh

.PHONY: test-t-peer-breaker benchmark-t-peer-breaker
test-t-peer-breaker:
	bash ./scripts/test-t-peer-breaker.sh
benchmark-t-peer-breaker:
	bash ./scripts/benchmark-t-peer-breaker.sh

.PHONY: test-mu01-connector-lifecycle
test-mu01-connector-lifecycle:
	bash ./scripts/test-mu01-connector-lifecycle.sh

.PHONY: format-mu01-connector-lifecycle
format-mu01-connector-lifecycle:
	bash ./scripts/format-mu01-connector-lifecycle.sh

.PHONY: benchmark-mu01-connector-lifecycle
benchmark-mu01-connector-lifecycle:
	bash ./scripts/benchmark-mu01-connector-lifecycle.sh

.PHONY: test-mu01-connector-package
test-mu01-connector-package:
	bash ./scripts/test-mu01-connector-package.sh

.PHONY: race-mu01-connector-lifecycle
race-mu01-connector-lifecycle:
	bash ./scripts/race-mu01-connector-lifecycle.sh

.PHONY: vet-mu01-connector-lifecycle
vet-mu01-connector-lifecycle:
	bash ./scripts/vet-mu01-connector-lifecycle.sh

.PHONY: test-t-u02-compact-protocol
test-t-u02-compact-protocol:
	bash ./scripts/test-t-u02-compact-protocol.sh

.PHONY: format-t-u02-compact-protocol
format-t-u02-compact-protocol:
	bash ./scripts/format-t-u02-compact-protocol.sh

.PHONY: benchmark-t-u02-compact-protocol
benchmark-t-u02-compact-protocol:
	bash ./scripts/benchmark-t-u02-compact-protocol.sh

.PHONY: measure-t-u02-wire-size
measure-t-u02-wire-size:
	bash ./scripts/measure-t-u02-wire-size.sh

.PHONY: test-t-u02-compact-package
test-t-u02-compact-package:
	bash ./scripts/test-t-u02-compact-package.sh

.PHONY: race-t-u02-compact-protocol
race-t-u02-compact-protocol:
	bash ./scripts/race-t-u02-compact-protocol.sh

.PHONY: vet-t-u02-compact-protocol
vet-t-u02-compact-protocol:
	bash ./scripts/vet-t-u02-compact-protocol.sh

.PHONY: test-m-u09-frontier
test-m-u09-frontier:
	bash ./scripts/test-m-u09-frontier.sh

.PHONY: format-m-u09-frontier
format-m-u09-frontier:
	bash ./scripts/format-m-u09-frontier.sh

.PHONY: benchmark-m-u09-frontier
benchmark-m-u09-frontier:
	bash ./scripts/benchmark-m-u09-frontier.sh

.PHONY: test-m-u09-package
test-m-u09-package:
	bash ./scripts/test-m-u09-package.sh

.PHONY: race-m-u09-frontier
race-m-u09-frontier:
	bash ./scripts/race-m-u09-frontier.sh

.PHONY: vet-m-u09-frontier
vet-m-u09-frontier:
	bash ./scripts/vet-m-u09-frontier.sh

.PHONY: format-t-u02-session
format-t-u02-session:
	bash ./scripts/format-t-u02-session.sh

.PHONY: test-t-u02-session
test-t-u02-session:
	bash ./scripts/test-t-u02-session.sh

.PHONY: test-t-u02-session-package
test-t-u02-session-package:
	bash ./scripts/test-t-u02-session-package.sh

.PHONY: race-t-u02-session
race-t-u02-session:
	bash ./scripts/race-t-u02-session.sh

.PHONY: vet-t-u02-session
vet-t-u02-session:
	bash ./scripts/vet-t-u02-session.sh

.PHONY: benchmark-t-u02-session
benchmark-t-u02-session:
	bash ./scripts/benchmark-t-u02-session.sh

.PHONY: publish-t-u02-session
publish-t-u02-session:
	bash ./scripts/publish-t-u02-session.sh
.PHONY: format-t-u28-lifecycle
format-t-u28-lifecycle:
	@bash scripts/format-t-u28-lifecycle.sh
.PHONY: test-t-u28-lifecycle
test-t-u28-lifecycle:
	@bash scripts/test-t-u28-lifecycle.sh
.PHONY: test-t-u28-package
test-t-u28-package:
	@bash scripts/test-t-u28-package.sh
.PHONY: race-t-u28-lifecycle
race-t-u28-lifecycle:
	@bash scripts/race-t-u28-lifecycle.sh
.PHONY: vet-t-u28-lifecycle
vet-t-u28-lifecycle:
	@bash scripts/vet-t-u28-lifecycle.sh
.PHONY: benchmark-t-u28-lifecycle
benchmark-t-u28-lifecycle:
	@bash scripts/benchmark-t-u28-lifecycle.sh
.PHONY: publish-t-u28-lifecycle
publish-t-u28-lifecycle:
	@bash scripts/publish-t-u28-lifecycle.sh
.PHONY: format-t-u29-stream
format-t-u29-stream:
	@bash scripts/format-t-u29-stream.sh
.PHONY: test-t-u29-stream
test-t-u29-stream:
	@bash scripts/test-t-u29-stream.sh
.PHONY: test-t-u29-package
test-t-u29-package:
	@bash scripts/test-t-u29-package.sh
.PHONY: race-t-u29-stream
race-t-u29-stream:
	@bash scripts/race-t-u29-stream.sh
.PHONY: vet-t-u29-stream
vet-t-u29-stream:
	@bash scripts/vet-t-u29-stream.sh
.PHONY: benchmark-t-u29-stream
benchmark-t-u29-stream:
	@bash scripts/benchmark-t-u29-stream.sh
.PHONY: publish-t-u29-stream
publish-t-u29-stream:
	@bash scripts/publish-t-u29-stream.sh
.PHONY: format-m-u09-snapshot
format-m-u09-snapshot:
	@bash scripts/format-m-u09-snapshot.sh
.PHONY: test-m-u09-snapshot
test-m-u09-snapshot:
	@bash scripts/test-m-u09-snapshot.sh
.PHONY: race-m-u09-snapshot
race-m-u09-snapshot:
	@bash scripts/race-m-u09-snapshot.sh
.PHONY: vet-m-u09-snapshot
vet-m-u09-snapshot:
	@bash scripts/vet-m-u09-snapshot.sh
.PHONY: benchmark-m-u09-snapshot
benchmark-m-u09-snapshot:
	@bash scripts/benchmark-m-u09-snapshot.sh
.PHONY: publish-m-u09-snapshot
publish-m-u09-snapshot:
	@bash scripts/publish-m-u09-snapshot.sh
.PHONY: format-m-u33-retention
format-m-u33-retention:
	@bash scripts/format-m-u33-retention.sh
.PHONY: test-m-u33-retention
test-m-u33-retention:
	@bash scripts/test-m-u33-retention.sh
.PHONY: test-m-u33-package
test-m-u33-package:
	@bash scripts/test-m-u33-package.sh
.PHONY: race-m-u33-retention
race-m-u33-retention:
	@bash scripts/race-m-u33-retention.sh
.PHONY: vet-m-u33-retention
vet-m-u33-retention:
	@bash scripts/vet-m-u33-retention.sh
.PHONY: benchmark-m-u33-retention
benchmark-m-u33-retention:
	@bash scripts/benchmark-m-u33-retention.sh
.PHONY: publish-m-u33-retention
publish-m-u33-retention:
	@bash scripts/publish-m-u33-retention.sh

.PHONY: format-t-u40
format-t-u40:
	@bash scripts/format-t-u40.sh

.PHONY: test-t-u40
test-t-u40:
	@bash scripts/test-t-u40.sh

.PHONY: benchmark-t-u40
benchmark-t-u40:
	@bash scripts/benchmark-t-u40.sh

.PHONY: publish-t-u40
publish-t-u40:
	@bash scripts/publish-t-u40.sh

.PHONY: format-t-u41
format-t-u41:
	@bash scripts/format-t-u41.sh

.PHONY: test-t-u41
test-t-u41:
	@bash scripts/test-t-u41.sh

.PHONY: benchmark-t-u41
benchmark-t-u41:
	@bash scripts/benchmark-t-u41.sh

.PHONY: publish-t-u41
publish-t-u41:
	@bash scripts/publish-t-u41.sh

.PHONY: format-t-u43
format-t-u43:
	@bash scripts/format-t-u43.sh

.PHONY: test-t-u43
test-t-u43:
	@bash scripts/test-t-u43.sh

.PHONY: benchmark-t-u43
benchmark-t-u43:
	@bash scripts/benchmark-t-u43.sh

.PHONY: publish-t-u43
publish-t-u43:
	@bash scripts/publish-t-u43.sh

.PHONY: test-t-u45
test-t-u45:
	@bash scripts/test-t-u45.sh

.PHONY: format-t-u45
format-t-u45:
	@bash scripts/format-t-u45.sh

.PHONY: benchmark-t-u45
benchmark-t-u45:
	@bash scripts/benchmark-t-u45.sh

.PHONY: publish-t-u45
publish-t-u45:
	@bash scripts/publish-t-u45.sh

.PHONY: test-t-u47
test-t-u47:
	@bash scripts/test-t-u47.sh

.PHONY: format-t-u47
format-t-u47:
	@bash scripts/format-t-u47.sh

.PHONY: benchmark-t-u47
benchmark-t-u47:
	@bash scripts/benchmark-t-u47.sh

.PHONY: publish-t-u47
publish-t-u47:
	@bash scripts/publish-t-u47.sh

.PHONY: test-t-u46
test-t-u46:
	@bash scripts/test-t-u46.sh

.PHONY: format-t-u46
format-t-u46:
	@bash scripts/format-t-u46.sh

.PHONY: benchmark-t-u46
benchmark-t-u46:
	@bash scripts/benchmark-t-u46.sh

.PHONY: publish-t-u46
publish-t-u46:
	@bash scripts/publish-t-u46.sh

.PHONY: test-t-u48
test-t-u48:
	@bash scripts/test-t-u48.sh

.PHONY: format-t-u48
format-t-u48:
	@bash scripts/format-t-u48.sh

.PHONY: benchmark-t-u48
benchmark-t-u48:
	@bash scripts/benchmark-t-u48.sh

.PHONY: publish-t-u48
publish-t-u48:
	@bash scripts/publish-t-u48.sh

benchmark-t-u49-baseline:
	bash ./scripts/benchmark-t-u49-baseline.sh

test-t-u49:
	bash ./scripts/test-t-u49.sh

format-t-u49:
	bash ./scripts/format-t-u49.sh

benchmark-t-u49:
	bash ./scripts/benchmark-t-u49.sh

verify-t-u49:
	bash ./scripts/verify-t-u49.sh

test-t-u50:
	bash ./scripts/test-t-u50.sh

format-t-u50:
	bash ./scripts/format-t-u50.sh

benchmark-t-u50:
	bash ./scripts/benchmark-t-u50.sh

verify-t-u50:
	bash ./scripts/verify-t-u50.sh

test-t-u52:
	bash ./scripts/test-t-u52.sh

format-t-u52:
	bash ./scripts/format-t-u52.sh

benchmark-t-u52:
	bash ./scripts/benchmark-t-u52.sh

verify-t-u52:
	bash ./scripts/verify-t-u52.sh

test-t-u02:
	bash ./scripts/test-t-u02.sh

format-t-u02:
	bash ./scripts/format-t-u02.sh

benchmark-t-u02:
	bash ./scripts/benchmark-t-u02.sh

verify-t-u02:
	bash ./scripts/verify-t-u02.sh

test-t-u07:
	bash ./scripts/test-t-u07.sh

format-t-u07:
	bash ./scripts/format-t-u07.sh

benchmark-t-u07:
	bash ./scripts/benchmark-t-u07.sh

verify-t-u07:
	bash ./scripts/verify-t-u07.sh

test-t-u08:
	bash ./scripts/test-t-u08.sh

format-t-u08:
	bash ./scripts/format-t-u08.sh

benchmark-t-u08:
	bash ./scripts/benchmark-t-u08.sh

verify-t-u08:
	bash ./scripts/verify-t-u08.sh

test-t-u11:
	bash ./scripts/test-t-u11.sh

format-t-u11:
	bash ./scripts/format-t-u11.sh

benchmark-t-u11:
	bash ./scripts/benchmark-t-u11.sh

verify-t-u11:
	bash ./scripts/verify-t-u11.sh

test-t-u15:
	bash ./scripts/test-t-u15.sh

format-t-u15:
	bash ./scripts/format-t-u15.sh

benchmark-t-u15:
	bash ./scripts/benchmark-t-u15.sh

verify-t-u15:
	bash ./scripts/verify-t-u15.sh

format-t-u42:
	bash ./scripts/format-t-u42.sh

test-t-u42:
	bash ./scripts/test-t-u42.sh

benchmark-t-u42:
	bash ./scripts/benchmark-t-u42.sh

verify-t-u42:
	bash ./scripts/verify-t-u42.sh

format-m201:
	bash ./scripts/format-m201.sh

test-m201:
	bash ./scripts/test-m201.sh

benchmark-m201:
	bash ./scripts/benchmark-m201.sh

verify-m201:
	bash ./scripts/verify-m201.sh

format-m202:
	bash ./scripts/format-m202.sh

test-m202:
	bash ./scripts/test-m202.sh

benchmark-m202:
	bash ./scripts/benchmark-m202.sh

verify-m202:
	bash ./scripts/verify-m202.sh
.PHONY: test-m207
test-m207:
	sh scripts/test-m207.sh

.PHONY: benchmark-m207
benchmark-m207:
	sh scripts/benchmark-m207.sh

.PHONY: format-m207
format-m207:
	sh scripts/format-m207.sh

.PHONY: verify-m207
verify-m207:
	sh scripts/verify-m207.sh
.PHONY: test-t239
test-t239:
	sh scripts/test-t239.sh

.PHONY: benchmark-t239
benchmark-t239:
	sh scripts/benchmark-t239.sh

.PHONY: format-t239
format-t239:
	sh scripts/format-t239.sh

.PHONY: verify-t239
verify-t239:
	sh scripts/verify-t239.sh
