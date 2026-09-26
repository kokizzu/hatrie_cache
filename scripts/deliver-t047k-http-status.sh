#!/usr/bin/env bash
set -euo pipefail

mode="${1:-plan}"
commit_message='chore: add T047k delivery helper [skip ci]'

safe_paths=(
    T047K_HTTP_STATUS.md
    scripts/benchmark-t047k-http-status.sh
    scripts/deliver-t047k-http-status.sh
    hat/hatCache/http_cluster_write_commit.go
    hat/hatCache/tu47_cluster_write_commit_http_test.go
    hat/hatCache/tu47_cluster_write_commit_http_benchmark_test.go
)

stage_feature() {
    if ! git diff --cached --quiet; then
        printf '%s\n' 'refusing to stage: the index already contains unrelated staged changes' >&2
        exit 1
    fi
    if ! git grep -q 'benchmark-t047k-http-status' HEAD -- Makefile; then
    git apply --cached --whitespace=nowarn <<'PATCH'
diff --git a/Makefile b/Makefile
--- a/Makefile
+++ b/Makefile
@@ -28188,7 +28188,15 @@
 .PHONY: deliver-t047j-http-transport
 deliver-t047j-http-transport:
 	bash ./scripts/deliver-t047j-http-transport.sh deliver
 
+.PHONY: benchmark-t047k-http-status
+benchmark-t047k-http-status:
+	bash ./scripts/benchmark-t047k-http-status.sh
+
+.PHONY: deliver-t047k-http-status
+deliver-t047k-http-status:
+	bash ./scripts/deliver-t047k-http-status.sh deliver
+
 .PHONY: test-m032-snapshot-provider
 test-m032-snapshot-provider:
 	@bash scripts/test-m032-snapshot-provider.sh
diff --git a/README.md b/README.md
--- a/README.md
+++ b/README.md
@@ -81,4 +81,5 @@ security guidance before exposing it on a network.
 - Opt-in journal-wide synchronous write quorum bound to exact journal sequences and fence tokens: [TU10_JOURNAL_WRITE_QUORUM.md](TU10_JOURNAL_WRITE_QUORUM.md), with measured validation and execution costs in [BENCHMARK.md](BENCHMARK.md#t-u10-journal-wide-synchronous-write-quorum)
 - Opt-in two-phase cluster write commit with prepare barriers and explicit indeterminate-outcome handling: [T047_CLUSTER_WRITE_COMMIT.md](T047_CLUSTER_WRITE_COMMIT.md), with measured control-plane tradeoffs in [BENCHMARK.md](BENCHMARK.md#t047-cluster-write-commit)
+- Opt-in authenticated HTTP participant status reads for T047 recovery: [T047K_HTTP_STATUS.md](T047K_HTTP_STATUS.md)
 - Opt-in durable generation-fenced cluster membership journal with atomic recovery: [TU13_DURABLE_CLUSTER_MEMBERSHIP.md](TU13_DURABLE_CLUSTER_MEMBERSHIP.md), with bounded snapshot and persistence measurements in [BENCHMARK.md](BENCHMARK.md#t-u13-durable-cluster-membership)
 - Opt-in durable tuple field-operation journal with bounded replay and atomic recovery: [TU19_DURABLE_TUPLE_OPERATION_JOURNAL.md](TU19_DURABLE_TUPLE_OPERATION_JOURNAL.md), with measured in-memory and fsync-backed costs in [BENCHMARK.md](BENCHMARK.md#t-u19-durable-tuple-field-operation-journal)
diff --git a/INSPIRATION.md b/INSPIRATION.md
--- a/INSPIRATION.md
+++ b/INSPIRATION.md
@@ -778,4 +778,5 @@ explicit regional partitioning and simple backups over automatic sharding.
 - [x] T047h gRPC cluster-write phase transport. `CacheService.ClusterWriteCommit` exposes authenticated prepare/commit/abort phases over an opt-in participant endpoint, and `ExecuteClusterWriteCommitOverGRPC` reuses one caller-owned client per participant; TLS, dialing, automatic command wiring, and connection cleanup remain caller-owned. See [T047H_GRPC_PHASE_TRANSPORT.md](T047H_GRPC_PHASE_TRANSPORT.md).
 - [x] T047i Coordinator-owned durable write state. `ExecuteClusterWriteCommitWithStateStore` persists proposed, prepared, commit-started, and terminal phase boundaries through an atomic CRC32C file store; the default coordinator path remains unchanged and recovery reconciliation is explicit. See [T047I_COORDINATOR_DURABILITY.md](T047I_COORDINATOR_DURABILITY.md).
 - [x] T047j Opt-in HTTP prepare/commit/abort phase transport. `ClusterWriteCommitHTTPHandler` and `ExecuteClusterWriteCommitOverHTTP` provide strict bounded JSON requests, constant-time token authentication, and caller-owned HTTP/TLS lifecycle without registering a default route; see [T047J_HTTP_PHASE_TRANSPORT.md](T047J_HTTP_PHASE_TRANSPORT.md).
+- [x] T047k Authenticated HTTP participant status reads. `ClusterWriteCommitHTTPClient.Status` and the opt-in handler `GET` path expose bounded read-only participant records for recovery after an unknown coordinator outcome; unknown query fields are rejected and no default route or mutation is added. See [T047K_HTTP_STATUS.md](T047K_HTTP_STATUS.md).
 - [x] T048 Replication sets and peer topology.
diff --git a/CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md b/CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md
--- a/CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md
+++ b/CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md
@@ -268,4 +268,5 @@ records a separate implementation boundary.
 - [ ] T047 Synchronous replication with an explicit quorum. The public single-command path is now opt-in through `MonitoringOptions.WriteQuorum` / `CacheGRPCOptions.WriteQuorum`; atomic `BATCH` quorum semantics are implemented, while end-to-end transport wiring, durable participant state, and reconciliation remain open.
 - [x] T047e Transport-neutral two-phase cluster write commit with a prepare barrier and explicit indeterminate commit outcome; see [T047_CLUSTER_WRITE_COMMIT.md](T047_CLUSTER_WRITE_COMMIT.md).
 - [x] T047f Bounded durable participant phase state for two-phase writes. `ClusterWriteCommitParticipant` provides idempotent prepare/commit/abort transitions, deterministic bounded `HCP1` snapshots, strict restore validation, and atomic replacement; transport wiring, application-data durability, and reconciliation policy remain caller-owned. See [T047_PARTICIPANT_STATE.md](T047_PARTICIPANT_STATE.md).
+- [x] T047k Authenticated HTTP participant status reads for recovery inspection; see [T047K_HTTP_STATUS.md](T047K_HTTP_STATUS.md).
 - [ ] T103 Native FFI extension boundary.
diff --git a/BENCHMARK.md b/BENCHMARK.md
--- a/BENCHMARK.md
+++ b/BENCHMARK.md
@@ -38888,3 +38888,19 @@
 The incremental helper retains exact string multiplicities and is opt-in; the
 existing int64 path, SQL planner, and default behavior remain unchanged. See
 [M037L_DIFFERENTIAL_STRING_COUNT_DISTINCT.md](M037L_DIFFERENTIAL_STRING_COUNT_DISTINCT.md).
+<a id="t047k-http-status"></a>
+## T047k HTTP participant status
+
+Command: `make benchmark-t047k-http-status`.
+
+Measured on Linux/amd64, AMD Ryzen 9 5950X, with `-benchtime=200ms -count=3`:
+
+| Path | Median ns/op | B/op | allocs/op | Relative latency |
+| --- | ---: | ---: | ---: | ---: |
+| Direct participant `Status` | 19.1 | 0 | 0 | 1.00x |
+| HTTP status transport | 70,085 | 10,822 | 98 | 3,669x slower |
+
+The status endpoint is a recovery/control-plane operation and is opt-in. The
+existing HTTP phase path showed no measurable regression: its median changed
+from `159,493 ns/op`, `28,970 B/op`, `254 allocs/op` to `158,121 ns/op`,
+`28,970 B/op`, `254 allocs/op` across three-run samples.
PATCH
    fi
    git add -- "${safe_paths[@]}"
    git diff --cached --check
}

case "$mode" in
plan)
    git status --short --branch
    git diff --check
    printf '%s\n' 'T047k feature paths:' "${safe_paths[@]}" 'shared paths are staged by exact hunks during deliver'
    ;;
stage)
    stage_feature
    git status --short
    git diff --cached --stat
    ;;
commit)
    git commit -m "$commit_message"
    ;;
push)
    git push origin HEAD
    ;;
deliver)
    stage_feature
    git commit -m "$commit_message"
    git push origin HEAD
    ;;
*)
    printf 'usage: %s {plan|stage|commit|push|deliver}\n' "$0" >&2
    exit 2
    ;;
esac
