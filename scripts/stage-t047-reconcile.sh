#!/usr/bin/env bash
set -euo pipefail

repo_root=$(git rev-parse --show-toplevel)
cd "$repo_root"

tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-t047-stage.XXXXXX")
trap 'rm -rf "$tmp_dir"' EXIT

feature_paths=(
  BENCHMARK.md
  INSPIRATION.md
  Makefile
  T047_PARTICIPANT_RECONCILIATION.md
  hat/hatReplication/tu47_cluster_write_commit_reconcile.go
  hat/hatReplication/tu47_cluster_write_commit_reconcile_benchmark_test.go
  hat/hatReplication/tu47_cluster_write_commit_reconcile_test.go
  scripts/benchmark-t047-reconcile-baseline.sh
  scripts/benchmark-t047-reconcile.sh
  scripts/commit-t047-reconcile.sh
  scripts/format-t047-reconcile.sh
  scripts/push-t047-reconcile.sh
  scripts/race-t047-reconcile.sh
  scripts/stage-t047-reconcile.sh
  scripts/test-t047-reconcile-package.sh
  scripts/test-t047-reconcile.sh
  scripts/vet-t047-reconcile.sh
)

git show HEAD:Makefile > "$tmp_dir/Makefile"
cat >> "$tmp_dir/Makefile" <<'EOF'

benchmark-t047-reconcile-baseline:

	bash scripts/benchmark-t047-reconcile-baseline.sh

test-t047-reconcile:

	bash scripts/test-t047-reconcile.sh

format-t047-reconcile:

	bash scripts/format-t047-reconcile.sh

benchmark-t047-reconcile:

	bash scripts/benchmark-t047-reconcile.sh

test-t047-reconcile-package:

	bash scripts/test-t047-reconcile-package.sh

race-t047-reconcile:

	bash scripts/race-t047-reconcile.sh

vet-t047-reconcile:

	bash scripts/vet-t047-reconcile.sh

stage-t047-reconcile:

	bash scripts/stage-t047-reconcile.sh

commit-t047-reconcile:

	bash scripts/commit-t047-reconcile.sh

push-t047-reconcile:

	bash scripts/push-t047-reconcile.sh
EOF
makefile_blob=$(git hash-object -w "$tmp_dir/Makefile")
git update-index --add --cacheinfo "100644,$makefile_blob,Makefile"

git show HEAD:INSPIRATION.md > "$tmp_dir/INSPIRATION.md"
old_t047='- [ ] T047 Synchronous replication with an explicit quorum. The public single-command path and atomic `BATCH` quorum semantics are implemented and tested through opt-in `MonitoringOptions.WriteQuorum` / `CacheGRPCOptions.WriteQuorum`; end-to-end transport wiring, durable participant state, and reconciliation remain open.'
new_t047='- [ ] T047 Synchronous replication with an explicit quorum. The public single-command path and atomic `BATCH` quorum semantics are implemented and tested through opt-in `MonitoringOptions.WriteQuorum` / `CacheGRPCOptions.WriteQuorum`; transport wiring and coordinator-owned durability remain open, while participant snapshots and batch reconciliation are available as caller-owned primitives.'
t047f='- [x] T047f Ordered atomic participant reconciliation. `ClusterWriteCommitParticipant.PreparedRecords` exposes canonical prepared state and `Reconcile` applies validated commit/abort decisions in one lock-protected batch with idempotent terminal repeats and no mutation on validation failure. See [T047_PARTICIPANT_RECONCILIATION.md](T047_PARTICIPANT_RECONCILIATION.md).'
awk -v old="$old_t047" -v replacement="$new_t047" -v row="$t047f" '
  $0 == old { print replacement; replaced++; next }
  { print }
  index($0, "- [x] T047e ") == 1 { print row; inserted++ }
  END {
    if (replaced != 1 || inserted != 1) exit 41
  }
' "$tmp_dir/INSPIRATION.md" > "$tmp_dir/INSPIRATION.updated.md"
inspiration_blob=$(git hash-object -w "$tmp_dir/INSPIRATION.updated.md")
git update-index --add --cacheinfo "100644,$inspiration_blob,INSPIRATION.md"

cat > "$tmp_dir/BENCHMARK.section" <<'EOF'
<a id="t047f-participant-reconciliation"></a>
## T047f Participant Reconciliation

Command: `make benchmark-t047-reconcile`.

This paired benchmark reconciles 32 prepared transactions after restoring the
same prepared snapshot. The existing path calls `Status` and `Commit` once per
transaction; the new path validates and applies one ordered `Reconcile` batch.
Five samples ran on the AMD Ryzen 9 5950X with `-benchmem`.

| Path | Median ns/op | Bytes/op | Allocs/op | Improvement |
| --- | ---: | ---: | ---: | ---: |
| Per-record `Status` + `Commit` | 3,705 | 0 | 0 | 1.00x |
| Batch `Reconcile` | 3,418 | 0 | 0 | 1.08x faster |

Raw output:

```text
BenchmarkTU047ParticipantStatusLoop: 3705 ns/op 0 B/op 0 allocs/op
BenchmarkTU047ParticipantStatusLoop: 3713 ns/op 0 B/op 0 allocs/op
BenchmarkTU047ParticipantStatusLoop: 3719 ns/op 0 B/op 0 allocs/op
BenchmarkTU047ParticipantStatusLoop: 3676 ns/op 0 B/op 0 allocs/op
BenchmarkTU047ParticipantStatusLoop: 3686 ns/op 0 B/op 0 allocs/op
BenchmarkTU047ParticipantBatchReconcile: 3415 ns/op 0 B/op 0 allocs/op
BenchmarkTU047ParticipantBatchReconcile: 3450 ns/op 0 B/op 0 allocs/op
BenchmarkTU047ParticipantBatchReconcile: 3423 ns/op 0 B/op 0 allocs/op
BenchmarkTU047ParticipantBatchReconcile: 3389 ns/op 0 B/op 0 allocs/op
BenchmarkTU047ParticipantBatchReconcile: 3418 ns/op 0 B/op 0 allocs/op
```

The batch path removes repeated lock/unlock cycles without adding retained or
transient allocations for canonical ordered input. It remains transport-
neutral and caller-owned; ordinary asynchronous replication is unchanged. See
[T047_PARTICIPANT_RECONCILIATION.md](T047_PARTICIPANT_RECONCILIATION.md).

EOF
git show HEAD:BENCHMARK.md > "$tmp_dir/BENCHMARK.md"
awk -v section_file="$tmp_dir/BENCHMARK.section" '
  index($0, "<a id=\"c154e-durable-rolling-schema-checkpoint\"></a>") == 1 {
    while ((getline line < section_file) > 0) print line
    close(section_file)
    inserted++
  }
  { print }
  END {
    if (inserted != 1) exit 42
  }
' "$tmp_dir/BENCHMARK.md" > "$tmp_dir/BENCHMARK.updated.md"
benchmark_blob=$(git hash-object -w "$tmp_dir/BENCHMARK.updated.md")
git update-index --add --cacheinfo "100644,$benchmark_blob,BENCHMARK.md"

git add -- \
  T047_PARTICIPANT_RECONCILIATION.md \
  hat/hatReplication/tu47_cluster_write_commit_reconcile.go \
  hat/hatReplication/tu47_cluster_write_commit_reconcile_benchmark_test.go \
  hat/hatReplication/tu47_cluster_write_commit_reconcile_test.go \
  scripts/benchmark-t047-reconcile-baseline.sh \
  scripts/benchmark-t047-reconcile.sh \
  scripts/commit-t047-reconcile.sh \
  scripts/format-t047-reconcile.sh \
  scripts/push-t047-reconcile.sh \
  scripts/race-t047-reconcile.sh \
  scripts/stage-t047-reconcile.sh \
  scripts/test-t047-reconcile-package.sh \
  scripts/test-t047-reconcile.sh \
  scripts/vet-t047-reconcile.sh

expected_file="$tmp_dir/expected"
staged_file="$tmp_dir/staged"
printf '%s\n' "${feature_paths[@]}" | sort > "$expected_file"
git diff --cached --name-only | sort > "$staged_file"
if ! cmp -s "$expected_file" "$staged_file"; then
  printf '%s\n' 'Unexpected staged paths:' >&2
  git diff --cached --name-only >&2
  exit 43
fi
git diff --cached --check
printf 'Staged T047f reconciliation paths: %s\n' "${#feature_paths[@]}"
