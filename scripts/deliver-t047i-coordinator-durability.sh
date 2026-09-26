#!/usr/bin/env bash
set -euo pipefail

mode=${1:-plan}
repo_root=$(git rev-parse --show-toplevel)
cd "$repo_root"

feature_paths=(
  "T047I_COORDINATOR_DURABILITY.md"
  "hat/hatReplication/tu47_cluster_write_commit.go"
  "hat/hatReplication/tu47_cluster_write_commit_coordinator.go"
  "hat/hatReplication/tu47_cluster_write_commit_coordinator_benchmark_test.go"
  "hat/hatReplication/tu47_cluster_write_commit_coordinator_store.go"
  "hat/hatReplication/tu47_cluster_write_commit_coordinator_test.go"
  "scripts/benchmark-t047i-coordinator-durability.sh"
  "scripts/deliver-t047i-coordinator-durability.sh"
  "scripts/format-t047i-coordinator-durability.sh"
  "scripts/race-t047i-coordinator-durability.sh"
  "scripts/test-t047i-coordinator-durability.sh"
  "scripts/test-t047i-package.sh"
  "Makefile"
  "INSPIRATION.md"
  "BENCHMARK.md"
)

make_add=$(mktemp)
inspiration_add=$(mktemp)
benchmark_add=$(mktemp)
old_make=$(mktemp)
new_make=$(mktemp)
old_inspiration=$(mktemp)
new_inspiration=$(mktemp)
old_benchmark=$(mktemp)
new_benchmark=$(mktemp)
patch_file=$(mktemp)
cleanup() {
  rm -f "$make_add" "$inspiration_add" "$benchmark_add" "$old_make" "$new_make" \
    "$old_inspiration" "$new_inspiration" "$old_benchmark" "$new_benchmark" "$patch_file"
}
trap cleanup EXIT

cat > "$make_add" <<'EOF'
.PHONY: test-t047i-coordinator-durability
test-t047i-coordinator-durability:
	bash ./scripts/test-t047i-coordinator-durability.sh
.PHONY: format-t047i-coordinator-durability
format-t047i-coordinator-durability:
	bash ./scripts/format-t047i-coordinator-durability.sh
.PHONY: benchmark-t047i-coordinator-durability
benchmark-t047i-coordinator-durability:
	bash ./scripts/benchmark-t047i-coordinator-durability.sh
.PHONY: race-t047i-coordinator-durability
race-t047i-coordinator-durability:
	bash ./scripts/race-t047i-coordinator-durability.sh

.PHONY: test-t047i-package
test-t047i-package:
	bash ./scripts/test-t047i-package.sh
.PHONY: deliver-t047i-coordinator-durability
deliver-t047i-coordinator-durability:
	bash ./scripts/deliver-t047i-coordinator-durability.sh deliver
.PHONY: plan-t047i-coordinator-durability
plan-t047i-coordinator-durability:
	bash ./scripts/deliver-t047i-coordinator-durability.sh plan
.PHONY: stage-t047i-coordinator-durability
stage-t047i-coordinator-durability:
	bash ./scripts/deliver-t047i-coordinator-durability.sh stage
.PHONY: commit-t047i-coordinator-durability
commit-t047i-coordinator-durability:
	bash ./scripts/deliver-t047i-coordinator-durability.sh commit
.PHONY: push-t047i-coordinator-durability
push-t047i-coordinator-durability:
	bash ./scripts/deliver-t047i-coordinator-durability.sh push
EOF
printf '%s\n' '- [x] T047i Coordinator-owned durable write state. `ExecuteClusterWriteCommitWithStateStore` persists proposed, prepared, commit-started, and terminal phase boundaries through an atomic CRC32C file store; the default coordinator path remains unchanged and recovery reconciliation is explicit. See [T047I_COORDINATOR_DURABILITY.md](T047I_COORDINATOR_DURABILITY.md).' > "$inspiration_add"
cat > "$benchmark_add" <<'EOF'
## T047i Coordinator-Owned Durable Write State

Raw `make benchmark-t047i-coordinator-durability` results (`-benchtime=1s`,
`-count=3`, Linux/amd64, AMD Ryzen 9 5950X):

```text
BenchmarkT047iClusterWriteCommitDirect-32               442782       2389 ns/op    1264 B/op  19 allocs/op
BenchmarkT047iClusterWriteCommitDirect-32               493826       2342 ns/op    1264 B/op  19 allocs/op
BenchmarkT047iClusterWriteCommitDirect-32               472081       2399 ns/op    1264 B/op  19 allocs/op
BenchmarkT047iClusterWriteCommitMemoryStateStore-32     247760       4790 ns/op    4144 B/op  39 allocs/op
BenchmarkT047iClusterWriteCommitMemoryStateStore-32     227168       4686 ns/op    4144 B/op  39 allocs/op
BenchmarkT047iClusterWriteCommitMemoryStateStore-32     245670       4661 ns/op    4144 B/op  39 allocs/op
BenchmarkT047iClusterWriteCommitFileStoreSaveLoad-32       650    1579395 ns/op    3579 B/op  35 allocs/op
BenchmarkT047iClusterWriteCommitFileStoreSaveLoad-32       870    1434641 ns/op    3583 B/op  35 allocs/op
BenchmarkT047iClusterWriteCommitFileStoreSaveLoad-32       831    1442261 ns/op    3579 B/op  35 allocs/op
BenchmarkT047iClusterWriteCommitFileStateStore-32          210    9335651 ns/op   12399 B/op 123 allocs/op
BenchmarkT047iClusterWriteCommitFileStateStore-32          186    5784702 ns/op   12399 B/op 123 allocs/op
BenchmarkT047iClusterWriteCommitFileStateStore-32          210    5701395 ns/op   12399 B/op 123 allocs/op
```

The default direct path is unchanged. Durable execution is intentionally
opt-in; its roughly 2,421x latency cost versus direct execution buys
coordinator recovery boundaries and should only be enabled where that recovery
guarantee is required. See [T047I_COORDINATOR_DURABILITY.md](T047I_COORDINATOR_DURABILITY.md).
EOF

stage_patch() {
  local path=$1
  local old=$2
  local new=$3
  local diff_status=0
  if diff -u --label "a/$path" --label "b/$path" "$old" "$new" > "$patch_file"; then
    return 0
  else
    diff_status=$?
  fi
  if [[ $diff_status -ne 1 ]]; then
    printf 'failed to generate patch for %s\n' "$path" >&2
    return "$diff_status"
  fi
  git apply --cached "$patch_file"
}

stage_feature() {
  if [[ -n "$(git diff --cached --name-only)" ]]; then
    printf 'refusing to mix existing staged changes with T047i delivery\n' >&2
    exit 1
  fi

  git add -- \
    T047I_COORDINATOR_DURABILITY.md \
    hat/hatReplication/tu47_cluster_write_commit.go \
    hat/hatReplication/tu47_cluster_write_commit_coordinator.go \
    hat/hatReplication/tu47_cluster_write_commit_coordinator_benchmark_test.go \
    hat/hatReplication/tu47_cluster_write_commit_coordinator_store.go \
    hat/hatReplication/tu47_cluster_write_commit_coordinator_test.go \
    scripts/benchmark-t047i-coordinator-durability.sh \
    scripts/deliver-t047i-coordinator-durability.sh \
    scripts/format-t047i-coordinator-durability.sh \
    scripts/race-t047i-coordinator-durability.sh \
    scripts/test-t047i-coordinator-durability.sh \
    scripts/test-t047i-package.sh

  git show HEAD:Makefile > "$old_make"
  awk -v addition_file="$make_add" '
    BEGIN {
      while ((getline line < addition_file) > 0) {
        addition = addition line "\n"
      }
      close(addition_file)
      inserted = 0
    }
    {
      print
      if (!inserted && $0 ~ /deliver-m033c-global-timestamp-grpc\.sh deliver/) {
        printf "%s", addition
        inserted = 1
      }
    }
    END {
      if (!inserted) exit 2
    }
  ' "$old_make" > "$new_make"
  stage_patch Makefile "$old_make" "$new_make"

  git show HEAD:INSPIRATION.md > "$old_inspiration"
  awk -v addition_file="$inspiration_add" '
    BEGIN {
      while ((getline line < addition_file) > 0) {
        addition = addition line "\n"
      }
      close(addition_file)
      inserted = 0
    }
    {
      print
      if (!inserted && $0 ~ /T047h /) {
        printf "%s", addition
        inserted = 1
      }
    }
    END {
      if (!inserted) exit 2
    }
  ' "$old_inspiration" > "$new_inspiration"
  stage_patch INSPIRATION.md "$old_inspiration" "$new_inspiration"

  git show HEAD:BENCHMARK.md > "$old_benchmark"
  cp "$old_benchmark" "$new_benchmark"
  printf '\n' >> "$new_benchmark"
  cat "$benchmark_add" >> "$new_benchmark"
  stage_patch BENCHMARK.md "$old_benchmark" "$new_benchmark"

  git diff --cached --check
  while IFS= read -r path; do
    case "$path" in
      T047I_COORDINATOR_DURABILITY.md|hat/hatReplication/tu47_cluster_write_commit.go|hat/hatReplication/tu47_cluster_write_commit_coordinator.go|hat/hatReplication/tu47_cluster_write_commit_coordinator_benchmark_test.go|hat/hatReplication/tu47_cluster_write_commit_coordinator_store.go|hat/hatReplication/tu47_cluster_write_commit_coordinator_test.go|scripts/benchmark-t047i-coordinator-durability.sh|scripts/deliver-t047i-coordinator-durability.sh|scripts/format-t047i-coordinator-durability.sh|scripts/race-t047i-coordinator-durability.sh|scripts/test-t047i-coordinator-durability.sh|scripts/test-t047i-package.sh|Makefile|INSPIRATION.md|BENCHMARK.md) ;;
      *) printf 'unexpected staged path: %s\n' "$path" >&2; exit 1 ;;
    esac
  done < <(git diff --cached --name-only)
  git diff --cached --stat
}

case "$mode" in
  plan)
    printf 'T047i delivery paths:\n'
    printf '  %s\n' "${feature_paths[@]}"
    ;;
  stage)
    stage_feature
    ;;
  commit)
    git diff --cached --quiet && { printf 'nothing staged for T047i commit\n' >&2; exit 1; }
    git diff --cached --check
    git commit -m 'feat(replication): add coordinator durable write state [skip ci]'
    ;;
  push)
    git push origin HEAD
    ;;
  deliver)
    stage_feature
    git commit -m 'feat(replication): add coordinator durable write state [skip ci]'
    git push origin HEAD
    ;;
  *)
    printf 'usage: %s {plan|stage|commit|push|deliver}\n' "$0" >&2
    exit 2
    ;;
esac
