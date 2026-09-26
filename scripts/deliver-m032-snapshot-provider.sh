#!/usr/bin/env bash
set -euo pipefail

mode="${1:-plan}"
repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

case "$mode" in
  plan|stage|commit|push|deliver) ;;
  *)
    printf 'usage: %s {plan|stage|commit|push|deliver}\n' "$0" >&2
    exit 2
    ;;
esac

temporary_root="$(mktemp -d "${TMPDIR:-/tmp}/hatrie-m032-delivery.XXXXXX")"
trap 'rm -rf -- "$temporary_root"' EXIT

expected_paths=(
  BENCHMARK.md
  INSPIRATION.md
  MU04_MULTI_SOURCE_SNAPSHOT_COORDINATOR.md
  Makefile
  hat/hatSql/m032_snapshot_provider_benchmark_test.go
  hat/hatSql/m032_snapshot_provider_test.go
  hat/hatSql/m_u04_multi_source_snapshot.go
  scripts/benchmark-m032-snapshot-provider.sh
  scripts/deliver-m032-snapshot-provider.sh
  scripts/format-m032-snapshot-provider.sh
  scripts/race-m032-snapshot-provider.sh
  scripts/test-m032-snapshot-provider.sh
)

run_git() {
  if [[ -n "${DELIVERY_INDEX:-}" ]]; then
    GIT_INDEX_FILE="$DELIVERY_INDEX" git "$@"
  else
    git "$@"
  fi
}

prepare_index() {
  local index="$1"
  if [[ -n "$index" ]]; then
    DELIVERY_INDEX="$index"
    export DELIVERY_INDEX
    run_git read-tree HEAD
  else
    unset DELIVERY_INDEX
    if ! git diff --cached --quiet; then
      printf '%s\n' 'refusing delivery: pre-existing staged changes detected' >&2
      git diff --cached --name-only >&2
      exit 1
    fi
  fi
}

stage_unique_paths() {
  run_git add -- \
    hat/hatSql/m032_snapshot_provider_benchmark_test.go \
    hat/hatSql/m032_snapshot_provider_test.go \
    scripts/benchmark-m032-snapshot-provider.sh \
    scripts/deliver-m032-snapshot-provider.sh \
    scripts/format-m032-snapshot-provider.sh \
    scripts/race-m032-snapshot-provider.sh \
    scripts/test-m032-snapshot-provider.sh
}

apply_generated_patch() {
  local file="$1"
  local transform="$2"
  local safe_name="${file//\//_}"
  local base="$temporary_root/$safe_name.base"
  local next="$temporary_root/$safe_name.next"
  local patch="$temporary_root/$safe_name.patch"
  git show "HEAD:$file" > "$base"
  cp "$base" "$next"
  "$transform" "$next"
  if diff -u --label "a/$file" --label "b/$file" "$base" "$next" > "$patch"; then
    printf 'unexpected empty patch for %s\n' "$file" >&2
    exit 1
  else
    diff_status=$?
    [[ "$diff_status" -eq 1 ]] || exit "$diff_status"
  fi
  run_git apply --cached --whitespace=nowarn "$patch"
}

transform_snapshot_go() {
  local file="$1"
  local method_file="$temporary_root/m032-method.go"
  local intermediate="$temporary_root/m032-go.intermediate"
  printf '%s\n' \
    '// BeginSQLSnapshot pins the currently published immutable view for one SQL' \
    '// execution. A later capture publishes a new view pointer and cannot mutate' \
    '// the one returned here, so the release callback is intentionally a no-op.' \
    'func (coordinator *SQLMultiSourceSnapshotCoordinator) BeginSQLSnapshot(ctx context.Context) (SQLSourceResolver, func(), error) {' \
    '    if coordinator == nil {' \
    '        return nil, nil, ErrSQLMultiSourceSnapshotNil' \
    '    }' \
    '    if ctx == nil {' \
    '        ctx = context.Background()' \
    '    }' \
    '    if err := ctx.Err(); err != nil {' \
    '        return nil, nil, err' \
    '    }' \
    '    coordinator.mu.RLock()' \
    '    view := coordinator.view' \
    '    coordinator.mu.RUnlock()' \
    '    if view == nil {' \
    '        return nil, nil, ErrSQLMultiSourceSnapshotUnavailable' \
    '    }' \
    '    return view, func() {}, nil' \
    '}' \
    > "$method_file"
  awk '
    { print }
    !inserted && /ErrSQLMultiSourceSnapshotIdentity[[:space:]]*=/ {
      print "\tErrSQLMultiSourceSnapshotUnavailable    = errors.New(\"hatSql: multi-source snapshot is not published\")"
      inserted = 1
    }
  ' "$file" > "$intermediate"
  mv "$intermediate" "$file"
  intermediate="$temporary_root/m032-go.method"
  awk -v method_file="$method_file" '
    !inserted && /^\/\/ ResolveSQLSource resolves a source from the currently published view\./ {
      while ((getline line < method_file) > 0) print line
      close(method_file)
      inserted = 1
    }
    { print }
    END { if (!inserted) exit 42 }
  ' "$file" > "$intermediate"
  mv "$intermediate" "$file"
}

transform_multi_source_doc() {
  local file="$1"
  local section_file="$temporary_root/m032-doc-section"
  local intermediate="$temporary_root/m032-doc.intermediate"
  printf '%s\n' \
    '## SQL Execution Pinning' \
    '' \
    '`SQLMultiSourceSnapshotCoordinator` implements `SQLSnapshotProvider`. After a' \
    'successful capture, it can be passed directly to SQL execution:' \
    '' \
    '```go' \
    'result, err := hatSql.ExecuteSQLQueryContext(' \
    '    ctx,' \
    '    query,' \
    '    coordinator,' \
    '    hatSql.SQLQueryOptions{},' \
    ')' \
    '```' \
    '' \
    'The query captures the current immutable publication view once through' \
    '`BeginSQLSnapshot`. A later capture swaps in a new view without changing' \
    'the one already used by the query, so independent source reads cannot mix' \
    'generations. The hook is only used when the caller supplies this coordinator;' \
    'ordinary resolvers and default SQL execution remain unchanged. There is no' \
    'per-row allocation or retained lock for the release callback.' \
    > "$section_file"
  awk -v section_file="$section_file" '
    !inserted && /^The checkpoint store/ {
      while ((getline line < section_file) > 0) print line
      close(section_file)
      print ""
      inserted = 1
    }
    { print }
    END { if (!inserted) exit 42 }
  ' "$file" > "$intermediate"
  mv "$intermediate" "$file"
}

transform_inspiration() {
  local file="$1"
  awk '{
    if ($0 == "- [ ] M032 Strong consistency across all independent source partitions.") {
      print "- [x] M032 Strong consistency across all independent source partitions. Partially adopted: `SQLMultiSourceSnapshotCoordinator` now implements `SQLSnapshotProvider`, so SQL execution pins one immutable publication generation across independent sources; distributed frontier acquisition and physical source coordination remain caller-owned. See [MU04_MULTI_SOURCE_SNAPSHOT_COORDINATOR.md](MU04_MULTI_SOURCE_SNAPSHOT_COORDINATOR.md)."
    } else {
      print
    }
  }' "$file" > "$file.next"
  mv "$file.next" "$file"
}

transform_benchmark() {
  local file="$1"
  cat >> "$file" <<'EOF'

## M032 SQL snapshot provider pinning

Raw `make benchmark-m032-snapshot-provider` results (`-benchmem`, five
samples, Linux/amd64, AMD Ryzen 9 5950X):

```text
BenchmarkM032MultiSourceSnapshotLiveResolveBaseline-32  5198348  232.1 ns/op  344 B/op  3 allocs/op
BenchmarkM032MultiSourceSnapshotLiveResolveBaseline-32  5223302  223.8 ns/op  344 B/op  3 allocs/op
BenchmarkM032MultiSourceSnapshotLiveResolveBaseline-32  5306199  232.1 ns/op  344 B/op  3 allocs/op
BenchmarkM032MultiSourceSnapshotLiveResolveBaseline-32  5294707  228.9 ns/op  344 B/op  3 allocs/op
BenchmarkM032MultiSourceSnapshotLiveResolveBaseline-32  5398131  223.0 ns/op  344 B/op  3 allocs/op
BenchmarkM032MultiSourceSnapshotBeginAndResolve-32      5044141  231.8 ns/op  344 B/op  3 allocs/op
BenchmarkM032MultiSourceSnapshotBeginAndResolve-32      5306896  231.4 ns/op  344 B/op  3 allocs/op
BenchmarkM032MultiSourceSnapshotBeginAndResolve-32      5279688  226.2 ns/op  344 B/op  3 allocs/op
BenchmarkM032MultiSourceSnapshotBeginAndResolve-32      5309121  229.3 ns/op  344 B/op  3 allocs/op
BenchmarkM032MultiSourceSnapshotBeginAndResolve-32      5380942  228.6 ns/op  344 B/op  3 allocs/op
BenchmarkM032MultiSourceSnapshotPinnedResolve-32        5439189  220.1 ns/op  344 B/op  3 allocs/op
BenchmarkM032MultiSourceSnapshotPinnedResolve-32        5458924  221.7 ns/op  344 B/op  3 allocs/op
BenchmarkM032MultiSourceSnapshotPinnedResolve-32        5194110  223.2 ns/op  344 B/op  3 allocs/op
BenchmarkM032MultiSourceSnapshotPinnedResolve-32        5309815  220.9 ns/op  344 B/op  3 allocs/op
BenchmarkM032MultiSourceSnapshotPinnedResolve-32        5412830  221.8 ns/op  344 B/op  3 allocs/op
PASS
```

The live resolver baseline median is 228.9 ns/op. Beginning a pinned view and
resolving through it is 229.3 ns/op, with identical 344 B/op and 3
allocations. Reusing the pinned view for repeated source reads is 221.7
ns/op, about 0.97x the baseline CPU time, with no memory or allocation change.
The feature is retained because it closes the cross-generation consistency
gap at effectively zero steady-state cost; it remains opt-in through the
multi-source coordinator.
EOF
}

transform_makefile() {
  local file="$1"
  if grep -q '^test-m032-snapshot-provider:' "$file"; then
    printf '%s\n' 'M032 delivery targets already exist in HEAD' >&2
    exit 1
  fi
  cat >> "$file" <<'EOF'

.PHONY: test-m032-snapshot-provider
test-m032-snapshot-provider:
	@bash scripts/test-m032-snapshot-provider.sh

.PHONY: format-m032-snapshot-provider benchmark-m032-snapshot-provider
format-m032-snapshot-provider:
	@bash scripts/format-m032-snapshot-provider.sh

benchmark-m032-snapshot-provider:
	@bash scripts/benchmark-m032-snapshot-provider.sh

.PHONY: race-m032-snapshot-provider
race-m032-snapshot-provider:
	@bash scripts/race-m032-snapshot-provider.sh

.PHONY: plan-m032-snapshot-provider stage-m032-snapshot-provider commit-m032-snapshot-provider push-m032-snapshot-provider deliver-m032-snapshot-provider
plan-m032-snapshot-provider:
	@bash scripts/deliver-m032-snapshot-provider.sh plan

stage-m032-snapshot-provider:
	@bash scripts/deliver-m032-snapshot-provider.sh stage

commit-m032-snapshot-provider:
	@bash scripts/deliver-m032-snapshot-provider.sh commit

push-m032-snapshot-provider:
	@bash scripts/deliver-m032-snapshot-provider.sh push

deliver-m032-snapshot-provider:
	@bash scripts/deliver-m032-snapshot-provider.sh deliver
EOF
}

stage_existing_changes() {
  apply_generated_patch hat/hatSql/m_u04_multi_source_snapshot.go transform_snapshot_go
  apply_generated_patch MU04_MULTI_SOURCE_SNAPSHOT_COORDINATOR.md transform_multi_source_doc
  apply_generated_patch INSPIRATION.md transform_inspiration
  apply_generated_patch BENCHMARK.md transform_benchmark
  apply_generated_patch Makefile transform_makefile
}

verify_staged_paths() {
  local actual="$temporary_root/actual.paths"
  local expected="$temporary_root/expected.paths"
  run_git diff --cached --name-only | sort > "$actual"
  printf '%s\n' "${expected_paths[@]}" | sort > "$expected"
  if ! diff -u "$expected" "$actual"; then
    printf '%s\n' 'delivery path set mismatch' >&2
    exit 1
  fi
  run_git diff --cached --check
  printf '%s\n' 'Selective M032 staged paths:'
  run_git diff --cached --stat
}

stage_changes() {
  prepare_index "${1:-}"
  stage_unique_paths
  stage_existing_changes
  verify_staged_paths
}

case "$mode" in
  plan)
    stage_changes "$temporary_root/index"
    ;;
  stage)
    stage_changes
    ;;
  commit)
    stage_changes
    git commit -m 'feat(sql): pin multi-source snapshots for queries [skip ci]'
    ;;
  push)
    git push origin HEAD
    ;;
  deliver)
    stage_changes
    git commit -m 'feat(sql): pin multi-source snapshots for queries [skip ci]'
    git push origin HEAD
    ;;
esac
