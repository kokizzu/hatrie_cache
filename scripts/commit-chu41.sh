#!/usr/bin/env bash
set -euo pipefail

base=$(git rev-parse HEAD)
workdir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-chu41-commit.XXXXXX")
index="$workdir/index"
trap 'rm -rf "$workdir"' EXIT

mkdir -p "$workdir/hat/hatSql" "$workdir/hat/hatCache" "$workdir/scripts"

copy_current() {
  local path=$1
  cp "$path" "$workdir/$path"
}

copy_current hat/hatSql/ch_u41_prepared_cache_persistence.go
copy_current hat/hatSql/ch_u41_prepared_cache_persistence_test.go
copy_current hat/hatSql/ch_u41_prepared_cache_persistence_benchmark_test.go
copy_current hat/hatCache/sql_prepared_cache_persistence.go
copy_current CHU41_SCHEMA_VERSIONED_PLAN_CACHE.md
copy_current scripts/format-chu41.sh
copy_current scripts/test-chu41.sh
copy_current scripts/benchmark-chu41.sh
copy_current scripts/test-chu41-package.sh
copy_current scripts/race-chu41.sh
copy_current scripts/vet-chu41.sh
copy_current scripts/verify-chu41.sh
copy_current scripts/commit-chu41.sh
copy_current scripts/push-chu41.sh

git show "$base:Makefile" > "$workdir/Makefile"
printf '%s\n' \
  '' \
  '.PHONY: format-chu41 test-chu41 benchmark-chu41 test-chu41-package race-chu41 vet-chu41 verify-chu41 commit-chu41 push-chu41' \
  '' \
  'format-chu41:' \
  $'\tbash ./scripts/format-chu41.sh' \
  '' \
  'test-chu41:' \
  $'\tbash ./scripts/test-chu41.sh' \
  '' \
  'benchmark-chu41:' \
  $'\tbash ./scripts/benchmark-chu41.sh' \
  '' \
  'test-chu41-package:' \
  $'\tbash ./scripts/test-chu41-package.sh' \
  '' \
  'race-chu41:' \
  $'\tbash ./scripts/race-chu41.sh' \
  '' \
  'vet-chu41:' \
  $'\tbash ./scripts/vet-chu41.sh' \
  '' \
  'verify-chu41:' \
  $'\tbash ./scripts/verify-chu41.sh' \
  '' \
  'commit-chu41:' \
  $'\tbash ./scripts/commit-chu41.sh' \
  '' \
  'push-chu41:' \
  $'\tbash ./scripts/push-chu41.sh' >> "$workdir/Makefile"

git show "$base:README.md" > "$workdir/README.md"
awk '
  {
    print
    if ($0 == "- Current 50-per-product implementation queue: [PRODUCT_IDEA_GAPS.md](PRODUCT_IDEA_GAPS.md)") {
      print "- Prepared SQL plans can be warmed across restarts with the opt-in, schema-validated cache described in [CHU41_SCHEMA_VERSIONED_PLAN_CACHE.md](CHU41_SCHEMA_VERSIONED_PLAN_CACHE.md)."
    }
  }
' "$workdir/README.md" > "$workdir/README.md.tmp"
mv "$workdir/README.md.tmp" "$workdir/README.md"

git show "$base:PRODUCT_IDEA_GAPS.md" > "$workdir/PRODUCT_IDEA_GAPS.md"
awk -v replacement='| CH-U41 | Schema-versioned plan cache | Implemented opt-in bounded binary persistence for prepared SQL templates, exact schema-version admission, checksum validation, atomic writes, and parameter-value isolation. See [CHU41_SCHEMA_VERSIONED_PLAN_CACHE.md](CHU41_SCHEMA_VERSIONED_PLAN_CACHE.md). | Safe invalidation, parameter isolation, and memory cap. |' '
  /^\| CH-U41 \|/ { print replacement; next }
  { print }
' "$workdir/PRODUCT_IDEA_GAPS.md" > "$workdir/PRODUCT_IDEA_GAPS.md.tmp"
mv "$workdir/PRODUCT_IDEA_GAPS.md.tmp" "$workdir/PRODUCT_IDEA_GAPS.md"

git show "$base:BENCHMARK.md" > "$workdir/BENCHMARK.md"
if ! rg -q '^## CH-U41 Schema-Versioned Plan Cache$' BENCHMARK.md; then
  printf '%s\n' 'current BENCHMARK.md does not contain the CH-U41 section' >&2
  exit 1
fi
sed -n '/^## CH-U41 Schema-Versioned Plan Cache$/,$p' BENCHMARK.md >> "$workdir/BENCHMARK.md"

rm -f "$index"
GIT_INDEX_FILE="$index" git read-tree "$base"
GIT_INDEX_FILE="$index" git --work-tree="$workdir" add -- \
  Makefile README.md PRODUCT_IDEA_GAPS.md BENCHMARK.md \
  CHU41_SCHEMA_VERSIONED_PLAN_CACHE.md \
  hat/hatSql/ch_u41_prepared_cache_persistence.go \
  hat/hatSql/ch_u41_prepared_cache_persistence_test.go \
  hat/hatSql/ch_u41_prepared_cache_persistence_benchmark_test.go \
  hat/hatCache/sql_prepared_cache_persistence.go \
  scripts/format-chu41.sh scripts/test-chu41.sh scripts/benchmark-chu41.sh \
  scripts/test-chu41-package.sh scripts/race-chu41.sh scripts/vet-chu41.sh \
  scripts/verify-chu41.sh scripts/commit-chu41.sh scripts/push-chu41.sh
GIT_INDEX_FILE="$index" git diff --cached --check
tree=$(GIT_INDEX_FILE="$index" git write-tree)
commit=$(printf '%s\n\n%s\n' \
  'feat(sql): persist schema-versioned prepared query plans' \
  'Add bounded checksummed snapshots with exact schema admission and atomic restore.' \
  | GIT_INDEX_FILE="$index" git commit-tree "$tree" -p "$base")
head_ref=$(git symbolic-ref -q HEAD || true)
if [ -n "$head_ref" ]; then
  git update-ref "$head_ref" "$commit" "$base"
  printf 'created %s on %s\n' "$commit" "${head_ref#refs/heads/}"
else
  git update-ref HEAD "$commit" "$base"
  printf 'created %s on detached HEAD\n' "$commit"
fi
