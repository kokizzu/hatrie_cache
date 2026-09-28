#!/usr/bin/env bash
set -euo pipefail

readonly commit_message='feat(sql): add incremental grouped MIN/MAX maintenance [skip ci]'
readonly feature_files=(
  'hat/hatSql/m037_incremental_group_min_max.go'
  'hat/hatSql/m037_sql_incremental_group_min_max_test.go'
  'hat/hatSql/m037_sql_incremental_group_min_max_benchmark_test.go'
  'hat/hatSql/mz034_sql_incremental_group.go'
  'M037_SQL_INCREMENTAL_GROUP_MIN_MAX.md'
  'scripts/test-m037-sql-incremental-group-minmax.sh'
  'scripts/benchmark-m037-sql-group-minmax-baseline.sh'
  'scripts/benchmark-m037-sql-incremental-group-minmax.sh'
  'scripts/format-m037-sql-incremental-group-minmax.sh'
  'scripts/race-m037-sql-incremental-group-minmax.sh'
  'scripts/deliver-m037-sql-incremental-group-minmax.sh'
)

readonly makefile_block='\n.PHONY: format-m037-sql-incremental-group-minmax test-m037-sql-incremental-group-minmax benchmark-m037-sql-group-minmax-baseline benchmark-m037-sql-incremental-group-minmax race-m037-sql-incremental-group-minmax\nformat-m037-sql-incremental-group-minmax:\n\tbash scripts/format-m037-sql-incremental-group-minmax.sh\n\ntest-m037-sql-incremental-group-minmax:\n\tbash scripts/test-m037-sql-incremental-group-minmax.sh\n\nbenchmark-m037-sql-group-minmax-baseline:\n\tbash scripts/benchmark-m037-sql-group-minmax-baseline.sh\n\nbenchmark-m037-sql-incremental-group-minmax:\n\tbash scripts/benchmark-m037-sql-incremental-group-minmax.sh\n\nrace-m037-sql-incremental-group-minmax:\n\tbash scripts/race-m037-sql-incremental-group-minmax.sh\n\n.PHONY: stage-m037-sql-incremental-group-minmax commit-m037-sql-incremental-group-minmax push-m037-sql-incremental-group-minmax status-m037-sql-incremental-group-minmax\nstage-m037-sql-incremental-group-minmax:\n\tbash scripts/deliver-m037-sql-incremental-group-minmax.sh stage\n\ncommit-m037-sql-incremental-group-minmax:\n\tbash scripts/deliver-m037-sql-incremental-group-minmax.sh commit\n\npush-m037-sql-incremental-group-minmax:\n\tbash scripts/deliver-m037-sql-incremental-group-minmax.sh push\n\nstatus-m037-sql-incremental-group-minmax:\n\tbash scripts/deliver-m037-sql-incremental-group-minmax.sh status\n'

readonly benchmark_block='## M037 SQL Incremental Group MIN/MAX

Command: `make benchmark-m037-sql-incremental-group-minmax`

One `-benchmem` sample on Linux/amd64, AMD Ryzen 9 5950X, using the existing
10,000-row grouped MIN/MAX workload:

| Path | ns/op | B/op | allocs/op | Relative |
| --- | ---: | ---: | ---: | --- |
| Rebuild grouped MIN/MAX from 10,000 rows | 549,264 | 939,695 | 5,386 | baseline |
| Warm two-update incremental insert/retract | 2,413 | 2,052 | 20 | 227.6x faster, 457.9x lower bytes, 269.3x fewer allocs |

The incremental path retains per-group value multiplicities for exact endpoint
retractions. It is opt-in through `CompileIncrementalGroupAggregate`; normal
executor behavior and unsupported query fallback remain unchanged. Full
semantics are documented in
[M037_SQL_INCREMENTAL_GROUP_MIN_MAX.md](M037_SQL_INCREMENTAL_GROUP_MIN_MAX.md).
'

readonly inspiration_block='- [x] M037m Compiled SQL grouped `MIN`/`MAX` now has opt-in exact signed maintenance for restricted integer row-source queries, including duplicate-preserving endpoint retractions, WHERE filtering, and atomic validation; mixed or global shapes retain the normal executor path. See [M037_SQL_INCREMENTAL_GROUP_MIN_MAX.md](M037_SQL_INCREMENTAL_GROUP_MIN_MAX.md).'

readonly temp_dir="$(mktemp -d)"
trap 'rm -rf -- "${temp_dir:-}"' EXIT

usage() {
  printf 'usage: %s status|stage|commit|push\n' "$0"
}

stage_generated_file() {
  local source=$1
  local output=$2
  local blob
  blob=$(git hash-object -w "$source")
  git update-index --add --cacheinfo "100644,$blob,$output"
}

append_makefile() {
  git show HEAD:Makefile > "$temp_dir/Makefile"
  printf '%b' "$makefile_block" >> "$temp_dir/Makefile"
  stage_generated_file "$temp_dir/Makefile" Makefile
}

append_benchmark() {
  git show HEAD:BENCHMARK.md > "$temp_dir/BENCHMARK.md"
  if grep -Fq '## M037 SQL Incremental Group MIN/MAX' "$temp_dir/BENCHMARK.md"; then
    printf 'M037 benchmark block already exists in HEAD\n' >&2
    exit 1
  fi
  printf '\n%b\n' "$benchmark_block" >> "$temp_dir/BENCHMARK.md"
  stage_generated_file "$temp_dir/BENCHMARK.md" BENCHMARK.md
}

insert_catalog_block() {
  local source=$1
  local marker=$2
  local block_file=$3
  local output=$4
  awk -v marker="$marker" -v block_file="$block_file" '
    {
      print
      if ($0 == marker) {
        while ((getline line < block_file) > 0) print line
        close(block_file)
      }
    }
  ' < <(git show "HEAD:$source") > "$temp_dir/$output"
  grep -Fq 'M037m Compiled SQL grouped' "$temp_dir/$output" || {
    printf 'failed to insert M037m catalog block into %s\n' "$output" >&2
    exit 1
  }
  stage_generated_file "$temp_dir/$output" "$output"
}

verify_staged() {
  local unexpected path
  while IFS= read -r path; do
    case "$path" in
      BENCHMARK.md|CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md|INSPIRATION.md|Makefile|M037_SQL_INCREMENTAL_GROUP_MIN_MAX.md|hat/hatSql/m037_incremental_group_min_max.go|hat/hatSql/m037_sql_incremental_group_min_max_test.go|hat/hatSql/m037_sql_incremental_group_min_max_benchmark_test.go|hat/hatSql/mz034_sql_incremental_group.go|scripts/test-m037-sql-incremental-group-minmax.sh|scripts/benchmark-m037-sql-group-minmax-baseline.sh|scripts/benchmark-m037-sql-incremental-group-minmax.sh|scripts/format-m037-sql-incremental-group-minmax.sh|scripts/race-m037-sql-incremental-group-minmax.sh|scripts/deliver-m037-sql-incremental-group-minmax.sh) ;;
      *)
        unexpected=1
        printf 'unexpected staged path: %s\n' "$path" >&2
        ;;
    esac
  done < <(git diff --cached --name-only)
  [[ -z "${unexpected:-}" ]] || exit 1
}

stage() {
  git reset -- \
    BENCHMARK.md CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md INSPIRATION.md Makefile \
    M037_SQL_INCREMENTAL_GROUP_MIN_MAX.md hat/hatSql/m037_incremental_group_min_max.go \
    hat/hatSql/m037_sql_incremental_group_min_max_test.go \
    hat/hatSql/m037_sql_incremental_group_min_max_benchmark_test.go \
    hat/hatSql/mz034_sql_incremental_group.go \
    scripts/test-m037-sql-incremental-group-minmax.sh \
    scripts/benchmark-m037-sql-group-minmax-baseline.sh \
    scripts/benchmark-m037-sql-incremental-group-minmax.sh \
    scripts/format-m037-sql-incremental-group-minmax.sh \
    scripts/race-m037-sql-incremental-group-minmax.sh \
    scripts/deliver-m037-sql-incremental-group-minmax.sh
  git add -- "${feature_files[@]}"
  append_makefile
  append_benchmark
  printf '%s\n' "$inspiration_block" > "$temp_dir/catalog-block"
  insert_catalog_block INSPIRATION.md '- [ ] M037 Generic negative-diff support for every SQL operator.' "$temp_dir/catalog-block" INSPIRATION.md
  insert_catalog_block CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md '- [ ] M037 Generic negative-diff support for every SQL operator.' "$temp_dir/catalog-block" CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md
  verify_staged
  git diff --cached --stat
}

status() {
  git status --short
}

case "${1:-}" in
  status) status ;;
  stage) stage ;;
  commit) stage; git commit -m "$commit_message" ;;
  push) git push origin HEAD ;;
  *) usage >&2; exit 2 ;;
esac
