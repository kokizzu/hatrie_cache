#!/usr/bin/env bash
set -euo pipefail

readonly commit_message='feat(sql): add incremental DISTINCT maintenance [skip ci]'
readonly feature_files=(
  'hat/hatSql/m038_sql_incremental_distinct.go'
  'hat/hatSql/m038_sql_incremental_distinct_test.go'
  'hat/hatSql/m038_sql_incremental_distinct_benchmark_test.go'
  'hat/hatSql/mz034_sql_incremental_projection.go'
  'M038_SQL_INCREMENTAL_DISTINCT.md'
  'scripts/test-m038-sql-incremental-distinct.sh'
  'scripts/benchmark-m038-sql-distinct-baseline.sh'
  'scripts/format-m038-sql-incremental-distinct.sh'
  'scripts/race-m038-sql-incremental-distinct.sh'
  'scripts/deliver-m038-sql-incremental-distinct.sh'
)

readonly makefile_block='\n.PHONY: format-m038-sql-incremental-distinct test-m038-sql-incremental-distinct benchmark-m038-sql-distinct-baseline race-m038-sql-incremental-distinct\nformat-m038-sql-incremental-distinct:\n\tbash scripts/format-m038-sql-incremental-distinct.sh\n\ntest-m038-sql-incremental-distinct:\n\tbash scripts/test-m038-sql-incremental-distinct.sh\n\nbenchmark-m038-sql-distinct-baseline:\n\tbash scripts/benchmark-m038-sql-distinct-baseline.sh\n\nrace-m038-sql-incremental-distinct:\n\tbash scripts/race-m038-sql-incremental-distinct.sh\n\n.PHONY: stage-m038-sql-incremental-distinct commit-m038-sql-incremental-distinct push-m038-sql-incremental-distinct status-m038-sql-incremental-distinct\nstage-m038-sql-incremental-distinct:\n\tbash scripts/deliver-m038-sql-incremental-distinct.sh stage\n\ncommit-m038-sql-incremental-distinct:\n\tbash scripts/deliver-m038-sql-incremental-distinct.sh commit\n\npush-m038-sql-incremental-distinct:\n\tbash scripts/deliver-m038-sql-incremental-distinct.sh push\n\nstatus-m038-sql-incremental-distinct:\n\tbash scripts/deliver-m038-sql-incremental-distinct.sh status\n'

readonly benchmark_block='## M038i SQL Incremental DISTINCT

Command: `make benchmark-m038-sql-distinct-baseline`

One `-benchmem` sample on Linux/amd64, AMD Ryzen 9 5950X, using 4,096 source
rows with 256 repeated projected values:

| Path | ns/op | B/op | allocs/op | Relative |
| --- | ---: | ---: | ---: | --- |
| Rebuild `SELECT DISTINCT` over 4,096 rows | 295,263 | 270,043 | 541 | baseline |
| Warm incremental two-row insert/retract delta | 2,979 | 2,437 | 23 | 99.1x faster, 110.8x lower bytes, 23.5x fewer allocs |

The incremental path retains exact duplicate multiplicity and pays persistent
state memory for projected rows and typed canonical keys. It is opt-in through
`CompiledSQLQuery.CompileIncrementalDistinct`; unsupported global query shapes
remain on the normal executor path. Full semantics are documented in
[M038_SQL_INCREMENTAL_DISTINCT.md](M038_SQL_INCREMENTAL_DISTINCT.md).
'

readonly inspiration_block='- [x] M038i Compiled SQL `SELECT DISTINCT` supports exact signed incremental
  maintenance for restricted row-source projections, including duplicate
  suppression, final retractions, typed canonical keys, and atomic failures;
  broader operator coverage remains open. See
  [M038_SQL_INCREMENTAL_DISTINCT.md](M038_SQL_INCREMENTAL_DISTINCT.md).'

readonly temp_dir="$(mktemp -d)"
trap 'rm -rf -- "${temp_dir:-}"' EXIT

usage() {
  printf 'usage: %s status|stage|commit|push\n' "$0"
}

status() {
  git status --short
}

stage_generated_file() {
  local source="$1"
  local output="$2"
  local mode blob
  mode="$(git ls-tree HEAD -- "$output" | cut -d' ' -f1)"
  [[ -n "$mode" ]] || mode=100644
  blob="$(git hash-object -w --path="$output" "$source")"
  git update-index --add --cacheinfo "$mode,$blob,$output"
}

append_makefile() {
  git show HEAD:Makefile > "$temp_dir/Makefile"
  printf '%b' "$makefile_block" >> "$temp_dir/Makefile"
  stage_generated_file "$temp_dir/Makefile" Makefile
}

append_benchmark() {
  git show HEAD:BENCHMARK.md > "$temp_dir/BENCHMARK.md"
  printf '\n%s\n' "$benchmark_block" >> "$temp_dir/BENCHMARK.md"
  stage_generated_file "$temp_dir/BENCHMARK.md" BENCHMARK.md
}

insert_catalog_block() {
  local source="$1"
  local output="$2"
  local block="$3"
  printf '%s\n' "$block" > "$temp_dir/block"
  awk -v block_file="$temp_dir/block" '
    {
      print
      if ($0 == "- [ ] M038 Generic multiset duplicate preservation across all operators.") {
        while ((getline line < block_file) > 0) print line
        close(block_file)
      }
    }
  ' < <(git show "HEAD:$source") > "$temp_dir/$output"
  grep -Fq 'M038i Compiled SQL' "$temp_dir/$output" || {
    printf 'failed to insert M038i catalog block into %s\n' "$output" >&2
    exit 1
  }
  stage_generated_file "$temp_dir/$output" "$output"
}

verify_staged() {
  local unexpected path
  while IFS= read -r path; do
    case "$path" in
      BENCHMARK.md|CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md|INSPIRATION.md|Makefile|M038_SQL_INCREMENTAL_DISTINCT.md|hat/hatSql/m038_sql_incremental_distinct.go|hat/hatSql/m038_sql_incremental_distinct_test.go|hat/hatSql/m038_sql_incremental_distinct_benchmark_test.go|hat/hatSql/mz034_sql_incremental_projection.go|scripts/test-m038-sql-incremental-distinct.sh|scripts/benchmark-m038-sql-distinct-baseline.sh|scripts/format-m038-sql-incremental-distinct.sh|scripts/race-m038-sql-incremental-distinct.sh|scripts/deliver-m038-sql-incremental-distinct.sh) ;;
      *)
        unexpected=1
        printf 'unexpected staged path: %s\n' "$path" >&2
        ;;
    esac
  done < <(git diff --cached --name-only)
  [[ -z "${unexpected:-}" ]] || exit 1
}

stage() {
  if ! git diff --cached --quiet; then
    verify_staged
    git reset -- \
      BENCHMARK.md CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md INSPIRATION.md Makefile \
      M038_SQL_INCREMENTAL_DISTINCT.md hat/hatSql/m038_sql_incremental_distinct.go \
      hat/hatSql/m038_sql_incremental_distinct_test.go \
      hat/hatSql/m038_sql_incremental_distinct_benchmark_test.go \
      hat/hatSql/mz034_sql_incremental_projection.go \
      scripts/test-m038-sql-incremental-distinct.sh \
      scripts/benchmark-m038-sql-distinct-baseline.sh \
      scripts/format-m038-sql-incremental-distinct.sh \
      scripts/race-m038-sql-incremental-distinct.sh \
      scripts/deliver-m038-sql-incremental-distinct.sh
  fi
  git add -- "${feature_files[@]}"
  append_makefile
  append_benchmark
  insert_catalog_block INSPIRATION.md INSPIRATION.md "$inspiration_block"
  insert_catalog_block CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md "$inspiration_block"
  verify_staged
  git diff --cached --stat
}

commit() {
  if git diff --cached --quiet; then
    stage
  else
    verify_staged
  fi
  git commit -m "$commit_message"
}

push() {
  git push
}

main() {
  [[ "$#" -eq 1 ]] || { usage >&2; exit 2; }
  case "$1" in
    status) status ;;
    stage) stage ;;
    commit) commit ;;
    push) push ;;
    *) usage >&2; exit 2 ;;
  esac
}

main "$@"
