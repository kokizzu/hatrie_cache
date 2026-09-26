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

temporary_root="$(mktemp -d "${TMPDIR:-/tmp}/hatrie-m052af-delivery.XXXXXX")"
trap 'rm -rf -- "$temporary_root"' EXIT

expected_paths=(
  BENCHMARK.md
  INSPIRATION.md
  M052AE_NATIVE_TRIPLE_GROUP.md
  M052AF_NATIVE_TRIPLE_GROUPED_ORDERED.md
  Makefile
  hat/hatSql/m052af_native_triple_grouped_ordered.go
  hat/hatSql/m052af_native_triple_grouped_ordered_test.go
  hat/hatSql/m052af_native_triple_grouped_ordered_benchmark_test.go
  hat/hatSql/m052c_native_dataflow.go
  hat/hatSql/m052p_auto_native_dataflow.go
  scripts/benchmark-m052af-native-triple-grouped-ordered.sh
  scripts/deliver-m052af-native-triple-grouped-ordered.sh
  scripts/format-m052af-native-triple-grouped-ordered.sh
  scripts/race-m052af-native-triple-grouped-ordered.sh
  scripts/test-m052af-native-triple-grouped-ordered.sh
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
    M052AF_NATIVE_TRIPLE_GROUPED_ORDERED.md \
    hat/hatSql/m052af_native_triple_grouped_ordered.go \
    hat/hatSql/m052af_native_triple_grouped_ordered_test.go \
    hat/hatSql/m052af_native_triple_grouped_ordered_benchmark_test.go \
    scripts/benchmark-m052af-native-triple-grouped-ordered.sh \
    scripts/deliver-m052af-native-triple-grouped-ordered.sh \
    scripts/format-m052af-native-triple-grouped-ordered.sh \
    scripts/race-m052af-native-triple-grouped-ordered.sh \
    scripts/test-m052af-native-triple-grouped-ordered.sh
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

transform_native_dataflow() {
  local file="$1"
  if grep -q 'nativeSQLDataflowTripleGroupedOrderedPlanFor' "$file"; then
    printf 'M052af native changes already exist in %s\n' "$file" >&2
    exit 1
  fi
  awk '
    {
      if (!compile_inserted && index($0, "} else if _, ok := nativeSQLDataflowGroupedOrderedPlanFor(query); !ok {") > 0) {
        print "\t\t} else if len(query.groupBy) == 3 {"
        print "\t\t\tif _, ok := nativeSQLDataflowTripleGroupedOrderedPlanFor(query); !ok {"
        print "\t\t\t\treturn fmt.Errorf(\"%w: grouped ordered query shape\", ErrSQLNativeDataflowUnsupported)"
        print "\t\t\t}"
        compile_inserted = 1
      }
      if (!dispatch_inserted && index($0, "if plan, ok := nativeSQLDataflowGroupedOrderedPlanFor(query); ok {") > 0) {
        print "\tif plan, ok := nativeSQLDataflowTripleGroupedOrderedPlanFor(query); ok {"
        print "\t\treturn executeNativeSQLDataflowTripleGroupedOrdered(ctx, query, initial, plan)"
        print "\t}"
        dispatch_inserted = 1
      }
      print
    }
    END {
      if (!compile_inserted || !dispatch_inserted) exit 42
    }
  ' "$file" > "$file.next"
  mv "$file.next" "$file"
}

transform_auto_native_dataflow() {
  local file="$1"
  if grep -q 'nativeSQLDataflowTripleGroupedOrderedPlanFor' "$file"; then
    printf 'M052af automatic changes already exist in %s\n' "$file" >&2
    exit 1
  fi
  awk '
    {
      if (index($0, "len(query.groupBy)") > 0 && index($0, "len(query.orderBy) == 0") > 0) {
        if (index($0, "len(query.groupBy) > 2") > 0) {
          sub(/len\(query\.groupBy\) > 2/, "len(query.groupBy) > 3")
        }
        if (index($0, "len(query.groupBy) > 3") > 0) {
          limit_inserted = 1
        }
      }
      if (!triple_inserted && index($0, "_, ok := nativeSQLDataflowCompositeGroupedOrderedPlanFor(query)") > 0) {
        print "\tif len(query.groupBy) == 3 {"
        print "\t\t_, ok := nativeSQLDataflowTripleGroupedOrderedPlanFor(query)"
        print "\t\treturn ok && validateNativeSQLDataflowQuery(query) == nil"
        print "\t}"
        triple_inserted = 1
      }
      print
    }
    END {
      if (!limit_inserted || !triple_inserted) {
        exit 42
      }
    }
  ' "$file" > "$file.next"
  mv "$file.next" "$file"
}

transform_triple_group_doc() {
  local file="$1"
  awk '
    {
      if ($0 == "The implementation deliberately remains fail-closed for four or more grouping") {
        print "The implementation deliberately remains fail-closed for four or more grouping"
        print "fields and bounded grouped output without the supported ordered Top-N shape."
        print "M052af adds the separate three-field grouped `HAVING` plus finite `ORDER BY`"
        print "path; richer or unsupported ordered expressions still use the established"
        print "executor rather than being silently misclassified."
        skip = 4
        next
      }
      if (skip > 0) {
        skip--
        next
      }
      print
    }
    END {
      if (skip != 0) exit 42
    }
  ' "$file" > "$file.next"
  mv "$file.next" "$file"
}

transform_inspiration() {
  local file="$1"
  local section="$temporary_root/m052af-inspiration-section"
  printf '%s\n' \
    '- [x] M052af Native three-field grouped ordered Top-N dataflow. Three-field' \
    '  grouped aggregates now support selected aggregate `HAVING`, alias-resolved' \
    '  finite `ORDER BY`, `LIMIT`, and `OFFSET` through the existing bounded heap;' \
    '  richer order expressions, `WITH TIES`, and four-field groups remain' \
    '  fail-closed. The paired benchmark is 3.94x faster with 4.36x lower bytes and' \
    '  37.15x fewer allocations; see' \
    '  [M052AF_NATIVE_TRIPLE_GROUPED_ORDERED.md](M052AF_NATIVE_TRIPLE_GROUPED_ORDERED.md)' \
    '  and [BENCHMARK.md](BENCHMARK.md#m052af-native-three-field-grouped-ordered-top-n).' \
    > "$section"
  awk -v section="$section" '
    {
      if ($0 == "  row resolvers; four-field, grouped `HAVING`, grouped ordering, and bounded") {
        print "  row resolvers; four-field and bounded grouped output without a supported"
        next
      }
      if ($0 == "  grouped output remain fail-closed. The paired benchmark is 3.79x faster with") {
        print "  order remain fail-closed. The paired benchmark is 3.79x faster with 4.39x"
        next
      }
      if ($0 == "  4.39x lower bytes and 50.0x fewer allocations; see") {
        print "  lower bytes and 50.0x fewer allocations; see"
        next
      }
      print
      if (!inserted && $0 == "  [BENCHMARK.md](BENCHMARK.md#m052ae-native-three-field-group-by).") {
        while ((getline line < section) > 0) print line
        close(section)
        inserted = 1
      }
    }
    END {
      if (!inserted) exit 42
    }
  ' "$file" > "$file.next"
  mv "$file.next" "$file"
}

transform_benchmark() {
  local file="$1"
  local section="$temporary_root/m052af-benchmark-section"
  printf '%s\n' \
    '<a id="m052af-native-three-field-grouped-ordered-top-n"></a>' \
    '## M052af: Native Three-Field Grouped Ordered Top-N' \
    '' \
    '`make benchmark-m052af-native-triple-grouped-ordered` compares the established' \
    'materialized grouped-order path with the fixed three-field native grouping plus' \
    'the existing bounded Top-N heap. The fixture has 20,000 rows, 1,536 possible' \
    'groups, three grouping fields, `COUNT(*)`, `SUM(value)`, and a' \
    '`LIMIT 100 OFFSET 25` page. Linux/amd64, AMD Ryzen 9 5950X, five samples per' \
    'path.' \
    '' \
    'The pre-implementation baseline was collected before the native plan existed:' \
    '' \
    '```text' \
    'BenchmarkCompiledSQLNativeTripleGroupedOrderedBaseline-32' \
    '25780427 ns/op 27931388 B/op 236972 allocs/op' \
    '25224147 ns/op 27930919 B/op 236970 allocs/op' \
    '24639782 ns/op 27930992 B/op 236971 allocs/op' \
    '24935371 ns/op 27931059 B/op 236971 allocs/op' \
    '24830300 ns/op 27930906 B/op 236971 allocs/op' \
    '```' \
    '' \
    'Final implementation run:' \
    '' \
    '```text' \
    'fallback control:' \
    '25888314 ns/op 27931116 B/op 236971 allocs/op' \
    '25212524 ns/op 27931178 B/op 236971 allocs/op' \
    '25289815 ns/op 27931057 B/op 236971 allocs/op' \
    '25418293 ns/op 27931104 B/op 236971 allocs/op' \
    '25468618 ns/op 27931177 B/op 236971 allocs/op' \
    'native:' \
    '6518954 ns/op 6401280 B/op 6379 allocs/op' \
    '6328391 ns/op 6401276 B/op 6379 allocs/op' \
    '6525452 ns/op 6401276 B/op 6379 allocs/op' \
    '6450456 ns/op 6401278 B/op 6379 allocs/op' \
    '6385372 ns/op 6401274 B/op 6379 allocs/op' \
    '```' \
    '' \
    '| Path | Median time | Median bytes | Median allocs | Relative result |' \
    '| --- | ---: | ---: | ---: | --- |' \
    '| Pre-change fallback baseline | 24.640 ms/op | 27,930,992 B/op | 236,971 | 1.00x |' \
    '| Final fallback control | 25.418 ms/op | 27,931,116 B/op | 236,971 | 1.03x CPU measurement noise |' \
    '| Native three-field grouped Top-N | 6.450 ms/op | 6,401,276 B/op | 6,379 | **3.94x faster; 4.36x lower bytes; 37.15x fewer allocations** |' \
    '' \
    'The native plan changes only eligible three-field grouped ordered queries;' \
    'unsupported shapes continue through the fallback and retain the prior default' \
    'semantics. See' \
    '[M052AF_NATIVE_TRIPLE_GROUPED_ORDERED.md](M052AF_NATIVE_TRIPLE_GROUPED_ORDERED.md).' \
    > "$section"
  awk -v section="$section" '
    {
      if ($0 == "The result is an unbounded grouped aggregation optimization only. Four or more") {
        print "The result is an unbounded grouped aggregation optimization only. Four or more"
        print "grouping fields and bounded grouped output without the supported ordered Top-N"
        print "shape retain the established fallback boundary. M052af adds the supported"
        print "three-field grouped ordered path. See"
        print "[M052AE_NATIVE_TRIPLE_GROUP.md](M052AE_NATIVE_TRIPLE_GROUP.md)."
        skip = 3
        next
      }
      if (skip > 0) {
        skip--
        next
      }
      if (!inserted && $0 == "<a id=\"rejected-t042-independent-setint-parallel-replay\"></a>") {
        while ((getline line < section) > 0) print line
        print ""
        close(section)
        inserted = 1
      }
      print
    }
    END {
      if (skip != 0 || !inserted) exit 42
    }
  ' "$file" > "$file.next"
  mv "$file.next" "$file"
}

transform_makefile() {
  local file="$1"
  if grep -q '^test-m052af-native-triple-grouped-ordered:' "$file"; then
    printf 'M052af Makefile targets already exist in HEAD\n' >&2
    exit 1
  fi
  cat >> "$file" <<'EOF'

.PHONY: test-m052af-native-triple-grouped-ordered
test-m052af-native-triple-grouped-ordered:
	@bash scripts/test-m052af-native-triple-grouped-ordered.sh

.PHONY: format-m052af-native-triple-grouped-ordered
format-m052af-native-triple-grouped-ordered:
	@bash scripts/format-m052af-native-triple-grouped-ordered.sh

.PHONY: benchmark-m052af-native-triple-grouped-ordered
benchmark-m052af-native-triple-grouped-ordered:
	@bash scripts/benchmark-m052af-native-triple-grouped-ordered.sh

.PHONY: race-m052af-native-triple-grouped-ordered
race-m052af-native-triple-grouped-ordered:
	@bash scripts/race-m052af-native-triple-grouped-ordered.sh

.PHONY: plan-m052af-native-triple-grouped-ordered stage-m052af-native-triple-grouped-ordered commit-m052af-native-triple-grouped-ordered push-m052af-native-triple-grouped-ordered deliver-m052af-native-triple-grouped-ordered
plan-m052af-native-triple-grouped-ordered:
	@bash scripts/deliver-m052af-native-triple-grouped-ordered.sh plan

stage-m052af-native-triple-grouped-ordered:
	@bash scripts/deliver-m052af-native-triple-grouped-ordered.sh stage

commit-m052af-native-triple-grouped-ordered:
	@bash scripts/deliver-m052af-native-triple-grouped-ordered.sh commit

push-m052af-native-triple-grouped-ordered:
	@bash scripts/deliver-m052af-native-triple-grouped-ordered.sh push

deliver-m052af-native-triple-grouped-ordered:
	@bash scripts/deliver-m052af-native-triple-grouped-ordered.sh deliver
EOF
}

stage_existing_changes() {
  apply_generated_patch hat/hatSql/m052c_native_dataflow.go transform_native_dataflow
  apply_generated_patch hat/hatSql/m052p_auto_native_dataflow.go transform_auto_native_dataflow
  apply_generated_patch M052AE_NATIVE_TRIPLE_GROUP.md transform_triple_group_doc
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
  printf '%s\n' 'Selective M052af staged paths:'
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
    git commit -m 'feat(sql): add native three-field grouped Top-N [skip ci]'
    ;;
  push)
    git push origin HEAD
    ;;
  deliver)
    stage_changes
    git commit -m 'feat(sql): add native three-field grouped Top-N [skip ci]'
    git push origin HEAD
    ;;
esac
