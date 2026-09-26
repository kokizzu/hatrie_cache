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

temporary_root="$(mktemp -d "${TMPDIR:-/tmp}/hatrie-m052ag-delivery.XXXXXX")"
trap 'rm -rf -- "$temporary_root"' EXIT

expected_paths=(
  BENCHMARK.md
  INSPIRATION.md
  M052AE_NATIVE_TRIPLE_GROUP.md
  M052AF_NATIVE_TRIPLE_GROUPED_ORDERED.md
  M052AG_NATIVE_QUAD_GROUPED_ORDERED.md
  Makefile
  hat/hatSql/m052ae_native_triple_group_test.go
  hat/hatSql/m052af_native_triple_grouped_ordered_test.go
  hat/hatSql/m052ag_native_quad_grouped_ordered.go
  hat/hatSql/m052ag_native_quad_grouped_ordered_test.go
  hat/hatSql/m052ag_native_quad_grouped_ordered_benchmark_test.go
  hat/hatSql/m052c_native_dataflow.go
  hat/hatSql/m052o_native_composite_group_test.go
  hat/hatSql/m052p_auto_native_dataflow.go
  scripts/benchmark-m052ag-native-quad-grouped-ordered.sh
  scripts/deliver-m052ag-native-quad-grouped-ordered.sh
  scripts/format-m052ag-native-quad-grouped-ordered.sh
  scripts/race-m052ag-native-quad-grouped-ordered.sh
  scripts/test-m052ag-native-quad-grouped-ordered.sh
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
    M052AG_NATIVE_QUAD_GROUPED_ORDERED.md \
    hat/hatSql/m052ag_native_quad_grouped_ordered.go \
    hat/hatSql/m052ag_native_quad_grouped_ordered_test.go \
    hat/hatSql/m052ag_native_quad_grouped_ordered_benchmark_test.go \
    scripts/benchmark-m052ag-native-quad-grouped-ordered.sh \
    scripts/deliver-m052ag-native-quad-grouped-ordered.sh \
    scripts/format-m052ag-native-quad-grouped-ordered.sh \
    scripts/race-m052ag-native-quad-grouped-ordered.sh \
    scripts/test-m052ag-native-quad-grouped-ordered.sh
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
  if grep -q 'nativeSQLDataflowQuadGroupedOrderedPlanFor' "$file"; then
    printf 'M052ag native changes already exist in %s\n' "$file" >&2
    exit 1
  fi
  awk '
    {
      if (index($0, "} else if _, ok := nativeSQLDataflowGroupedOrderedPlanFor(query); !ok {") > 0) {
        print "\t\t} else if len(query.groupBy) == 4 {"
        print "\t\t\tif _, ok := nativeSQLDataflowQuadGroupedOrderedPlanFor(query); !ok {"
        print "\t\t\t\treturn fmt.Errorf(\"%w: grouped ordered query shape\", ErrSQLNativeDataflowUnsupported)"
        print "\t\t\t}"
        ordered_compile = 1
      }
      if (index($0, "} else if _, ok := nativeSQLDataflowGroupPlanFor(query); !ok {") > 0) {
        print "\t\t} else if len(query.groupBy) == 4 {"
        print "\t\t\tif _, ok := nativeSQLDataflowQuadGroupPlanFor(query); !ok {"
        print "\t\t\t\treturn fmt.Errorf(\"%w: grouped query shape\", ErrSQLNativeDataflowUnsupported)"
        print "\t\t\t}"
        ordered_compile = 1
      }
      if (index($0, "if plan, ok := nativeSQLDataflowGroupedOrderedPlanFor(query); ok {") > 0) {
        print "\tif plan, ok := nativeSQLDataflowQuadGroupedOrderedPlanFor(query); ok {"
        print "\t\treturn executeNativeSQLDataflowQuadGroupedOrdered(ctx, query, initial, plan)"
        print "\t}"
        ordered_dispatch = 1
      }
      if (index($0, "if plan, ok := nativeSQLDataflowGroupPlanFor(query); ok {") > 0) {
        print "\tif plan, ok := nativeSQLDataflowQuadGroupPlanFor(query); ok {"
        print "\t\treturn executeNativeSQLDataflowQuadGroups(ctx, query, initial, plan)"
        print "\t}"
        group_dispatch = 1
      }
      print
    }
    END {
      if (!ordered_compile || !group_dispatch || !ordered_dispatch) exit 42
    }
  ' "$file" > "$file.next"
  mv "$file.next" "$file"
}

transform_auto_native_dataflow() {
  local file="$1"
  if grep -q 'nativeSQLDataflowQuadGroupedOrderedPlanFor' "$file"; then
    printf 'M052ag automatic changes already exist in %s\n' "$file" >&2
    exit 1
  fi
  awk '
    {
      if (index($0, "len(query.groupBy) > 3") > 0) {
        sub(/len\(query\.groupBy\) > 3/, "len(query.groupBy) > 4")
        limit_updates++
      }
      if (index($0, "_, ok := nativeSQLDataflowCompositeGroupPlanFor(query)") > 0) {
        print "\tif len(query.groupBy) == 4 {"
        print "\t\t_, ok := nativeSQLDataflowQuadGroupPlanFor(query)"
        print "\t\treturn ok && validateNativeSQLDataflowQuery(query) == nil"
        print "\t}"
        group_branch++
      }
      if (index($0, "_, ok := nativeSQLDataflowCompositeGroupedOrderedPlanFor(query)") > 0) {
        print "\tif len(query.groupBy) == 4 {"
        print "\t\t_, ok := nativeSQLDataflowQuadGroupedOrderedPlanFor(query)"
        print "\t\treturn ok && validateNativeSQLDataflowQuery(query) == nil"
        print "\t}"
        ordered_branch++
      }
      print
    }
    END {
      if (limit_updates != 2 || group_branch != 1 || ordered_branch != 1) exit 42
    }
  ' "$file" > "$file.next"
  mv "$file.next" "$file"
}

transform_triple_rejection() {
  local file="$1"
  awk '
    {
      if (index($0, "src.value AS value, COUNT(*) AS total GROUP BY src.region, src.tier, src.channel, src.value") > 0) {
        gsub("src.value AS value, COUNT(*)", "src.value AS value, src.segment AS segment, COUNT(*)")
        gsub("src.channel, src.value", "src.channel, src.value, src.segment")
        changed = 1
      }
      print
    }
    END { if (!changed) exit 42 }
  ' "$file" > "$file.next"
  mv "$file.next" "$file"
}

transform_composite_rejection() {
  local file="$1"
  awk '
    {
      if (index($0, "src.value AS value, src.channel AS channel, COUNT(*)") > 0 && index($0, "GROUP BY src.region, src.tier, src.value, src.channel") > 0) {
        gsub("src.channel AS channel, COUNT(*)", "src.channel AS channel, src.segment AS segment, COUNT(*)")
        gsub("src.value, src.channel", "src.value, src.channel, src.segment")
        changed = 1
      }
      print
    }
    END { if (!changed) exit 42 }
  ' "$file" > "$file.next"
  mv "$file.next" "$file"
}

transform_m052ae_doc() {
  local file="$1"
  awk '
    {
      if ($0 == "The implementation deliberately remains fail-closed for four or more grouping") {
        print "The implementation deliberately remains fail-closed for five or more grouping"
        print "fields and bounded grouped output without a supported ordered Top-N shape."
        print "M052af adds the separate three-field grouped `HAVING` plus finite `ORDER BY`"
        print "path, and M052ag extends that path to four fields; richer or unsupported"
        print "ordered expressions still use the established executor rather than being"
        print "silently misclassified."
        skip = 4
        changed = 1
        next
      }
      if (skip > 0) {
        skip--
        next
      }
      print
    }
    END { if (!changed || skip != 0) exit 42 }
  ' "$file" > "$file.next"
  mv "$file.next" "$file"
}

transform_m052af_doc() {
  local file="$1"
  awk '
    {
      if ($0 == "The path is fail-closed for four or more group fields, `WITH TIES`, unselected") {
        print "The path is fail-closed for five or more group fields, `WITH TIES`, unselected"
        print "or expression-based order keys, unsupported `HAVING`, and other richer SQL"
        print "shapes. Four-field grouped ordered Top-N is handled by the separate M052ag"
        print "path; all other unsupported queries retain the established materialized"
        print "executor."
        skip = 2
        changed = 1
        next
      }
      if (skip > 0) {
        skip--
        next
      }
      print
    }
    END { if (!changed || skip != 0) exit 42 }
  ' "$file" > "$file.next"
  mv "$file.next" "$file"
}

transform_inspiration() {
  local file="$1"
  local section="$temporary_root/m052ag-inspiration-section"
  printf '%s\n' \
    '- [x] M052ag Native four-field grouped ordered Top-N dataflow. Four-field' \
    '  grouped aggregates reuse the fixed comparable-key and bounded Top-N pattern' \
    '  with selected aggregate `HAVING`, alias-resolved finite `ORDER BY`, `LIMIT`,' \
    '  and `OFFSET`; five-field groups and richer expressions remain fail-closed.' \
    '  The paired benchmark is 3.77x faster with 4.10x lower bytes and 43.42x fewer' \
    '  allocations; see' \
    '  [M052AG_NATIVE_QUAD_GROUPED_ORDERED.md](M052AG_NATIVE_QUAD_GROUPED_ORDERED.md)' \
    '  and [BENCHMARK.md](BENCHMARK.md#m052ag-native-four-field-grouped-ordered-top-n).' \
    > "$section"
  awk -v section="$section" '
    {
      if (index($0, "row resolvers; four-field and bounded grouped output without a supported") > 0) {
        sub("four-field", "five-field")
      }
      if (index($0, "richer order expressions, `WITH TIES`, and four-field groups remain") > 0) {
        sub("four-field", "five-field")
      }
      print
      if (!inserted && $0 == "  and [BENCHMARK.md](BENCHMARK.md#m052af-native-three-field-grouped-ordered-top-n).") {
        while ((getline line < section) > 0) print line
        close(section)
        inserted = 1
      }
    }
    END { if (!inserted) exit 42 }
  ' "$file" > "$file.next"
  mv "$file.next" "$file"
}

transform_benchmark() {
  local file="$1"
  local section="$temporary_root/m052ag-benchmark-section"
  printf '%s\n' \
    '<a id="m052ag-native-four-field-grouped-ordered-top-n"></a>' \
    '## M052ag: Native Four-Field Grouped Ordered Top-N' \
    '' \
    '`make benchmark-m052ag-native-quad-grouped-ordered` compares the materialized' \
    'executor with the four-field fixed-key native grouped Top-N path. The fixture' \
    'has 20,000 rows, four grouping fields, `COUNT(*)`, `SUM(value)`, and a' \
    '`LIMIT 100 OFFSET 25` page. The fallback explicitly sets' \
    '`DisableNativeDataflow: true`. Linux/amd64, AMD Ryzen 9 5950X, five samples per' \
    'path:' \
    '' \
    '```text' \
    'BenchmarkCompiledSQLNativeQuadGroupedOrderedBaseline-32' \
    '30059435 ns/op 30876720 B/op 276975 allocs/op' \
    '29513826 ns/op 30876741 B/op 276975 allocs/op' \
    '29226831 ns/op 30876782 B/op 276975 allocs/op' \
    '28843628 ns/op 30876712 B/op 276975 allocs/op' \
    '29535320 ns/op 30876826 B/op 276975 allocs/op' \
    'BenchmarkCompiledSQLNativeQuadGroupedOrderedNative-32' \
    '7682056 ns/op 7534180 B/op 6379 allocs/op' \
    '7884404 ns/op 7534182 B/op 6379 allocs/op' \
    '7813638 ns/op 7534179 B/op 6379 allocs/op' \
    '7852378 ns/op 7534181 B/op 6379 allocs/op' \
    '7821610 ns/op 7534180 B/op 6379 allocs/op' \
    '```' \
    '' \
    '| Path | Median time | Median bytes | Median allocs | Relative result |' \
    '| --- | ---: | ---: | ---: | --- |' \
    '| Materialized fallback | 29.514 ms/op | 30,876,741 B/op | 276,975 | 1.00x |' \
    '| Native four-field grouped Top-N | 7.822 ms/op | 7,534,180 B/op | 6,379 | **3.77x faster; 4.10x lower bytes; 43.42x fewer allocations** |' \
    '' \
    'Five-field groups, `WITH TIES`, unsupported order expressions, and richer SQL' \
    'remain on the established fallback. See' \
    '[M052AG_NATIVE_QUAD_GROUPED_ORDERED.md](M052AG_NATIVE_QUAD_GROUPED_ORDERED.md).' \
    > "$section"
  awk -v section="$section" '
    {
      if ($0 == "The result is an unbounded grouped aggregation optimization only. Four or more") {
        print "The result is an unbounded grouped aggregation optimization only. Five or more"
        next
      }
      if ($0 == "grouping fields and bounded grouped output without the supported ordered Top-N") {
        print "grouping fields and bounded grouped output without a supported ordered Top-N"
        next
      }
      if ($0 == "shape retain the established fallback boundary. M052af adds the supported") {
        print "shape retain the established fallback boundary. M052af adds the supported"
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
    END { if (!inserted) exit 42 }
  ' "$file" > "$file.next"
  mv "$file.next" "$file"
}

transform_makefile() {
  local file="$1"
  if grep -q '^test-m052ag-native-quad-grouped-ordered:' "$file"; then
    printf 'M052ag Makefile targets already exist in HEAD\n' >&2
    exit 1
  fi
  cat >> "$file" <<'EOF'

.PHONY: test-m052ag-native-quad-grouped-ordered
test-m052ag-native-quad-grouped-ordered:
	@bash scripts/test-m052ag-native-quad-grouped-ordered.sh

.PHONY: format-m052ag-native-quad-grouped-ordered
format-m052ag-native-quad-grouped-ordered:
	@bash scripts/format-m052ag-native-quad-grouped-ordered.sh

.PHONY: benchmark-m052ag-native-quad-grouped-ordered
benchmark-m052ag-native-quad-grouped-ordered:
	@bash scripts/benchmark-m052ag-native-quad-grouped-ordered.sh

.PHONY: race-m052ag-native-quad-grouped-ordered
race-m052ag-native-quad-grouped-ordered:
	@bash scripts/race-m052ag-native-quad-grouped-ordered.sh

.PHONY: plan-m052ag-native-quad-grouped-ordered stage-m052ag-native-quad-grouped-ordered commit-m052ag-native-quad-grouped-ordered push-m052ag-native-quad-grouped-ordered deliver-m052ag-native-quad-grouped-ordered
plan-m052ag-native-quad-grouped-ordered:
	@bash scripts/deliver-m052ag-native-quad-grouped-ordered.sh plan

stage-m052ag-native-quad-grouped-ordered:
	@bash scripts/deliver-m052ag-native-quad-grouped-ordered.sh stage

commit-m052ag-native-quad-grouped-ordered:
	@bash scripts/deliver-m052ag-native-quad-grouped-ordered.sh commit

push-m052ag-native-quad-grouped-ordered:
	@bash scripts/deliver-m052ag-native-quad-grouped-ordered.sh push

deliver-m052ag-native-quad-grouped-ordered:
	@bash scripts/deliver-m052ag-native-quad-grouped-ordered.sh deliver
EOF
}

stage_existing_changes() {
  apply_generated_patch hat/hatSql/m052c_native_dataflow.go transform_native_dataflow
  apply_generated_patch hat/hatSql/m052p_auto_native_dataflow.go transform_auto_native_dataflow
  apply_generated_patch hat/hatSql/m052ae_native_triple_group_test.go transform_triple_rejection
  apply_generated_patch hat/hatSql/m052af_native_triple_grouped_ordered_test.go transform_triple_rejection
  apply_generated_patch hat/hatSql/m052o_native_composite_group_test.go transform_composite_rejection
  apply_generated_patch M052AE_NATIVE_TRIPLE_GROUP.md transform_m052ae_doc
  apply_generated_patch M052AF_NATIVE_TRIPLE_GROUPED_ORDERED.md transform_m052af_doc
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
  printf '%s\n' 'Selective M052ag staged paths:'
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
    git commit -m 'feat(sql): add native four-field grouped Top-N [skip ci]'
    ;;
  push)
    git push origin HEAD
    ;;
  deliver)
    stage_changes
    git commit -m 'feat(sql): add native four-field grouped Top-N [skip ci]'
    git push origin HEAD
    ;;
esac
