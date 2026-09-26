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

temporary_root="$(mktemp -d "${TMPDIR:-/tmp}/hatrie-m052ae-delivery.XXXXXX")"
trap 'rm -rf -- "$temporary_root"' EXIT

expected_paths=(
  BENCHMARK.md
  INSPIRATION.md
  M052AE_NATIVE_TRIPLE_GROUP.md
  Makefile
  hat/hatSql/m052ae_native_triple_group.go
  hat/hatSql/m052ae_native_triple_group_test.go
  hat/hatSql/m052ae_native_triple_group_benchmark_test.go
  hat/hatSql/m052c_native_dataflow.go
  hat/hatSql/m052o_native_composite_group_test.go
  hat/hatSql/m052p_auto_native_dataflow.go
  scripts/benchmark-m052ae-native-triple-group.sh
  scripts/deliver-m052ae-native-triple-group.sh
  scripts/format-m052ae-native-triple-group.sh
  scripts/race-m052ae-native-triple-group.sh
  scripts/test-m052ae-native-triple-group.sh
  scripts/test-m052ae-sql-package.sh
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
    M052AE_NATIVE_TRIPLE_GROUP.md \
    hat/hatSql/m052ae_native_triple_group.go \
    hat/hatSql/m052ae_native_triple_group_test.go \
    hat/hatSql/m052ae_native_triple_group_benchmark_test.go \
    scripts/benchmark-m052ae-native-triple-group.sh \
    scripts/deliver-m052ae-native-triple-group.sh \
    scripts/format-m052ae-native-triple-group.sh \
    scripts/race-m052ae-native-triple-group.sh \
    scripts/test-m052ae-native-triple-group.sh \
    scripts/test-m052ae-sql-package.sh
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
  if grep -q 'nativeSQLDataflowTripleGroupPlanFor' "$file"; then
    printf 'triple-group native changes already exist in %s\n' "$file" >&2
    exit 1
  fi
  awk '
    {
      if (!compile_inserted && index($0, "} else if _, ok := nativeSQLDataflowGroupPlanFor(query); !ok {") > 0) {
        print "\t\t} else if len(query.groupBy) == 3 {"
        print "\t\t\tif _, ok := nativeSQLDataflowTripleGroupPlanFor(query); !ok {"
        print "\t\t\t\treturn fmt.Errorf(\"%w: grouped query shape\", ErrSQLNativeDataflowUnsupported)"
        print "\t\t\t}"
        compile_inserted = 1
      }
      if (!dispatch_inserted && index($0, "if aggregates, ok := nativeSQLDataflowAggregatePlan(query); ok {") > 0) {
        print "\tif plan, ok := nativeSQLDataflowTripleGroupPlanFor(query); ok {"
        print "\t\treturn executeNativeSQLDataflowTripleGroups(ctx, query, initial, plan)"
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
  if grep -q 'nativeSQLDataflowTripleGroupPlanFor' "$file"; then
    printf 'triple-group automatic changes already exist in %s\n' "$file" >&2
    exit 1
  fi
  awk '
    {
      if (index($0, "len(query.groupBy) > 2") > 0) {
        sub(/len\(query\.groupBy\) > 2/, "len(query.groupBy) > 3")
      }
      if (!triple_inserted && index($0, "_, ok := nativeSQLDataflowCompositeGroupPlanFor(query)") > 0) {
        print "\tif len(query.groupBy) == 3 {"
        print "\t\t_, ok := nativeSQLDataflowTripleGroupPlanFor(query)"
        print "\t\treturn ok && validateNativeSQLDataflowQuery(query) == nil"
        print "\t}"
        triple_inserted = 1
      }
      print
    }
    END {
      if (!triple_inserted) exit 42
    }
  ' "$file" > "$file.next"
  mv "$file.next" "$file"
}

transform_composite_group_test() {
  local file="$1"
  awk '
    {
      if (index($0, "src.region AS region, src.tier AS tier, src.value AS value, COUNT(*) AS total GROUP BY src.region, src.tier, src.value") > 0) {
        sub("src.value AS value, COUNT", "src.value AS value, src.channel AS channel, COUNT")
        sub("GROUP BY src.region, src.tier, src.value", "GROUP BY src.region, src.tier, src.value, src.channel")
        replaced = 1
      }
      print
    }
    END {
      if (!replaced) exit 42
    }
  ' "$file" > "$file.next"
  mv "$file.next" "$file"
}

transform_inspiration() {
  local file="$1"
  local section="$temporary_root/m052ae-inspiration-section"
  printf '%s\n' \
    '- [x] M052ae Native three-field composite `GROUP BY` dataflow. A fixed' \
    '  comparable three-component key supports integer, string, and `NULL` fields,' \
    '  preserves first-seen group order, and is selected automatically for ordinary' \
    '  row resolvers; four-field, grouped `HAVING`, grouped ordering, and bounded' \
    '  grouped output remain fail-closed. The paired benchmark is 3.79x faster with' \
    '  4.39x lower bytes and 50.0x fewer allocations; see' \
    '  [M052AE_NATIVE_TRIPLE_GROUP.md](M052AE_NATIVE_TRIPLE_GROUP.md) and' \
    '  [BENCHMARK.md](BENCHMARK.md#m052ae-native-three-field-group-by).' \
    > "$section"
  awk -v section="$section" '
    {
      print
      if (!inserted && $0 == "  and [BENCHMARK.md](BENCHMARK.md#m052ad-automatic-native-conditional-aggregates).") {
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
  local section="$temporary_root/m052ae-benchmark-section"
  printf '%s\n' \
    '<a id="m052ae-native-three-field-group-by"></a>' \
    '## M052ae: Native Three-Field GROUP BY' \
    '' \
    '`make benchmark-m052ae-native-triple-group` compares the established' \
    'materialized executor with the new fixed-key native path for a three-field' \
    '`GROUP BY`. The fixture has 20,000 rows, about 1,536 groups, three grouping' \
    'fields, `COUNT(*)`, and `SUM(value)`. The fallback explicitly sets' \
    '`DisableNativeDataflow: true`. Linux/amd64, AMD Ryzen 9 5950X, five samples' \
    'per path:' \
    '' \
    '```text' \
    'fallback:' \
    '23637973 ns/op 27791391 B/op 235425 allocs/op' \
    '23466769 ns/op 27791260 B/op 235425 allocs/op' \
    '23010441 ns/op 27791467 B/op 235425 allocs/op' \
    '22960331 ns/op 27791267 B/op 235425 allocs/op' \
    '22513380 ns/op 27791255 B/op 235425 allocs/op' \
    'native:' \
    '6070375 ns/op 6329905 B/op 4708 allocs/op' \
    '6065369 ns/op 6329904 B/op 4708 allocs/op' \
    '6054045 ns/op 6329903 B/op 4708 allocs/op' \
    '6140381 ns/op 6329904 B/op 4708 allocs/op' \
    '6105030 ns/op 6329906 B/op 4708 allocs/op' \
    '```' \
    '' \
    '| Path | Median time | Median bytes | Median allocs | Relative result |' \
    '| --- | ---: | ---: | ---: | --- |' \
    '| Materialized fallback | 23.010 ms/op | 27,791,267 B/op | 235,425 | 1.00x |' \
    '| Native three-field grouping | 6.070 ms/op | 6,329,904 B/op | 4,708 | **3.79x faster; 4.39x lower bytes; 50.0x fewer allocations** |' \
    '' \
    'The result is an unbounded grouped aggregation optimization only. Four or more' \
    'grouping fields, grouped `HAVING`, grouped ordering, and bounded grouped output' \
    'retain the established fallback boundary. See' \
    '[M052AE_NATIVE_TRIPLE_GROUP.md](M052AE_NATIVE_TRIPLE_GROUP.md).' \
    > "$section"
  awk -v section="$section" '
    {
      if (!inserted && $0 == "<a id=\"rejected-t042-independent-setint-parallel-replay\"></a>") {
        while ((getline line < section) > 0) print line
        print ""
        close(section)
        inserted = 1
      }
      print
    }
    END {
      if (!inserted) exit 42
    }
  ' "$file" > "$file.next"
  mv "$file.next" "$file"
}

transform_makefile() {
  local file="$1"
  if grep -q '^test-m052ae-native-triple-group:' "$file"; then
    printf 'M052ae Makefile targets already exist in HEAD\n' >&2
    exit 1
  fi
  cat >> "$file" <<'EOF'

.PHONY: test-m052ae-native-triple-group
test-m052ae-native-triple-group:
	@bash scripts/test-m052ae-native-triple-group.sh

.PHONY: format-m052ae-native-triple-group
format-m052ae-native-triple-group:
	@bash scripts/format-m052ae-native-triple-group.sh

.PHONY: benchmark-m052ae-native-triple-group
benchmark-m052ae-native-triple-group:
	@bash scripts/benchmark-m052ae-native-triple-group.sh

.PHONY: test-m052ae-sql-package
test-m052ae-sql-package:
	@bash scripts/test-m052ae-sql-package.sh

.PHONY: race-m052ae-native-triple-group
race-m052ae-native-triple-group:
	@bash scripts/race-m052ae-native-triple-group.sh

.PHONY: plan-m052ae-native-triple-group stage-m052ae-native-triple-group commit-m052ae-native-triple-group push-m052ae-native-triple-group deliver-m052ae-native-triple-group
plan-m052ae-native-triple-group:
	@bash scripts/deliver-m052ae-native-triple-group.sh plan

stage-m052ae-native-triple-group:
	@bash scripts/deliver-m052ae-native-triple-group.sh stage

commit-m052ae-native-triple-group:
	@bash scripts/deliver-m052ae-native-triple-group.sh commit

push-m052ae-native-triple-group:
	@bash scripts/deliver-m052ae-native-triple-group.sh push

deliver-m052ae-native-triple-group:
	@bash scripts/deliver-m052ae-native-triple-group.sh deliver
EOF
}

stage_existing_changes() {
  apply_generated_patch hat/hatSql/m052c_native_dataflow.go transform_native_dataflow
  apply_generated_patch hat/hatSql/m052p_auto_native_dataflow.go transform_auto_native_dataflow
  apply_generated_patch hat/hatSql/m052o_native_composite_group_test.go transform_composite_group_test
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
  printf '%s\n' 'Selective M052ae staged paths:'
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
    git commit -m 'feat(sql): add native three-field grouped dataflow [skip ci]'
    ;;
  push)
    git push origin HEAD
    ;;
  deliver)
    stage_changes
    git commit -m 'feat(sql): add native three-field grouped dataflow [skip ci]'
    git push origin HEAD
    ;;
esac
