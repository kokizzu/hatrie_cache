#!/usr/bin/env bash
set -euo pipefail

mode="${1:-plan}"
tmp_dir="$(mktemp -d "${TMPDIR:-/tmp}/ch039-delivery.XXXXXX")"
trap 'rm -rf -- "$tmp_dir"' EXIT

allowed_paths=(
  BENCHMARK.md
  INSPIRATION.md
  Makefile
  CH039_GROUPED_APPROXIMATE_AGGREGATES.md
  hat/hatSql/ch039_grouped_approx_benchmark_test.go
  hat/hatSql/ch039_grouped_approx_native_test.go
  hat/hatSql/m052ae_native_triple_group.go
  hat/hatSql/m052ag_native_quad_grouped_ordered.go
  hat/hatSql/m052c_native_dataflow.go
  scripts/benchmark-ch039-grouped-approx.sh
  scripts/deliver-ch039-grouped-approx.sh
  scripts/format-ch039-grouped-approx.sh
  scripts/race-ch039-grouped-approx.sh
  scripts/test-ch039-grouped-approx.sh
)

fail() {
  printf '%s\n' "$1" >&2
  exit 1
}

ensure_clean_index() {
  git diff --cached --quiet || fail "refusing to stage over existing staged changes"
}

make_patch() {
  local label="$1"
  local base="$2"
  local desired="$3"
  local patch="$4"
  if diff -u --label "a/$label" --label "b/$label" "$base" "$desired" > "$patch"; then
    fail "internal delivery patch unexpectedly has no changes: $label"
  else
    local status=$?
    [[ "$status" -eq 1 ]] || exit "$status"
  fi
}

build_benchmark_patch() {
  git show HEAD:BENCHMARK.md > "$tmp_dir/benchmark.base"
  awk '
    /^## CH-039 grouped approximate aggregate native dataflow$/ { capture=1 }
    capture && /^## CH-041 grouping branch plan sharing$/ { exit }
    capture { print }
  ' BENCHMARK.md > "$tmp_dir/benchmark.section"
  [[ -s "$tmp_dir/benchmark.section" ]] || fail "CH-039 benchmark section is missing"
  awk -v section="$tmp_dir/benchmark.section" '
    BEGIN {
      count = 0
      while ((getline line < section) > 0) {
        block[++count] = line
      }
      close(section)
    }
    $0 == "## CH-041 grouping branch plan sharing" {
      for (position = 1; position <= count; position++) {
        print block[position]
      }
    }
    { print }
  ' "$tmp_dir/benchmark.base" > "$tmp_dir/benchmark.desired"
  make_patch BENCHMARK.md "$tmp_dir/benchmark.base" "$tmp_dir/benchmark.desired" "$tmp_dir/benchmark.patch"
}

build_inspiration_patch() {
  git show HEAD:INSPIRATION.md > "$tmp_dir/inspiration.base"
  replacement="$(awk '/^- \[x\] C076 Approximate sketches for supported distinct and quantile queries, including native grouped/{print; exit}' INSPIRATION.md)"
  [[ -n "$replacement" ]] || fail "CH-039 inspiration entry is missing"
  awk -v replacement="$replacement" '
    /^- \[x\] C076 Approximate sketches for supported distinct and quantile queries\./ {
      print replacement
      next
    }
    { print }
  ' "$tmp_dir/inspiration.base" > "$tmp_dir/inspiration.desired"
  make_patch INSPIRATION.md "$tmp_dir/inspiration.base" "$tmp_dir/inspiration.desired" "$tmp_dir/inspiration.patch"
}

build_makefile_patch() {
  git show HEAD:Makefile > "$tmp_dir/makefile.base"
  printf '%s\n' \
    '.PHONY: test-ch039-grouped-approx' \
    'test-ch039-grouped-approx:' \
    $'\tbash scripts/test-ch039-grouped-approx.sh' \
    '' \
    '.PHONY: benchmark-ch039-grouped-approx' \
    'benchmark-ch039-grouped-approx:' \
    $'\tbash scripts/benchmark-ch039-grouped-approx.sh' \
    '' \
    '.PHONY: format-ch039-grouped-approx' \
    'format-ch039-grouped-approx:' \
    $'\tbash scripts/format-ch039-grouped-approx.sh' \
    '' \
    '.PHONY: race-ch039-grouped-approx' \
    'race-ch039-grouped-approx:' \
    $'\tbash scripts/race-ch039-grouped-approx.sh' \
    '' \
    '.PHONY: plan-ch039-grouped-approx' \
    'plan-ch039-grouped-approx:' \
    $'\tbash scripts/deliver-ch039-grouped-approx.sh plan' \
    '' \
    '.PHONY: stage-ch039-grouped-approx' \
    'stage-ch039-grouped-approx:' \
    $'\tbash scripts/deliver-ch039-grouped-approx.sh stage' \
    '' \
    '.PHONY: commit-ch039-grouped-approx' \
    'commit-ch039-grouped-approx:' \
    $'\tbash scripts/deliver-ch039-grouped-approx.sh commit' \
    '' \
    '.PHONY: push-ch039-grouped-approx' \
    'push-ch039-grouped-approx:' \
    $'\tbash scripts/deliver-ch039-grouped-approx.sh push' \
    '' \
    '.PHONY: deliver-ch039-grouped-approx' \
    'deliver-ch039-grouped-approx:' \
    $'\tbash scripts/deliver-ch039-grouped-approx.sh deliver' \
    > "$tmp_dir/makefile.block"
  awk -v block="$tmp_dir/makefile.block" '
    BEGIN {
      count = 0
      while ((getline line < block) > 0) {
        additions[++count] = line
      }
      close(block)
      marker = "\t@bash scripts/deliver-m052ag-native-quad-grouped-ordered.sh deliver"
      found = 0
    }
    $0 == marker {
      print
      for (position = 1; position <= count; position++) {
        print additions[position]
      }
      found = 1
      next
    }
    { print }
    END {
      if (!found) {
        exit 1
      }
    }
  ' "$tmp_dir/makefile.base" > "$tmp_dir/makefile.desired" || fail "M052ag delivery marker is missing from HEAD Makefile"
  make_patch Makefile "$tmp_dir/makefile.base" "$tmp_dir/makefile.desired" "$tmp_dir/makefile.patch"
}

stage_feature() {
  ensure_clean_index
  git diff --check
  git add -- \
    CH039_GROUPED_APPROXIMATE_AGGREGATES.md \
    hat/hatSql/ch039_grouped_approx_benchmark_test.go \
    hat/hatSql/ch039_grouped_approx_native_test.go \
    hat/hatSql/m052ae_native_triple_group.go \
    hat/hatSql/m052ag_native_quad_grouped_ordered.go \
    hat/hatSql/m052c_native_dataflow.go \
    scripts/benchmark-ch039-grouped-approx.sh \
    scripts/deliver-ch039-grouped-approx.sh \
    scripts/format-ch039-grouped-approx.sh \
    scripts/race-ch039-grouped-approx.sh \
    scripts/test-ch039-grouped-approx.sh
  build_benchmark_patch
  build_inspiration_patch
  build_makefile_patch
  git apply --cached "$tmp_dir/benchmark.patch"
  git apply --cached "$tmp_dir/inspiration.patch"
  git apply --cached "$tmp_dir/makefile.patch"

  declare -A allowed=()
  for path in "${allowed_paths[@]}"; do
    allowed["$path"]=1
  done
  while IFS= read -r path; do
    [[ -n "$path" ]] || continue
    [[ -n "${allowed[$path]:-}" ]] || fail "unexpected staged path: $path"
  done < <(git diff --cached --name-only)
  git diff --cached --check
  git diff --cached --stat
}

unstage_feature() {
  mapfile -t staged_paths < <(git diff --cached --name-only)
  if ((${#staged_paths[@]} != 0)); then
    git restore --staged -- "${staged_paths[@]}"
  fi
}

case "$mode" in
  plan)
    printf 'CH-039 delivery paths:\n'
    printf '  %s\n' "${allowed_paths[@]}"
    ;;
  stage)
    stage_feature
    ;;
  unstage)
    unstage_feature
    ;;
  commit)
    git diff --cached --check
    git commit -m 'feat(sql): add grouped approximate native dataflow [skip ci]'
    ;;
  push)
    git push
    ;;
  deliver)
    "$0" stage
    "$0" commit
    "$0" push
    ;;
  *)
    printf 'usage: %s {plan|stage|commit|push|deliver}\n' "$0" >&2
    exit 2
    ;;
esac
