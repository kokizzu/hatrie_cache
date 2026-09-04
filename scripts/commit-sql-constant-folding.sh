#!/bin/sh
set -eu

mode="${1:-inspect}"
root="$(git rev-parse --show-toplevel)"
cd "$root"

feature_paths="
ADOPTED_QUERY_ENGINE_IDEAS.md
BENCHMARK.md
CONSTANT_FOLDING.md
INSPIRATION.md
README.md
hat/hatSql/rewrite.go
hat/hatSql/constant_folding_test.go
hat/hatSql/constant_folding_benchmark_test.go
scripts/audit-next-inspiration.sh
scripts/benchmark-sql-constant-folding.sh
scripts/format-sql-constant-folding.sh
scripts/test-race-sql-constant-folding.sh
scripts/test-sql-constant-folding.sh
scripts/vet-sql-constant-folding.sh
scripts/commit-sql-constant-folding.sh
"

stage_makefile_targets() {
  if git diff --cached -- Makefile | rg -q '^\+\.PHONY: (audit-next-inspiration|test-sql-constant-folding|benchmark-sql-constant-folding|format-sql-constant-folding|test-race-sql-constant-folding|vet-sql-constant-folding)$'; then
    git restore --staged -- Makefile
  fi

  stage_dir="$(mktemp -d /tmp/hatrie-constant-folding-stage.XXXXXX)"
  trap 'rm -rf "$stage_dir"' EXIT HUP INT TERM
  git show HEAD:Makefile > "$stage_dir/base"
  awk '
    /^\.PHONY:/ {
      if (capture && $0 !~ /^\.PHONY: (audit-next-inspiration|test-sql-constant-folding|benchmark-sql-constant-folding|format-sql-constant-folding|test-race-sql-constant-folding|vet-sql-constant-folding)$/) {
        exit
      }
      if ($0 ~ /^\.PHONY: (audit-next-inspiration|test-sql-constant-folding|benchmark-sql-constant-folding|format-sql-constant-folding|test-race-sql-constant-folding|vet-sql-constant-folding)$/) {
        capture = 1
      }
    }
    capture { print }
  ' Makefile > "$stage_dir/feature-targets"
  while [ "$(tail -n 1 "$stage_dir/feature-targets")" = "" ]; do
    sed -i '${/^$/d;}' "$stage_dir/feature-targets"
  done
  if test ! -s "$stage_dir/feature-targets"; then
    printf '%s\n' 'missing constant-folding Makefile targets' >&2
    exit 1
  fi
  cat "$stage_dir/base" "$stage_dir/feature-targets" > "$stage_dir/desired"
  if diff -u --label a/Makefile --label b/Makefile "$stage_dir/base" "$stage_dir/desired" > "$stage_dir/patch"; then
    :
  else
    status=$?
    test "$status" -eq 1
  fi
  git apply --cached "$stage_dir/patch"
  trap - EXIT HUP INT TERM
  rm -rf "$stage_dir"
}

stage_feature() {
  git add -- $feature_paths
  stage_makefile_targets
  git diff --cached --check
}

case "$mode" in
inspect)
  git status --short
  git diff --stat -- $feature_paths Makefile
  git diff --check -- $feature_paths
  ;;
stage)
  stage_feature
  git diff --cached --stat
  ;;
commit)
  stage_feature
  git commit -m "sql: fold deterministic constant expressions"
  ;;
push)
  git push origin HEAD
  ;;
*)
  printf 'usage: %s [inspect|stage|commit|push]\n' "$0" >&2
  exit 2
  ;;
esac
