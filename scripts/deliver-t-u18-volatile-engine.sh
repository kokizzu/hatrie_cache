#!/usr/bin/env bash
set -euo pipefail

mode=${1:-status}
files=(
  Makefile
  ADOPTED_QUERY_ENGINE_IDEAS.md
  BENCHMARK.md
  PRODUCT_IDEA_GAPS.md
  README.md
  TU18_VOLATILE_ENGINE.md
  hat/hatDataStructure/volatile_engine.go
  hat/hatDataStructure/volatile_engine_test.go
  hat/hatDataStructure/volatile_engine_benchmark_test.go
  scripts/test-t-u18-volatile-engine.sh
  scripts/deliver-t-u18-volatile-engine.sh
)

case "$mode" in
  status)
    git status --short --branch -- "${files[@]}"
    ;;
  stage)
    git add -- "${files[@]}"
    git diff --cached --check
    git status --short --branch -- "${files[@]}"
    ;;
  commit)
    git diff --cached --check
    if git diff --cached --quiet -- "${files[@]}"; then
      printf '%s\n' 'no staged T-U18 changes'
      exit 1
    fi
    git commit -m 'feat(data-structure): add explicit volatile engine [skip ci]'
    ;;
  push)
    git push -u origin HEAD
    ;;
  *)
    printf 'usage: %s {status|stage|commit|push}\n' "$0" >&2
    exit 2
    ;;
esac
