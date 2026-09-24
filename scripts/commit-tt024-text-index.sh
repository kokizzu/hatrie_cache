#!/usr/bin/env bash
set -euo pipefail

paths=(
  Makefile
  ADOPTED_QUERY_ENGINE_IDEAS.md
  BENCHMARK.md
  ENGINE_IDEAS.md
  README.md
  TT024_POSITIONAL_TEXT_INDEX.md
  hat/hatSchema/materialized.go
  hat/hatSchema/text_index.go
  hat/hatSchema/text_index_resolver.go
  hat/hatSchema/tt024_text_proximity_index_benchmark_test.go
  hat/hatSchema/tt024_text_proximity_index_test.go
  scripts/benchmark-tt024-text-index.sh
  scripts/commit-tt024-text-index.sh
  scripts/format-tt024-text-index.sh
  scripts/push-tt024-text-index.sh
  scripts/test-tt024-text-index.sh
  scripts/verify-tt024-dependencies.sh
  scripts/verify-tt024-text-index.sh
)

git add -- "${paths[@]}"
git diff --cached --check
git commit -m "feat: add positional materialized text indexes"
