#!/usr/bin/env bash
set -euo pipefail

paths=(
  hat/hatDataStructure/tt045_tuple_compression.go
  hat/hatDataStructure/tt045_tuple_compression_test.go
  hat/hatDataStructure/tt045_tuple_compression_benchmark_test.go
  hat/hatDataStructure/tt045_tuple_compression_size_benchmark_test.go
  scripts/format-tt045-c309.sh
  scripts/test-tt045-c309.sh
  scripts/test-race-tt045-c309.sh
  scripts/vet-tt045-c309.sh
  scripts/benchmark-tt045-c309.sh
  scripts/review-tt045-c310.sh
  scripts/stage-tt045-c310.sh
  scripts/inspect-staged-tt045-c310.sh
  scripts/commit-tt045-c310.sh
  scripts/push-tt045-c310.sh
  TT045_TUPLE_COMPRESSION.md
  README.md
  ENGINE_IDEAS.md
  ADOPTED_QUERY_ENGINE_IDEAS.md
  BENCHMARK.md
)

for path in "${paths[@]}"; do
  [[ -e "$path" ]] || { printf 'missing feature path: %s\n' "$path" >&2; exit 1; }
done

printf '%s\n' 'Current worktree status:'
git status --short
printf '%s\n' 'Feature diff whitespace check:'
git diff --check -- "${paths[@]}"
