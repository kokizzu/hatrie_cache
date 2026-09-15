#!/usr/bin/env bash
set -euo pipefail

expected=(
  ADOPTED_QUERY_ENGINE_IDEAS.md
  BENCHMARK.md
  CHU17_DENSE_INTEGER_IN.md
  Makefile
  PRODUCT_IDEA_GAPS.md
  README.md
  hat/hatSql/chu17_in_program_benchmark_test.go
  hat/hatSql/chu17_in_program_test.go
  hat/hatSql/in_program.go
  scripts/benchmark-chu17-c240.sh
  scripts/commit-chu17-c240.sh
  scripts/format-chu17-c240.sh
  scripts/push-chu17-c240.sh
  scripts/race-chu17-c240.sh
  scripts/stage-chu17-c240.sh
  scripts/test-chu17-c240.sh
  scripts/test-chu17-package-c240.sh
  scripts/vet-chu17-c240.sh
)

expected_text="$(printf '%s\n' "${expected[@]}" | sort)"
actual_text="$(git diff --cached --name-only | sort)"
if [[ "$actual_text" != "$expected_text" ]]; then
  printf '%s\n' 'staged file set does not match CH-U17 allowlist' >&2
  printf '%s\n' 'expected:' >&2
  printf '%s\n' "$expected_text" >&2
  printf '%s\n' 'actual:' >&2
  printf '%s\n' "$actual_text" >&2
  exit 1
fi

git diff --cached --check
git commit -m 'feat: compact dense integer SQL IN sets'
