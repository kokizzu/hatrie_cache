#!/usr/bin/env bash
set -euo pipefail

paths=(
  T06_HASH_INDEX.md
  hat/hatDataStructure/hash_index.go
  hat/hatDataStructure/hash_index_benchmark_test.go
  hat/hatDataStructure/hash_index_public_test.go
  hat/hatDataStructure/hash_index_test.go
  scripts/benchmark-t-g06-hash-index.sh
  scripts/commit-t-g06-hash-index.sh
  scripts/format-t-g06-hash-index.sh
  scripts/inspect-t-g06-context.sh
  scripts/publish-t-g06-hash-index.sh
  scripts/race-t-g06-hash-index.sh
  scripts/test-t-g06-hash-index.sh
  scripts/test-t-g06-package.sh
  scripts/vet-t-g06-hash-index.sh
)

git add -- "${paths[@]}"
git diff --cached --check -- "${paths[@]}"
git commit --only -m 'feat(index): add typed hash index' -- "${paths[@]}"
