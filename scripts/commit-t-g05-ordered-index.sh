#!/usr/bin/env bash
set -euo pipefail

paths=(
  T05_ORDERED_INDEX_ITERATOR.md
  hat/hatDataStructure/ordered_index.go
  hat/hatDataStructure/ordered_index_benchmark_test.go
  hat/hatDataStructure/ordered_index_public_test.go
  hat/hatDataStructure/ordered_index_test.go
  scripts/benchmark-t-g05-mutation.sh
  scripts/benchmark-t-g05-ordered-index.sh
  scripts/commit-t-g05-ordered-index.sh
  scripts/format-t-g05-ordered-index.sh
  scripts/inspect-t-g05-context.sh
  scripts/inspect-t-g05-source.sh
  scripts/race-t-g05-ordered-index.sh
  scripts/test-t-g05-ordered-index.sh
  scripts/test-t-g05-package.sh
  scripts/vet-t-g05-ordered-index.sh
  scripts/publish-t-g05-ordered-index.sh
)

git add -- "${paths[@]}"
git diff --cached --check -- "${paths[@]}"
git commit --only -m 'feat(index): add ordered iterator' -- "${paths[@]}"
