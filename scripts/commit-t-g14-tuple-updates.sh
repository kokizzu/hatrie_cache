#!/usr/bin/env bash
set -euo pipefail

paths=(
  T14_TUPLE_FIELD_UPDATES.md
  hat/hatDataStructure/tuple_field_updates.go
  hat/hatDataStructure/tuple_field_updates_benchmark_test.go
  hat/hatDataStructure/tuple_field_updates_public_test.go
  hat/hatDataStructure/tuple_field_updates_test.go
  scripts/benchmark-t-g14-tuple-updates.sh
  scripts/commit-t-g14-tuple-updates.sh
  scripts/format-t-g14-tuple-updates.sh
  scripts/inspect-t-g14-context.sh
  scripts/publish-t-g14-tuple-updates.sh
  scripts/race-t-g14-tuple-updates.sh
  scripts/test-t-g14-package.sh
  scripts/test-t-g14-tuple-updates.sh
  scripts/vet-t-g14-tuple-updates.sh
)

git add -- "${paths[@]}"
git diff --cached --check -- "${paths[@]}"
git commit --only -m 'feat(tuple): add atomic field updates' -- "${paths[@]}"
