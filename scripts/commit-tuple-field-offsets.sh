#!/usr/bin/env bash
set -euo pipefail

paths=(
  TR19_TUPLE_FIELD_OFFSETS.md
  hat/hatDataStructure/tuple_field_offsets.go
  hat/hatDataStructure/tuple_field_offsets_public_test.go
  hat/hatDataStructure/tuple_field_offsets_test.go
  scripts/benchmark-tuple-field-offsets.sh
  scripts/commit-tuple-field-offsets.sh
  scripts/format-tuple-field-offsets.sh
  scripts/publish-tuple-field-offsets.sh
  scripts/race-tuple-field-offsets.sh
  scripts/test-tuple-field-offsets-package.sh
  scripts/test-tuple-field-offsets.sh
  scripts/vet-tuple-field-offsets.sh
)

git add -- "${paths[@]}"
git diff --cached --check -- "${paths[@]}"
git commit --only -m 'feat(tuple): add cached field offsets' -- "${paths[@]}"
