#!/usr/bin/env bash
set -euo pipefail

paths=(
	T15_TUPLE_FORMAT.md
	hat/hatDataStructure/tuple_format.go
	hat/hatDataStructure/tuple_format_test.go
	hat/hatDataStructure/tuple_field_offsets.go
	hat/hatDataStructure/tuple_field_updates.go
	scripts/format-t-g15-tuple-format.sh
	scripts/test-t-g15-tuple-format.sh
	scripts/test-t-g15-tuple-format-package.sh
	scripts/race-t-g15-tuple-format.sh
	scripts/vet-t-g15-tuple-format.sh
	scripts/benchmark-t-g15-tuple-format.sh
	scripts/commit-t-g15-tuple-format.sh
	scripts/publish-t-g15-tuple-format.sh
)

git add -- "${paths[@]}"
git commit --only -m "feat(tuple): add typed positional tuple formats" -- "${paths[@]}"
