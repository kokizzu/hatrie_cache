#!/usr/bin/env bash
set -euo pipefail

git diff --check
printf '%s\n' '## status'
git status --short
printf '%s\n' '## feature diff stat'
git diff --stat -- \
	Makefile \
	ENGINE_IDEAS.md \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	BENCHMARK.md \
	CH005_ADAPTIVE_DELETE_BITMAP.md \
	hat/hatDataStructure/persistent_delete_bitmap.go \
	hat/hatDataStructure/persistent_delete_bitmap_test.go \
	hat/hatDataStructure/ch005_adaptive_delete_bitmap_test.go \
	scripts/test-ch005-adaptive-delete-bitmap.sh \
	scripts/test-ch005-data-structure.sh \
	scripts/benchmark-ch005-adaptive-delete-bitmap.sh \
	scripts/format-ch005-adaptive-delete-bitmap.sh \
	scripts/race-ch005-adaptive-delete-bitmap.sh \
	scripts/vet-ch005-adaptive-delete-bitmap.sh \
	scripts/benchmark-ch005-delete-bitmap-package.sh \
	scripts/review-ch005-adaptive-delete-bitmap.sh \
	scripts/stage-ch005-adaptive-delete-bitmap.sh \
	scripts/commit-ch005-adaptive-delete-bitmap.sh \
	scripts/push-ch005-adaptive-delete-bitmap.sh
printf '%s\n' '## documentation anchors'
rg -n 'CH-005 Adaptive Delete Bitmap Encoding|CH005_ADAPTIVE_DELETE_BITMAP|version 1 dense' CH005_ADAPTIVE_DELETE_BITMAP.md ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md ENGINE_IDEAS.md
