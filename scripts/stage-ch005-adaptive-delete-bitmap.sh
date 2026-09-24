#!/usr/bin/env bash
set -euo pipefail

git add -- \
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

git diff --cached --check
