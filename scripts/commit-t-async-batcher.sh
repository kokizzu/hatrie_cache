#!/usr/bin/env bash
set -euo pipefail

files=(
	ASYNC_BATCHER.md
	README.md
	hat/hatPipeline/async_batcher.go
	hat/hatPipeline/async_batcher_test.go
	scripts/benchmark-t-async-batcher.sh
	scripts/format-t-async-batcher.sh
	scripts/race-t-async-batcher.sh
	scripts/test-t-async-batcher-package.sh
	scripts/test-t-async-batcher.sh
	scripts/vet-t-async-batcher.sh
	scripts/commit-t-async-batcher.sh
	scripts/publish-t-async-batcher.sh
)

git add "${files[@]}"
git diff --cached --check -- "${files[@]}"
git diff --cached --quiet -- "${files[@]}" && {
	printf '%s\n' 'no async batcher changes are staged'
	exit 1
}
git commit --only -m 'feat(pipeline): add bounded async batcher' -- "${files[@]}"
