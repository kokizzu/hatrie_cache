#!/usr/bin/env bash
set -euo pipefail

paths=(
	T42_KEY_WATCHER_FILTERS.md
	hat/hatCache/key_watchers.go
	hat/hatCache/key_watcher_filters_test.go
	hat/hatCache/main.go
	scripts/format-t-g42-key-watchers.sh
	scripts/test-t-g42-key-watchers.sh
	scripts/test-t-g42-key-watchers-package.sh
	scripts/race-t-g42-key-watchers.sh
	scripts/vet-t-g42-key-watchers.sh
	scripts/benchmark-t-g42-key-watchers.sh
	scripts/overlay-t-g42-main.sh
	scripts/commit-t-g42-key-watchers.sh
	scripts/publish-t-g42-key-watchers.sh
)

git add -- "${paths[@]}"
git commit --only -m "feat(watchers): add prefix filters and coalescing" -- "${paths[@]}"
