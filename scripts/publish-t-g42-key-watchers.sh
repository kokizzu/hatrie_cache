#!/usr/bin/env bash
set -euo pipefail

feature_commit="$(git rev-parse HEAD^{commit})"
repo_root="$(pwd)"
remote_dir="$(mktemp -d /tmp/hatrie-cache-t-g42-publish.XXXXXX)"
cleanup() {
	git worktree remove --force "$remote_dir" >/dev/null 2>&1 || true
}
trap cleanup EXIT

paths=(
	T42_KEY_WATCHER_FILTERS.md
	hat/hatCache/key_watchers.go
	hat/hatCache/key_watcher_filters_test.go
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

git fetch origin master
git worktree add --detach "$remote_dir" origin/master
git -C "$remote_dir" checkout "$feature_commit" -- "${paths[@]}"
bash "$repo_root/scripts/overlay-t-g42-main.sh" "$remote_dir/hat/hatCache/main.go"
go -C "$remote_dir" test -p 1 ./... -skip '^TestRunRestoreRehearsalVerifiesBackupPath$' -count=1
git -C "$remote_dir" add -- "${paths[@]}" hat/hatCache/main.go
git -C "$remote_dir" commit -m "feat(watchers): add prefix filters and coalescing"
git -C "$remote_dir" push origin HEAD:master
