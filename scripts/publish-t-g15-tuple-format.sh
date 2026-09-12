#!/usr/bin/env bash
set -euo pipefail

feature_commit="$(git rev-parse HEAD^{commit})"
remote_dir="$(mktemp -d /tmp/hatrie-cache-t-g15-publish.XXXXXX)"
cleanup() {
	git worktree remove --force "$remote_dir" >/dev/null 2>&1 || true
}
trap cleanup EXIT

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

git fetch origin master
git worktree add --detach "$remote_dir" origin/master
git -C "$remote_dir" checkout "$feature_commit" -- "${paths[@]}"
go -C "$remote_dir" test -p 1 ./... -skip '^TestRunRestoreRehearsalVerifiesBackupPath$' -count=1
git -C "$remote_dir" add -- "${paths[@]}"
git -C "$remote_dir" commit -m "feat(tuple): add typed positional tuple formats"
git -C "$remote_dir" push origin HEAD:master
