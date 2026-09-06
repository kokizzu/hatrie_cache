#!/bin/sh
set -eu

repo=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
tmp=$(mktemp -d)

cleanup() {
	git -C "$repo" worktree remove --force "$tmp" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

git -C "$repo" worktree add --detach "$tmp" origin/master >/dev/null
cp "$repo/hat/hatBackup/object_store.go" "$tmp/hat/hatBackup/"
cp "$repo/hat/hatBackup/object_store_verify_test.go" "$tmp/hat/hatBackup/"

cd "$tmp"
go test -run '^TestObjectStoreTargetVerifyChecksEveryPayload$$' ./hat/hatBackup
go test ./hat/hatBackup
go test -race ./hat/hatBackup
