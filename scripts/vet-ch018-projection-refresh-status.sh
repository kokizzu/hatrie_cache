#!/usr/bin/env bash
set -euo pipefail

repo=$(pwd)
worktree=$(mktemp -d /tmp/hatrie-cache-ch018-vet.XXXXXX)
cleanup() {
	git -C "$repo" worktree remove --force "$worktree" >/dev/null 2>&1 || true
}
trap cleanup EXIT

git -C "$repo" worktree add --detach "$worktree" HEAD >/dev/null
cp "$repo/hat/hatSql/ch018_projection_refresh_status.go" "$worktree/hat/hatSql/"
cp "$repo/hat/hatSql/ch018_projection_refresh_status_test.go" "$worktree/hat/hatSql/"
cp "$repo/hat/hatSql/incremental_projection.go" "$worktree/hat/hatSql/"

cd "$worktree"
go vet ./hat/hatSql
