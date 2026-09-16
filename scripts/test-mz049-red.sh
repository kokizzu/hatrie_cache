#!/usr/bin/env bash
set -euo pipefail

repo_root=$(pwd)
worktree=$(mktemp -d /tmp/hatrie-cache-mz049-red.XXXXXX)
log=$(mktemp /tmp/hatrie-cache-mz049-red-log.XXXXXX)

cleanup() {
	git -C "$repo_root" worktree remove --force "$worktree" >/dev/null 2>&1 || rm -rf "$worktree"
	rm -f "$log"
}
trap cleanup EXIT

git -C "$repo_root" worktree add --detach "$worktree" HEAD >/dev/null
cp "$repo_root/hat/hatSql/mz049_schema_drift_quarantine_test.go" "$worktree/hat/hatSql/mz049_schema_drift_quarantine_test.go"
cd "$worktree"
if go test ./hat/hatSql -run '^TestMZ049SchemaDrift'; then
	printf '%s\n' 'MZ-49 red test unexpectedly passed before implementation' >&2
	exit 1
fi
printf '%s\n' 'MZ-49 red test failed as expected before implementation.'
