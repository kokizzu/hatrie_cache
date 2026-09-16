#!/usr/bin/env bash
set -euo pipefail

repo_root=$(pwd)
worktree=$(mktemp -d /tmp/hatrie-cache-ch048-red.XXXXXX)
log=$(mktemp /tmp/hatrie-cache-ch048-red-log.XXXXXX)

cleanup() {
	git -C "$repo_root" worktree remove --force "$worktree" >/dev/null 2>&1 || rm -rf "$worktree"
	rm -f "$log"
}
trap cleanup EXIT

git -C "$repo_root" worktree add --detach "$worktree" HEAD >/dev/null
cp "$repo_root/hat/hatSql/ch048_external_schema_inference_test.go" "$worktree/hat/hatSql/ch048_external_schema_inference_test.go"
cd "$worktree"
if go test ./hat/hatSql -run '^TestCH048ExternalSchemaInference'; then
	printf '%s\n' 'CH-048 red test unexpectedly passed before implementation' >&2
	exit 1
fi
printf '%s\n' 'CH-048 red test failed as expected before implementation.'
