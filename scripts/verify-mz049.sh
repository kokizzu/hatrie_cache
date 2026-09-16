#!/usr/bin/env bash
set -euo pipefail

mode=${1:?verification mode is required}
repo_root=$(pwd)
worktree=$(mktemp -d /tmp/hatrie-cache-mz049-verify.XXXXXX)

cleanup() {
	git -C "$repo_root" worktree remove --force "$worktree" >/dev/null 2>&1 || rm -rf "$worktree"
}
trap cleanup EXIT

git -C "$repo_root" worktree add --detach "$worktree" HEAD >/dev/null
cp "$repo_root/hat/hatSql/mz049_schema_drift_quarantine.go" "$worktree/hat/hatSql/mz049_schema_drift_quarantine.go"
cp "$repo_root/hat/hatSql/mz049_schema_drift_quarantine_test.go" "$worktree/hat/hatSql/mz049_schema_drift_quarantine_test.go"
cp "$repo_root/hat/hatSql/mz049_schema_drift_baseline_benchmark_test.go" "$worktree/hat/hatSql/mz049_schema_drift_baseline_benchmark_test.go"
cp "$repo_root/hat/hatSql/mz049_schema_drift_quarantine_benchmark_test.go" "$worktree/hat/hatSql/mz049_schema_drift_quarantine_benchmark_test.go"
cd "$worktree"

case "$mode" in
test)
	go test ./hat/hatSql -run '^TestMZ049SchemaDrift'
	;;
race)
	go test -race ./hat/hatSql -run '^TestMZ049SchemaDrift'
	;;
vet)
	go vet ./hat/hatSql
	;;
full)
	go test ./hat/hatSql
	;;
*)
	printf 'usage: %s {test|race|vet|full}\n' "$0" >&2
	exit 2
	;;
esac
