#!/usr/bin/env bash
set -euo pipefail

repo_root=$(pwd)
worktree=$(mktemp -d /tmp/hatrie-cache-ch048-benchmark.XXXXXX)

cleanup() {
	git -C "$repo_root" worktree remove --force "$worktree" >/dev/null 2>&1 || rm -rf "$worktree"
}
trap cleanup EXIT

git -C "$repo_root" worktree add --detach "$worktree" HEAD >/dev/null
cp "$repo_root/hat/hatSql/external_schema.go" "$worktree/hat/hatSql/external_schema.go"
cp "$repo_root/hat/hatSql/ch048_external_schema_inference_test.go" "$worktree/hat/hatSql/ch048_external_schema_inference_test.go"
cp "$repo_root/hat/hatSql/ch048_external_schema_inference_benchmark_test.go" "$worktree/hat/hatSql/ch048_external_schema_inference_benchmark_test.go"
cd "$worktree"
go test ./hat/hatSql -run '^$' -bench '^BenchmarkCH048ExternalSchemaInference' -benchmem -benchtime=200ms -count=5
