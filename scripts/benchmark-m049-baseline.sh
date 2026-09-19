#!/usr/bin/env bash
set -euo pipefail

repo=$(git rev-parse --show-toplevel)
head=$(git rev-parse HEAD)
stage=$(mktemp -d ${TMPDIR:-/tmp}/hatrie-m049-baseline.XXXXXX)
archive=$(mktemp ${TMPDIR:-/tmp}/hatrie-m049-baseline.XXXXXX.tar)
trap 'rm -rf "$stage" "$archive"' EXIT

git archive --format=tar --output="$archive" "$head"
tar -xf "$archive" -C "$stage"
cp "$repo/hat/hatSql/m049_catalog_migration_baseline_benchmark_test.go" "$stage/hat/hatSql/"
cd "$stage"
go test ./hat/hatSql -run '^$' -bench '^BenchmarkM049Baseline$' -benchmem -count=5
