#!/usr/bin/env bash
set -euo pipefail

tmp_dir=$(mktemp -d)
trap 'rm -rf "$tmp_dir"' EXIT
git archive HEAD | tar -x -C "$tmp_dir"
cp hat/hatBackup/ch022_catalog_backup_baseline_benchmark_test.go "$tmp_dir/hat/hatBackup/"
(cd "$tmp_dir" && GOMAXPROCS=1 go test ./hat/hatBackup -run '^$' -bench '^BenchmarkCH022ObjectStoreBackupNoCatalogBaseline$' -benchmem -benchtime=200ms -count=5)
