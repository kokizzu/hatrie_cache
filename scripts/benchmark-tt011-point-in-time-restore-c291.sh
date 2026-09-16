#!/usr/bin/env bash
set -euo pipefail

repo_dir=$(pwd)
tmp_dir=$(mktemp -d)
trap 'rm -rf "$tmp_dir"' EXIT
git archive HEAD | tar -x -C "$tmp_dir"
cp "$repo_dir/hat/hatBackup/reports.go" "$tmp_dir/hat/hatBackup/reports.go"
cp "$repo_dir/hat/hatCache/backup_partition_restore.go" "$tmp_dir/hat/hatCache/backup_partition_restore.go"
cp "$repo_dir/hat/hatCache/backup_point_in_time_restore.go" "$tmp_dir/hat/hatCache/backup_point_in_time_restore.go"
cp "$repo_dir/hat/hatCache/backup_restore.go" "$tmp_dir/hat/hatCache/backup_restore.go"
cp "$repo_dir/hat/hatCache/backup_partition_restore_test.go" "$tmp_dir/hat/hatCache/backup_partition_restore_test.go"
cp "$repo_dir/hat/hatCache/tt011_restore_benchmark_test.go" "$tmp_dir/hat/hatCache/tt011_restore_benchmark_test.go"
cp "$repo_dir/hat/hatCache/tt011_restore_pit_benchmark_test.go" "$tmp_dir/hat/hatCache/tt011_restore_pit_benchmark_test.go"
cd "$tmp_dir"
go test ./hat/hatCache -run '^$' -bench '^BenchmarkTT011Restore' -benchtime=10x -benchmem -count=5
