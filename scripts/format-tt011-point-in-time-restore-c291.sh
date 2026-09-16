#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatBackup/reports.go \
  hat/hatCache/backup_partition_restore.go \
  hat/hatCache/backup_point_in_time_restore.go \
  hat/hatCache/backup_restore.go \
  hat/hatCache/backup_partition_restore_test.go \
  hat/hatCache/tt011_point_in_time_restore_test.go \
  hat/hatCache/tt011_restore_benchmark_test.go \
  hat/hatCache/tt011_restore_pit_benchmark_test.go \
  cmd/hatrie-cli/main.go \
  cmd/hatrie-cli/tt011_point_in_time_restore_test.go
