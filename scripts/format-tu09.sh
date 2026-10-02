#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatBackup/tu09_snapshot_join.go hat/hatBackup/tu09_snapshot_join_test.go hat/hatBackup/tu09_snapshot_join_benchmark_test.go
