#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatBackup/m050_logical_snapshot.go \
	hat/hatBackup/m050_logical_snapshot_test.go \
	hat/hatBackup/m050_logical_snapshot_public_test.go \
	hat/hatBackup/m050_logical_snapshot_baseline_benchmark_test.go \
	hat/hatBackup/m050_logical_snapshot_benchmark_test.go
