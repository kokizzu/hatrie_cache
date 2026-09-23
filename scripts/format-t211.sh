#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatCache/t211_wal_sync_baseline_benchmark_test.go \
	hat/hatCache/t211_wal_sync_test.go \
	hat/hatCache/t211_wal_sync_validation_test.go \
	hat/hatCache/t211_wal_sync_benchmark_test.go
