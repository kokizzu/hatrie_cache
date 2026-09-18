#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/tt049_row_locks.go hat/hatSql/tr037_deadlock_detection_test.go hat/hatSql/tr037_deadlock_detection_public_test.go hat/hatSql/tr037_deadlock_detection_benchmark_test.go
