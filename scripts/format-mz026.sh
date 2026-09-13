#!/usr/bin/env bash
set -eu

gofmt -w hat/hatSql/subscription_snapshot_export.go hat/hatSql/mz026_subscription_snapshot_export_test.go hat/hatSql/mz026_subscription_snapshot_export_benchmark_test.go
