#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/row_binary_stats.go hat/hatSql/row_binary_stats_test.go
