#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
gofmt -w hat/hatSql/ch002_physical_part_pruning_test.go hat/hatSql/ch002_physical_part_pruning_benchmark_test.go
