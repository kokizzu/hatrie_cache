#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/ch002_primary_mark_pruning_test.go hat/hatSql/ch002_primary_mark_pruning_benchmark_test.go hat/hatCache/ch002_primary_mark_pruning_test.go hat/hatCache/ch002_primary_mark_pruning_benchmark_test.go
