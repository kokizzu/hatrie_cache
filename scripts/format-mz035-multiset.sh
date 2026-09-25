#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/mz035_incremental_multiset.go hat/hatSql/mz035_incremental_multiset_test.go hat/hatSql/mz035_incremental_multiset_baseline_benchmark_test.go hat/hatSql/mz035_incremental_multiset_benchmark_test.go
