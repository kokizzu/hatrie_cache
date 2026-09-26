#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/m052ae_native_triple_group.go hat/hatSql/m052ae_native_triple_group_test.go hat/hatSql/m052ae_native_triple_group_benchmark_test.go
