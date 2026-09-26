#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/m052af_native_triple_grouped_ordered.go hat/hatSql/m052af_native_triple_grouped_ordered_test.go hat/hatSql/m052af_native_triple_grouped_ordered_benchmark_test.go
