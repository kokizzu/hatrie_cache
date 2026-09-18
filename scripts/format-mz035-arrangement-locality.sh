#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/mz024_arrangement_selection.go hat/hatSql/mu012_arrangement_explain.go hat/hatSql/mz035_arrangement_locality_test.go hat/hatSql/mz035_arrangement_locality_public_test.go hat/hatSql/mz035_arrangement_locality_benchmark_test.go
