#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/model.go hat/hatSql/query.go hat/hatSql/ch022_explain_pruning_test.go hat/hatSql/ch022_explain_pruning_benchmark_test.go
