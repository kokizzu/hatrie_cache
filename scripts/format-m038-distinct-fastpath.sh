#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/differential_distinct.go hat/hatSql/m038_differential_distinct_small_batch_test.go
