#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/operator_memory.go hat/hatSql/operator_memory_test.go hat/hatSql/chg42_operator_memory_test.go hat/hatSql/query.go
