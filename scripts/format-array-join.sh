#!/usr/bin/env bash
set -euo pipefail
gofmt -w hat/hatSql/array_join.go hat/hatSql/array_join_test.go hat/hatSql/query.go
