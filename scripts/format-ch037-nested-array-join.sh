#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/array_join.go hat/hatSql/query.go hat/hatSql/ch037_nested_array_join_test.go
