#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/query.go \
  hat/hatSql/hash_group_aggregate.go \
  hat/hatSql/ch038_or_null_columnar_test.go
