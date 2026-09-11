#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/query.go hat/hatSql/partitioned_source_test.go hat/hatCache/sql_query.go
