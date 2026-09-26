#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/ch048_numeric_in_test.go hat/hatSql/columnar_numeric_in_predicate.go hat/hatSql/query.go
