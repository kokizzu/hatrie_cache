#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/columnar_null_predicate.go hat/hatSql/ch048_null_predicate_test.go
