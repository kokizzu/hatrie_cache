#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatSql/columnar_numeric_predicate.go \
	hat/hatSql/columnar_numeric_predicate_test.go \
	hat/hatSql/query.go
