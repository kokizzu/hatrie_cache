#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/mu013_cardinality_estimates.go hat/hatSql/mu013_cardinality_estimates_test.go hat/hatSql/query.go
