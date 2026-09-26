#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/columnar_array_join.go hat/hatSql/ch037_columnar_array_join_test.go
