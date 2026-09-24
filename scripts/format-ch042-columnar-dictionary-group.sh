#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/ch042_columnar_dictionary_group.go \
  hat/hatSql/ch042_columnar_dictionary_group_test.go \
  hat/hatSql/ch042_columnar_dictionary_group_benchmark_test.go
