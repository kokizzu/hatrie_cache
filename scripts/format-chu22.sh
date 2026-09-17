#!/bin/sh
set -eu
gofmt -w hat/hatSql/typed_table_columnar_append.go hat/hatSql/chu22_columnar_append_test.go hat/hatSql/chu22_columnar_append_benchmark_test.go
