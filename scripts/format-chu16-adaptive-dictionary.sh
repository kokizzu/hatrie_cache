#!/bin/sh
set -eu

gofmt -w hat/hatSql/typed_table.go hat/hatSql/chu16_adaptive_dictionary_test.go hat/hatSql/chu16_adaptive_dictionary_benchmark_test.go
