#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatSql/typed_table_sorted_arrangement.go \
	hat/hatSql/typed_table_sorted_arrangement_dictionary.go \
	hat/hatSql/mz026_adaptive_dictionary_arrangement_test.go \
	hat/hatSql/mz026_adaptive_dictionary_arrangement_benchmark_test.go
