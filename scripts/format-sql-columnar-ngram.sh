#!/bin/sh
set -eu

gofmt -w hat/hatSql/contracts.go hat/hatSql/query.go hat/hatSql/columnar_ngram_benchmark_test.go hat/hatSql/columnar_ngram_segment_test.go hat/hatCache/sql_columnar_layout_cache.go hat/hatCache/sql_columnar_ngram_test.go
