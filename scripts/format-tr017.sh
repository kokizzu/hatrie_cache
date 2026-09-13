#!/usr/bin/env bash
set -eu

gofmt -w \
	hat/hatSql/query.go \
	hat/hatSql/tr017_single_source_execrow_test.go \
	hat/hatSql/tr017_single_source_execrow_benchmark_test.go
