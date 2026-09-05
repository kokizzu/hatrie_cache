#!/bin/sh
set -eu

gofmt -w \
	hat/hatSql/index_advisor.go \
	hat/hatSql/index_advisor_test.go \
	hat/hatSql/index_advisor_covering_benchmark_test.go \
	sql_covering_index_advisor_api.go
