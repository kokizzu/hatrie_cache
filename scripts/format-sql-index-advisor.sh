#!/bin/sh
set -eu

gofmt -w \
	hat/hatCache/sql_query.go \
	hat/hatSql/index_advisor_persistence.go \
	hat/hatSql/index_advisor_persistence_benchmark_test.go \
	hat/hatSql/index_advisor_persistence_test.go \
	sql_index_advisor_api.go
