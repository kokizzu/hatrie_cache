#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatSql/query.go \
	hat/hatSql/hash_group_aggregate.go \
	hat/hatSql/with_fill.go \
	hat/hatSql/with_fill_query.go \
	hat/hatSql/with_fill_query_test.go
