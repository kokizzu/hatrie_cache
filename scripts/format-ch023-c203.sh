#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatSql/index_advisor.go \
	hat/hatSql/index_advisor_persistence.go \
	hat/hatSql/ch023_primary_prefix_test.go \
	hat/hatCache/sql_query.go
