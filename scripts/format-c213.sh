#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatSql/c213_compiled_plan_cache.go \
	hat/hatSql/c213_compiled_plan_execution.go \
	hat/hatSql/c213_compiled_plan_cache_test.go \
	hat/hatSql/query.go
