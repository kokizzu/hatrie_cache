#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/c213_compiled_plan_cache.go hat/hatSql/m049_compiled_query_cache_singleflight_test.go
