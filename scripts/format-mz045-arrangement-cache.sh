#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/mz045_arrangement_plan_cache.go hat/hatSql/mz045_arrangement_cache_test.go hat/hatSql/query.go
