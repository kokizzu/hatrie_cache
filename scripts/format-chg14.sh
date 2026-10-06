#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/query.go hat/hatSql/prepared_cache_key.go hat/hatSql/chg14_plan_cache_metrics_test.go hat/hatSql/codex_chg14_compat_test.go
