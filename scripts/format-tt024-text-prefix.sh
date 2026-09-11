#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatCache/sql_text_prefix_test.go hat/hatCache/sql_text_prefix_benchmark_test.go hat/hatCache/sql_text_prefix.go hat/hatCache/sql_query.go hat/hatCache/monitoring.go hat/hatSql/contracts.go hat/hatSql/query.go hat/hatSql/rewrite.go hat/hatSql/text.go hat/hatSql/text_prefix.go
