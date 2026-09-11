#!/usr/bin/env bash
set -euo pipefail

git diff -- hat/hatSql/contracts.go hat/hatSql/query.go hat/hatSql/session.go hat/hatSql/m052p_auto_native_dataflow.go hat/hatSql/ordered_range.go hat/hatCache/sql_query.go hat/hatCache/monitoring.go
