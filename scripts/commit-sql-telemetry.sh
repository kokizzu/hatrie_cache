#!/bin/sh
set -eu

git add Makefile SQL.md hat/hatSql/sql_telemetry.go hat/hatSql/sql_telemetry_test.go scripts/commit-sql-telemetry.sh scripts/format-sql-telemetry.sh scripts/inspect-sql-telemetry.sh scripts/push-sql-telemetry.sh scripts/show-sql-telemetry-engine.sh scripts/test-sql-telemetry.sh scripts/verify-sql-telemetry.sh
git commit -m 'feat: add SQL telemetry exporters'
