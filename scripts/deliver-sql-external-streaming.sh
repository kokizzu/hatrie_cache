#!/usr/bin/env sh
set -eu

git diff --check -- Makefile README.md SQL.md hat/hatSql/external.go hat/hatSql/external_test.go scripts/bench-sql-external-streaming.sh scripts/deliver-sql-external-streaming.sh scripts/format-sql-external.sh
git add -- Makefile README.md SQL.md hat/hatSql/external.go hat/hatSql/external_test.go scripts/bench-sql-external-streaming.sh scripts/deliver-sql-external-streaming.sh scripts/format-sql-external.sh
git commit -m "feat(sql): stream Arrow and Parquet exports"
git push
