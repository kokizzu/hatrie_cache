#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$root"
git diff --check
git add Makefile SQL.md hat/hatSql/query.go hat/hatSql/json_path.go hat/hatSql/json_path_test.go hat/hatCache/sql_query.go hat/hatCache/sql_json_path_test.go scripts/show-sql-json-index-engine.sh scripts/test-sql-json-paths.sh scripts/format-sql-json-paths.sh scripts/verify-sql-json-path-feature.sh scripts/inspect-sql-json-path-feature.sh scripts/commit-sql-json-path-feature.sh
git commit -m "feat: add SQL JSON paths and indexes"
git push
