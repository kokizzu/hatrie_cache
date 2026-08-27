#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$root"
git status --short
git diff --check
git diff -- Makefile SQL.md hat/hatCache/main.go hat/hatCache/sql_query.go hat/hatCache/sql_bitmap_index_test.go scripts/show-sql-bitmap-index-engine.sh scripts/test-sql-bitmap-indexes.sh scripts/format-sql-bitmap-indexes.sh scripts/verify-sql-bitmap-index-feature.sh scripts/inspect-sql-bitmap-index-feature.sh
