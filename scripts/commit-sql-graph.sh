#!/usr/bin/env sh
set -eu

make review-sql-graph
git add -- Makefile hat/hatSql/graph.go hat/hatSql/graph_test.go scripts/test-sql-graph.sh scripts/format-sql-graph.sh scripts/review-sql-graph.sh scripts/commit-sql-graph.sh
git diff --cached --check
git commit --only -m 'feat: add SQL graph traversal' -- Makefile hat/hatSql/graph.go hat/hatSql/graph_test.go scripts/test-sql-graph.sh scripts/format-sql-graph.sh scripts/review-sql-graph.sh scripts/commit-sql-graph.sh
git push
