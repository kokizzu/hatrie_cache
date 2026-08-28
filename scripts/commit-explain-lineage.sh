#!/usr/bin/env sh
set -eu

git diff --check -- Makefile hat/hatSql/model.go hat/hatSql/query.go hat/hatSql/explain_lineage_test.go scripts/test-explain-lineage.sh scripts/format-explain-lineage.sh scripts/review-explain-lineage.sh scripts/commit-explain-lineage.sh
git add Makefile hat/hatSql/model.go hat/hatSql/query.go hat/hatSql/explain_lineage_test.go scripts/test-explain-lineage.sh scripts/format-explain-lineage.sh scripts/review-explain-lineage.sh scripts/commit-explain-lineage.sh
git diff --cached --check
git commit -m 'feat: add EXPLAIN projection lineage'
git push
