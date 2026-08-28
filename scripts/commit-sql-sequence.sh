#!/usr/bin/env sh
set -eu

make review-sql-sequence
git add -- Makefile hat/hatSql/sequence.go hat/hatSql/sequence_test.go scripts/test-sql-sequence.sh scripts/format-sql-sequence.sh scripts/review-sql-sequence.sh scripts/commit-sql-sequence.sh
git diff --cached --check
git commit --only -m 'feat: add SQL sequence matching' -- Makefile hat/hatSql/sequence.go hat/hatSql/sequence_test.go scripts/test-sql-sequence.sh scripts/format-sql-sequence.sh scripts/review-sql-sequence.sh scripts/commit-sql-sequence.sh
git push
