#!/usr/bin/env sh
set -eu

git diff --check -- Makefile hat/hatSql/import_diff.go hat/hatSql/import_diff_test.go scripts/test-import-diff.sh scripts/format-import-diff.sh scripts/review-import-diff.sh scripts/commit-import-diff.sh
git add Makefile hat/hatSql/import_diff.go hat/hatSql/import_diff_test.go scripts/test-import-diff.sh scripts/format-import-diff.sh scripts/review-import-diff.sh scripts/commit-import-diff.sh
git diff --cached --check
git commit -m 'feat: add import deduplication and table diffs'
git push
