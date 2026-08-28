#!/usr/bin/env sh
set -eu

git diff --check -- Makefile hat/hatSql/virtual_source.go hat/hatSql/virtual_source_test.go scripts/test-virtual-sources.sh scripts/format-virtual-sources.sh scripts/review-virtual-sources.sh scripts/commit-virtual-sources.sh
git add Makefile hat/hatSql/virtual_source.go hat/hatSql/virtual_source_test.go scripts/test-virtual-sources.sh scripts/format-virtual-sources.sh scripts/review-virtual-sources.sh scripts/commit-virtual-sources.sh
git diff --cached --check
git commit -m 'feat: add read-only SQL virtual sources'
git push
