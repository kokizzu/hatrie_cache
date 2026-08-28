#!/usr/bin/env sh
set -eu

git diff --check -- Makefile hat/hatSql/external_quality.go hat/hatSql/external_quality_test.go scripts/test-external-quality.sh scripts/format-external-quality.sh scripts/review-external-quality.sh scripts/commit-external-quality.sh
git add Makefile hat/hatSql/external_quality.go hat/hatSql/external_quality_test.go scripts/test-external-quality.sh scripts/format-external-quality.sh scripts/review-external-quality.sh scripts/commit-external-quality.sh
git diff --cached --check
git commit -m 'feat: add validated COPY CSV imports'
git push
