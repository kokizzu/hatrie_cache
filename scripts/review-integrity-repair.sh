#!/usr/bin/env sh
set -eu

git diff --check -- Makefile hat/hatCache/integrity.go hat/hatCache/integrity_test.go scripts/test-integrity-repair.sh scripts/format-integrity-repair.sh scripts/review-integrity-repair.sh scripts/commit-integrity-repair.sh
git diff --name-status -- Makefile hat/hatCache/integrity.go hat/hatCache/integrity_test.go scripts/test-integrity-repair.sh scripts/format-integrity-repair.sh scripts/review-integrity-repair.sh scripts/commit-integrity-repair.sh
