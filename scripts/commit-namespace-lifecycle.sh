#!/usr/bin/env sh
set -eu

git diff --check -- Makefile hat/hatStorage/namespace_lifecycle.go hat/hatStorage/namespace_lifecycle_test.go scripts/test-namespace-lifecycle.sh scripts/format-namespace-lifecycle.sh scripts/review-namespace-lifecycle.sh scripts/commit-namespace-lifecycle.sh
git add Makefile hat/hatStorage/namespace_lifecycle.go hat/hatStorage/namespace_lifecycle_test.go scripts/test-namespace-lifecycle.sh scripts/format-namespace-lifecycle.sh scripts/review-namespace-lifecycle.sh scripts/commit-namespace-lifecycle.sh
git diff --cached --check
git commit -m 'feat: add namespace lifecycle controls'
git push
