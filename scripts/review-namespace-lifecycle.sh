#!/usr/bin/env sh
set -eu

git diff --check -- Makefile hat/hatStorage/namespace_lifecycle.go hat/hatStorage/namespace_lifecycle_test.go scripts/test-namespace-lifecycle.sh scripts/format-namespace-lifecycle.sh scripts/review-namespace-lifecycle.sh scripts/commit-namespace-lifecycle.sh
git diff --name-status -- Makefile hat/hatStorage/namespace_lifecycle.go hat/hatStorage/namespace_lifecycle_test.go scripts/test-namespace-lifecycle.sh scripts/format-namespace-lifecycle.sh scripts/review-namespace-lifecycle.sh scripts/commit-namespace-lifecycle.sh
