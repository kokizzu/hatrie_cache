#!/usr/bin/env bash
set -euo pipefail

git diff --check
git status --short --branch
git diff --stat -- Makefile README.md PRODUCT_IDEA_GAPS.md BENCHMARK.md MU013_SQL_CARDINALITY_ESTIMATES.md hat/hatSql/query.go hat/hatSql/mu013_cardinality_estimates.go hat/hatSql/mu013_cardinality_estimates_test.go scripts/format-mu013-cardinality-estimates.sh scripts/test-mu013-cardinality-estimates.sh scripts/test-mu013-cardinality-package.sh scripts/test-mu013-all.sh scripts/benchmark-mu013-cardinality-estimates.sh scripts/race-mu013-cardinality-estimates.sh scripts/vet-mu013-cardinality-estimates.sh scripts/review-mu013-cardinality-estimates.sh
git diff -- Makefile hat/hatSql/query.go
git diff --cached --check
git diff --cached --stat
git diff --cached -- Makefile hat/hatSql/query.go
