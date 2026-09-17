#!/usr/bin/env bash
set -euo pipefail

git add -- Makefile README.md PRODUCT_IDEA_GAPS.md BENCHMARK.md MU013_SQL_CARDINALITY_ESTIMATES.md hat/hatSql/query.go hat/hatSql/mu013_cardinality_estimates.go hat/hatSql/mu013_cardinality_estimates_test.go scripts/benchmark-mu013-cardinality-estimates.sh scripts/commit-mu013-cardinality-estimates.sh scripts/format-mu013-cardinality-estimates.sh scripts/push-mu013-cardinality-estimates.sh scripts/race-mu013-cardinality-estimates.sh scripts/review-mu013-cardinality-estimates.sh scripts/stage-mu013-cardinality-estimates.sh scripts/test-mu013-all.sh scripts/test-mu013-cardinality-estimates.sh scripts/test-mu013-cardinality-package.sh scripts/vet-mu013-cardinality-estimates.sh
