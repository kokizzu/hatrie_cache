#!/usr/bin/env bash
set -euo pipefail
git diff --check -- \
	Makefile \
	README.md \
	PRODUCT_IDEA_GAPS.md \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	BENCHMARK.md \
	MU028_MONOTONICITY.md \
	hat/hatSql/mu028_monotonicity.go \
	hat/hatSql/mu028_monotonicity_test.go \
	hat/hatSql/mu028_monotonicity_benchmark_test.go \
	scripts/test-mu028-monotonicity.sh \
	scripts/test-mu028-package.sh \
	scripts/format-mu028-monotonicity.sh \
	scripts/race-mu028-monotonicity.sh \
	scripts/vet-mu028-monotonicity.sh \
	scripts/verify-mu028-monotonicity.sh \
	scripts/review-mu028-monotonicity.sh \
	scripts/benchmark-mu028-monotonicity.sh
git status --short -- \
	Makefile \
	README.md \
	PRODUCT_IDEA_GAPS.md \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	BENCHMARK.md \
	MU028_MONOTONICITY.md \
	hat/hatSql/mu028_monotonicity.go \
	hat/hatSql/mu028_monotonicity_test.go \
	hat/hatSql/mu028_monotonicity_benchmark_test.go \
	scripts
