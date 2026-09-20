#!/usr/bin/env bash
set -euo pipefail

	git add -- \
		BENCHMARK.md \
		Makefile \
		PRODUCT_IDEA_GAPS.md \
	README.md \
	TU33_FUNCTION_GRANTS.md \
	hat/hatAuth/rbac.go \
	hat/hatAuth/role_catalog.go \
	hat/hatAuth/tu33_function_grants_test.go \
	hat/hatAuth/tu33_function_grants_benchmark_test.go \
	scripts/format-tu33-function-grants.sh \
	scripts/test-tu33-function-grants.sh \
	scripts/benchmark-tu33-baseline.sh \
	scripts/benchmark-tu33-function-grants.sh \
	scripts/test-tu33-package.sh \
	scripts/race-tu33-function-grants.sh \
	scripts/vet-tu33-function-grants.sh \
	scripts/review-tu33-function-grants.sh \
	scripts/stage-tu33-function-grants.sh \
	scripts/commit-tu33-function-grants.sh \
	scripts/push-tu33-function-grants.sh
