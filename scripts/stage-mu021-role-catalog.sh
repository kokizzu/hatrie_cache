#!/usr/bin/env bash
set -euo pipefail

git add -- \
	BENCHMARK.md \
	Makefile \
	MU021_ROLE_NAMESPACE_CATALOG.md \
	PRODUCT_IDEA_GAPS.md \
	README.md \
	hat/hatAuth/mu021_role_catalog_baseline_benchmark_test.go \
	hat/hatAuth/mu021_role_catalog_benchmark_test.go \
	hat/hatAuth/mu021_role_catalog_test.go \
	hat/hatAuth/role_catalog.go \
	scripts/benchmark-mu021-role-catalog-baseline.sh \
	scripts/benchmark-mu021-role-catalog.sh \
	scripts/commit-mu021-role-catalog.sh \
	scripts/format-mu021-role-catalog.sh \
	scripts/push-mu021-role-catalog.sh \
	scripts/race-mu021-role-catalog.sh \
	scripts/review-mu021-role-catalog.sh \
	scripts/stage-mu021-role-catalog.sh \
	scripts/test-mu021-package.sh \
	scripts/test-mu021-role-catalog.sh \
	scripts/verify-mu021-role-catalog.sh \
	scripts/vet-mu021-role-catalog.sh

git diff --cached --check
git diff --cached --stat
git status --short
