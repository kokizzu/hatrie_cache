#!/usr/bin/env bash
set -euo pipefail

git add -- \
	BENCHMARK.md \
	Makefile \
	MU020_SECRET_CONNECTION_RESOURCES.md \
	PRODUCT_IDEA_GAPS.md \
	README.md \
	hat/hatAuth/mu020_secret_resource_baseline_benchmark_test.go \
	hat/hatAuth/mu020_secret_resources_test.go \
	hat/hatAuth/resource_registry.go \
	scripts/benchmark-mu020-secret-resources.sh \
	scripts/commit-mu020-secret-resources.sh \
	scripts/format-mu020-secret-resources.sh \
	scripts/push-mu020-secret-resources.sh \
	scripts/race-mu020-secret-resources.sh \
	scripts/review-mu020-secret-resources.sh \
	scripts/stage-mu020-secret-resources.sh \
	scripts/test-mu020-package.sh \
	scripts/test-mu020-secret-resources.sh \
	scripts/vet-mu020-secret-resources.sh

git diff --cached --check
git diff --cached --stat
git status --short
