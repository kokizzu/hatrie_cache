#!/usr/bin/env bash
set -euo pipefail

git add -- \
	BENCHMARK.md \
	MU023_CLUSTER_QUERY_ADMISSION.md \
	PRODUCT_IDEA_GAPS.md \
	README.md \
	Makefile \
	hat/hatSql/mu023_cluster_admission.go \
	hat/hatSql/mu023_cluster_admission_baseline_benchmark_test.go \
	hat/hatSql/mu023_cluster_admission_benchmark_test.go \
	hat/hatSql/mu023_cluster_admission_test.go \
	scripts/benchmark-mu023-cluster-admission-baseline.sh \
	scripts/benchmark-mu023-cluster-admission.sh \
	scripts/commit-mu023-cluster-admission.sh \
	scripts/format-mu023-cluster-admission.sh \
	scripts/push-mu023-cluster-admission.sh \
	scripts/race-mu023-cluster-admission.sh \
	scripts/review-mu023-cluster-admission.sh \
	scripts/stage-mu023-cluster-admission.sh \
	scripts/test-mu023-cluster-admission.sh \
	scripts/test-mu023-package.sh \
	scripts/verify-mu023-cluster-admission.sh \
	scripts/vet-mu023-cluster-admission.sh
git diff --cached --check
git diff --cached --stat
git status --short
