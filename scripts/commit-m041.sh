#!/usr/bin/env bash
set -euo pipefail

git add \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	BENCHMARK.md \
	M041_ARRANGEMENT_RECOVERY_BUNDLE.md \
	Makefile \
	PRODUCT_IDEA_GAPS.md \
	README.md \
	hat/hatSql/m_u05_arrangement_recovery.go \
	hat/hatSql/m_u05_arrangement_recovery_bundle.go \
	hat/hatSql/m_u05_arrangement_recovery_bundle_benchmark_test.go \
	hat/hatSql/m_u05_arrangement_recovery_bundle_test.go \
	scripts/benchmark-m041-mu05.sh \
	scripts/commit-m041.sh \
	scripts/format-m041.sh \
	scripts/push-m041.sh \
	scripts/race-m041-mu05.sh \
	scripts/status-m041.sh \
	scripts/test-m041-all.sh \
	scripts/test-m041-mu05.sh \
	scripts/test-m041-package.sh \
	scripts/vet-m041-mu05.sh
git diff --cached --check
git commit -m "feat(sql): bundle arrangement recovery"
