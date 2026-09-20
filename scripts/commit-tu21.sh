#!/usr/bin/env bash
set -euo pipefail

git add \
	BENCHMARK.md \
	Makefile \
	PRODUCT_IDEA_GAPS.md \
	README.md \
	TU21_VERSIONED_MIGRATION_MANAGER.md \
	hat/hatDataStructure/tu21_versioned_migration_manager_benchmark_test.go \
	hat/hatDataStructure/tu21_versioned_migration_manager_test.go \
	hat/hatDataStructure/versioned_migration_manager.go \
	scripts/benchmark-tu21.sh \
	scripts/commit-tu21.sh \
	scripts/format-tu21.sh \
	scripts/push-tu21.sh \
	scripts/test-tu21.sh \
	scripts/verify-tu21.sh

git commit -m "feat(migrations): add versioned migration manager"
