#!/usr/bin/env bash
set -euo pipefail

git add \
	BENCHMARK.md \
	Makefile \
	PRODUCT_IDEA_GAPS.md \
	TU14_VSHARD_BUCKET_MIGRATION.md \
	hat/hatTopology/tu14_bucket_migration.go \
	hat/hatTopology/tu14_bucket_migration_benchmark_test.go \
	hat/hatTopology/tu14_bucket_migration_contract_test.go \
	hat/hatTopology/tu14_bucket_migration_test.go \
	scripts/benchmark-tu14-vshard.sh \
	scripts/format-tu14-vshard.sh \
	scripts/inspect-inspiration-status.sh \
	scripts/inspect-tu14-implementation.sh \
	scripts/inspect-tu14-vshard.sh \
	scripts/race-tu14-vshard.sh \
	scripts/test-tu14-vshard-package.sh \
	scripts/test-tu14-vshard.sh \
	scripts/vet-tu14-vshard.sh \
	scripts/stage-tu14-vshard.sh \
	scripts/review-tu14-vshard.sh \
	scripts/commit-tu14-vshard.sh \
	scripts/push-tu14-vshard.sh

git diff --cached --check
