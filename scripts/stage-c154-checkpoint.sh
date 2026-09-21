#!/usr/bin/env bash
set -euo pipefail

git add \
	BENCHMARK.md \
	CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md \
	INSPIRATION.md \
	Makefile \
	README.md \
	SCHEMA_ROLLOUT.md \
	hat/hatSchema/rolling_schema.go \
	hat/hatSchema/rolling_schema_checkpoint.go \
	hat/hatSchema/rolling_schema_checkpoint_benchmark_test.go \
	hat/hatSchema/rolling_schema_checkpoint_test.go \
	scripts/benchmark-c154-checkpoint.sh \
	scripts/commit-c154-checkpoint.sh \
	scripts/format-c154-checkpoint.sh \
	scripts/push-c154-checkpoint.sh \
	scripts/race-c154-checkpoint.sh \
	scripts/stage-c154-checkpoint.sh \
	scripts/test-c154-checkpoint.sh \
	scripts/test-c154-schema-package.sh \
	scripts/vet-c154-checkpoint.sh
