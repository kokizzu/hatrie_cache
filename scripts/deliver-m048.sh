#!/usr/bin/env bash
set -euo pipefail

git diff --check
git add \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	BENCHMARK.md \
	CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md \
	M052_DATAFLOW_PLAN_CODEC.md \
	Makefile \
	hat/hatSql/m048_dataflow_plan_codec.go \
	hat/hatSql/m048_dataflow_plan_codec_benchmark_test.go \
	hat/hatSql/m048_dataflow_plan_codec_test.go \
	scripts/benchmark-m048-baseline.sh \
	scripts/benchmark-m048-codec.sh \
	scripts/diffcheck-m048.sh \
	scripts/deliver-m048.sh \
	scripts/format-m048.sh \
	scripts/race-m048.sh \
	scripts/test-m048-codec.sh \
	scripts/test-m048-package.sh \
	scripts/verify-m048-docs.sh \
	scripts/vet-m048.sh
git diff --cached --check
git commit -m "feat(sql): add compact dataflow plan codec"
git push origin HEAD:master
