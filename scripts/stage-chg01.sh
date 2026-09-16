#!/bin/sh
set -eu
git add \
	Makefile \
	README.md \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	INSPIRATION.md \
	BENCHMARK.md \
	CHG01_EXTERNAL_GROUP_SPILL.md \
	hat/hatSql/query.go \
	hat/hatSql/chg01_external_group_spill_test.go \
	hat/hatSql/chg01_external_group_spill_benchmark_test.go \
	scripts/format-chg01.sh \
	scripts/check-chg01.sh \
	scripts/stage-chg01.sh \
	scripts/commit-chg01.sh \
	scripts/push-chg01.sh \
	scripts/test-chg01.sh \
	scripts/test-chg01-package.sh \
	scripts/test-chg01-all.sh \
	scripts/benchmark-chg01.sh \
	scripts/prepare-chg01-benchmark-baseline.sh \
	scripts/benchmark-chg01-before.sh \
	scripts/benchmark-chg01-after.sh \
	scripts/print-chg01-benchmark.sh \
	scripts/race-chg01.sh \
	scripts/vet-chg01.sh \
	scripts/verify-chg01-docs.sh
