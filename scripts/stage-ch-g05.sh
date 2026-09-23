#!/usr/bin/env bash
set -euo pipefail

git add -- \
	BENCHMARK.md \
	CH005_RUNTIME_JOIN_PARTITION_FILTER.md \
	IDEA_GAP_CATALOG.md \
	Makefile \
	README.md \
	hat/hatSql/contracts.go \
	hat/hatSql/query.go \
	hat/hatSql/ch005_runtime_join_partition_filter.go \
	hat/hatSql/ch005_runtime_join_partition_filter_benchmark_test.go \
	scripts/m256-ch-g05-test.sh \
	scripts/m257-ch-g05-format.sh \
	scripts/m258-ch-g05-benchmark.sh \
	scripts/m259-ch-g05-race.sh \
	scripts/m260-ch-g05-docs.sh \
	scripts/stage-ch-g05.sh \
	scripts/commit-ch-g05.sh \
	scripts/push-ch-g05.sh
