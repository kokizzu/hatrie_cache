#!/usr/bin/env bash
set -euo pipefail

git add BENCHMARK.md CH003_PARALLEL_HASH_JOIN.md IDEA_GAP_CATALOG.md Makefile README.md \
	hat/hatSql/c003_parallel_hash_join_shared.go hat/hatSql/ch003_parallel_hash_join_benchmark_test.go hat/hatSql/query.go \
	scripts/m244-ch-g03-test.sh scripts/m246-ch-g03-baseline.sh scripts/m247-ch-g03-format.sh \
	scripts/m248-ch-g03-benchmark.sh scripts/m249-ch-g03-race.sh scripts/m250-ch-g03-docs.sh \
	scripts/stage-ch-g03.sh scripts/commit-ch-g03.sh scripts/push-ch-g03.sh
