#!/usr/bin/env bash
set -euo pipefail
git add -- \
	BENCHMARK.md \
	CH001_ADAPTIVE_JOIN.md \
	IDEA_GAP_CATALOG.md \
	Makefile \
	README.md \
	hat/hatSql/ch001_adaptive_join_benchmark_test.go \
	hat/hatSql/ch001_partial_merge_join.go \
	hat/hatSql/query.go \
	scripts/m240-ch-g01-baseline.sh \
	scripts/m240-ch-g01-benchmark.sh \
	scripts/m240-ch-g01-docs.sh \
	scripts/m240-ch-g01-format.sh \
	scripts/m240-ch-g01-package-test.sh \
	scripts/m240-ch-g01-push.sh \
	scripts/m240-ch-g01-race.sh \
	scripts/m240-ch-g01-stage.sh \
	scripts/m240-ch-g01-test.sh \
	scripts/m240-ch-g01-vet.sh \
	scripts/m240-ch-g01-commit.sh
