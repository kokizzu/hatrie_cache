#!/usr/bin/env bash
set -euo pipefail

git add \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	BENCHMARK.md \
	CH004_FINAL_PUSHDOWN.md \
	CH004_FINAL_READ.md \
	ENGINE_IDEAS.md \
	README.md \
	hat/hatSql/ch004_final.go \
	hat/hatSql/ch004_final_benchmark_test.go \
	hat/hatSql/ch004_final_test.go \
	hat/hatSql/ch004_final_pushdown_benchmark_test.go \
	hat/hatSql/ch004_final_pushdown_test.go \
	hat/hatSql/query.go \
	scripts/benchmark-ch004-final-baseline.sh \
	scripts/benchmark-ch004-final-rows.sh \
	scripts/benchmark-ch004-final.sh \
	scripts/commit-ch004-final.sh \
	scripts/format-ch004-final.sh \
	scripts/push-ch004-final.sh \
	scripts/race-ch004-final.sh \
	scripts/review-ch004-final.sh \
	scripts/stage-ch004-final.sh \
	scripts/test-ch004-all.sh \
	scripts/test-ch004-final.sh \
	scripts/test-ch004-package.sh \
	scripts/vet-ch004-final.sh

git diff --cached --check
