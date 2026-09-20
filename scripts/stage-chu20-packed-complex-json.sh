#!/usr/bin/env bash
set -euo pipefail

git add \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	BENCHMARK.md \
	CH031_TYPED_JSON_SUBCOLUMNS.md \
	CHU20_PACKED_COMPLEX_JSON.md \
	Makefile \
	PRODUCT_IDEA_GAPS.md \
	hat/hatSql/ch020_complex_json_subcolumn_benchmark_test.go \
	hat/hatSql/ch020_complex_json_subcolumn_test.go \
	hat/hatSql/ch031_automatic_json_subcolumn.go \
	hat/hatSql/columnar_json_subcolumn.go \
	hat/hatSql/contracts.go \
	hat/hatSql/json_path.go \
	scripts/benchmark-chu20-packed-json.sh \
	scripts/commit-chu20-packed-complex-json.sh \
	scripts/push-chu20-packed-complex-json.sh \
	scripts/stage-chu20-packed-complex-json.sh \
	scripts/test-chu20-packed-json.sh
git diff --cached --check
git diff --cached --stat
