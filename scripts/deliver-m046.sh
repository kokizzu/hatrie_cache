#!/usr/bin/env bash
set -euo pipefail

git diff --check
printf '%s\n' 'M046 changes before staging:'
git status --short

git add \
	Makefile \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	BENCHMARK.md \
	CH031_TYPED_JSON_SUBCOLUMNS.md \
	M046_JSON_SUBCOLUMN_TOPN.md \
	PRODUCT_IDEA_GAPS.md \
	hat/hatSql/ch031_typed_json_subcolumn_test.go \
	hat/hatSql/columnar_json_subcolumn_scan.go \
	hat/hatSql/m046_json_subcolumn_topn_benchmark_test.go \
	scripts/benchmark-m046-json-topn.sh \
	scripts/deliver-m046.sh \
	scripts/race-m046-json-topn.sh \
	scripts/test-m046-json-regression.sh \
	scripts/test-m046-json-topn.sh \
	scripts/test-m046-package.sh \
	scripts/verify-m046-docs.sh \
	scripts/vet-m046-json-topn.sh

git diff --cached --check
git commit -m 'feat(sql): order typed JSON subcolumns with bounded top-n'
git push origin codex/m046:master
git status --short
