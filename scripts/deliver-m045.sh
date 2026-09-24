#!/usr/bin/env bash
set -euo pipefail

mode=${1:-}
shift || true

files=(
  ADOPTED_QUERY_ENGINE_IDEAS.md
  BENCHMARK.md
  MU032_UDF_CAPABILITIES.md
  Makefile
  PRODUCT_IDEA_GAPS.md
  README.md
  hat/hatSql/function.go
  hat/hatSql/query.go
  hat/hatSql/registry.go
  hat/hatSql/m045_udf_constant_fold_test.go
  scripts/benchmark-m045-udf-fastpath.sh
  scripts/deliver-m045.sh
  scripts/race-m045-udf-fastpath.sh
  scripts/test-m045-udf-fastpath.sh
  scripts/verify-m045-udf-fastpath.sh
  scripts/vet-m045-udf-fastpath.sh
)

case "$mode" in
stage)
	git diff --check
	git add "${files[@]}"
	git diff --cached --check
	git diff --cached --stat
	;;
commit)
	git commit -m 'feat(sql): fold pure literal UDF batches'
	;;
push)
	git push origin HEAD:master
	;;
*)
	printf 'usage: %s {stage|commit|push}\n' "$0" >&2
	exit 2
	;;
esac
