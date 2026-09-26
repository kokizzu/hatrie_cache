#!/usr/bin/env bash
set -euo pipefail

if git diff --cached --quiet -- \
	Makefile \
	ENGINE_IDEAS.md \
	CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md \
	BENCHMARK.md \
	CH048_BETWEEN_PREDICATE.md \
	hat/hatSql/columnar_dictionary_predicate.go \
	hat/hatSql/query.go \
	hat/hatSql/ch048_between_predicate_test.go \
	scripts/benchmark-ch048-between.sh \
	scripts/test-ch048-between.sh \
	scripts/test-ch048-between-package.sh \
	scripts/format-ch048-between.sh \
	scripts/race-ch048-between.sh \
	scripts/vet-ch048-between.sh \
	scripts/verify-ch048-between-docs.sh \
	scripts/review-ch048-between.sh \
	scripts/stage-ch048-between.sh \
	scripts/commit-ch048-between.sh \
	scripts/push-ch048-between.sh; then
	printf '%s\n' 'no staged CH-048 BETWEEN changes' >&2
	exit 1
fi

git commit -m 'feat(sql): accelerate literal between predicates'
