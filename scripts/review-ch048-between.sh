#!/usr/bin/env bash
set -euo pipefail

git diff --check -- \
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
	scripts/push-ch048-between.sh
git diff --stat -- \
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
	scripts/push-ch048-between.sh
