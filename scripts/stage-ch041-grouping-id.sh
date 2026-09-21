#!/usr/bin/env bash
set -euo pipefail

git add -- \
	BENCHMARK.md \
	CH041_GROUPING_ID.md \
	ENGINE_IDEAS.md \
	Makefile \
	hat/hatSql/ch041_grouping_id_benchmark_test.go \
	hat/hatSql/grouping_identifier_test.go \
	hat/hatSql/grouping_sets.go \
	hat/hatSql/query.go \
	scripts/ch041-grouping-id.sh \
	scripts/commit-ch041-grouping-id.sh \
	scripts/push-ch041-grouping-id.sh \
	scripts/stage-ch041-grouping-id.sh
git diff --cached --check
git diff --cached --name-only
