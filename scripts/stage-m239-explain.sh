#!/usr/bin/env bash
set -euo pipefail

git add -- \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	BENCHMARK.md \
	INSPIRATION_ROUND2.md \
	M239_EXPLAIN_FRONTIER.md \
	Makefile \
	hat/hatSql/query.go \
	hat/hatSql/m239_explain_frontier.go \
	hat/hatSql/m239_explain_frontier_test.go \
	scripts/benchmark-m239-explain.sh \
	scripts/commit-m239-explain.sh \
	scripts/push-m239-explain.sh \
	scripts/stage-m239-explain.sh \
	scripts/test-m239-explain.sh
git diff --cached --check
git diff --cached --stat
