#!/usr/bin/env bash
set -euo pipefail

git add \
	Makefile \
	BENCHMARK.md \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	INSPIRATION_ROUND2.md \
	scripts/verify-t235-docs.sh \
	scripts/stage-t235.sh \
	scripts/commit-t235.sh \
	scripts/push-t235.sh

git diff --cached --check
git diff --cached --stat
