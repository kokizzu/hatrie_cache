#!/usr/bin/env bash
set -euo pipefail

git add \
	BENCHMARK.md \
	C236_DATA_SKIPPING_EXPLAIN.md \
	INSPIRATION_ROUND2.md \
	Makefile \
	scripts/commit-c236-index-explain.sh \
	scripts/push-c236-index-explain.sh

git diff --cached --check
git commit -m "docs: record data-skipping explain coverage [skip ci]"
