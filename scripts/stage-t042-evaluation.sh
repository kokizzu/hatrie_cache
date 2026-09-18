#!/usr/bin/env bash
set -euo pipefail

git add \
	BENCHMARK.md \
	Makefile \
	T042_RECOVERY_PARALLEL_REPLAY_EVALUATION.md \
	scripts/commit-t042-evaluation.sh \
	scripts/push-t042-evaluation.sh \
	scripts/stage-t042-evaluation.sh \
	scripts/verify-t042-rejection.sh \
	scripts/commit-t042-rejection.sh
git diff --cached --check
git diff --cached --stat
