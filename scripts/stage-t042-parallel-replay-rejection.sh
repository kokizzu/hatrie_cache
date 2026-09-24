#!/usr/bin/env bash
set -euo pipefail

git add \
  Makefile \
  BENCHMARK.md \
	scripts/stage-t042-parallel-replay-rejection.sh \
	scripts/commit-t042-parallel-replay-rejection.sh \
	scripts/push-t042-parallel-replay-rejection.sh
