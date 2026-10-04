#!/usr/bin/env bash
set -euo pipefail

git diff --check
git add -- \
  BENCHMARK.md \
  Makefile \
  PRODUCT_IDEA_GAPS.md \
  README.md \
  TU027_PEER_CONFIG_WATCH.md \
  hat/hatPeer/peer_config_watch.go \
  hat/hatPeer/tu27_peer_config_watch_benchmark_test.go \
  hat/hatPeer/tu27_peer_config_watch_test.go \
  scripts/benchmark-tu27.sh \
  scripts/commit-tu27.sh \
  scripts/format-tu27.sh \
  scripts/push-tu27.sh \
  scripts/stage-tu27.sh \
  scripts/test-tu27-package.sh \
  scripts/test-tu27.sh \
  scripts/verify-tu27.sh
git status --short
