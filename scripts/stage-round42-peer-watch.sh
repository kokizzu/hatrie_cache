#!/usr/bin/env bash
set -euo pipefail

git add \
  Makefile \
  PRODUCT_IDEA_GAPS.md \
  README.md \
  TU27_PEER_CONFIG_WATCH.md \
  hat/hatTopology/config_watch.go \
  hat/hatPeer/compact_config_watch.go \
  hat/hatPeer/compact_config_watch_benchmark_test.go \
  hat/hatPeer/compact_config_watch_test.go \
  scripts/benchmark-round42-peer-watch.sh \
  scripts/check-round42-peer-watch.sh \
  scripts/format-round42-peer-watch.sh \
  scripts/race-round42-peer-watch.sh \
  scripts/stage-round42-peer-watch.sh \
  scripts/test-round42-peer-watch.sh \
  scripts/vet-round42-peer-watch.sh \
  scripts/commit-round42-peer-watch.sh \
  scripts/push-round42-peer-watch.sh

git diff --cached --check
git diff --cached --stat
