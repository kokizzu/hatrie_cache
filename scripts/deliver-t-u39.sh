#!/usr/bin/env bash
set -euo pipefail

git add \
  BENCHMARK.md \
  PRODUCT_IDEA_GAPS.md \
  README.md \
  TU039_SPACE_CHANGEFEED.md \
  Makefile \
  hat/hatReplication/tu39_space_changefeed.go \
  hat/hatReplication/t_u39_space_changefeed_test.go \
  hat/hatReplication/t_u39_space_changefeed_benchmark_test.go \
  scripts/test-t-u39.sh \
  scripts/deliver-t-u39.sh
git commit -m 'feat(replication): add bounded named-space changefeed [skip ci]'
git push origin HEAD
