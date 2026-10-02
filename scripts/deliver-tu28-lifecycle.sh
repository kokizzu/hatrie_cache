#!/usr/bin/env bash
set -euo pipefail

mode=${1:-status}
case "$mode" in
  status)
    git status --short --untracked-files=all
    ;;
  stage)
    git add Makefile README.md ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md PRODUCT_IDEA_GAPS.md TU28_CONNECTION_POOL_LIFECYCLE.md \
      hat/hatPeer/connection_pool.go hat/hatPeer/peer_lifecycle.go \
      hat/hatPeer/connection_pool_lifecycle_hooks_test.go \
      hat/hatPeer/connection_pool_lifecycle_hooks_benchmark_test.go \
      scripts/test-tu28-lifecycle.sh scripts/test-tu28-package.sh scripts/format-tu28-lifecycle.sh \
      scripts/race-tu28-lifecycle.sh scripts/vet-tu28-lifecycle.sh scripts/verify-tu28-lifecycle.sh \
      scripts/benchmark-tu28-lifecycle.sh scripts/deliver-tu28-lifecycle.sh
    git diff --cached --name-status
    ;;
  commit)
    git commit -m 'feat(peer): add connection pool lifecycle hooks [skip ci]'
    ;;
  push)
    git push -u origin codex/t-u28-connection-lifecycle
    ;;
  *)
    printf 'unknown delivery mode: %s\n' "$mode" >&2
    exit 2
    ;;
esac
