#!/usr/bin/env bash
set -euo pipefail

base_makefile=$(mktemp)
trap 'rm -f "$base_makefile"' EXIT
git show HEAD:Makefile > "$base_makefile"
{
printf '\n'
printf '%s\n' \
  'test-connection-pool-c212:' \
  $'\t@bash ./scripts/test-connection-pool-c212.sh' \
  '' \
  'benchmark-connection-pool-c212:' \
  $'\t@bash ./scripts/benchmark-connection-pool-c212.sh' \
  '' \
  'format-connection-pool-c212:' \
  $'\t@bash ./scripts/format-connection-pool-c212.sh' \
  '' \
  'verify-connection-pool-c212:' \
  $'\t@bash ./scripts/verify-connection-pool-c212.sh' \
  '' \
  'inspect-connection-pool-diff-c212:' \
  $'\t@bash ./scripts/inspect-connection-pool-diff-c212.sh' \
  '' \
  'stage-connection-pool-c212:' \
  $'\t@bash ./scripts/stage-connection-pool-c212.sh' \
  '' \
  'commit-connection-pool-c212:' \
  $'\t@bash ./scripts/commit-connection-pool-c212.sh' \
  '' \
  'push-connection-pool-c212:' \
  $'\t@bash ./scripts/push-connection-pool-c212.sh' >> "$base_makefile"
}

git add CONNECTION_POOL.md CONNECTION_POOL_IDLE_FASTPATH.md BENCHMARK.md INSPIRATION.md hat/hatReplication/connection_pool.go hat/hatReplication/connection_pool_idle_fastpath_test.go scripts/test-connection-pool-c212.sh scripts/benchmark-connection-pool-c212.sh scripts/format-connection-pool-c212.sh scripts/verify-connection-pool-c212.sh scripts/inspect-connection-pool-diff-c212.sh scripts/stage-connection-pool-c212.sh scripts/commit-connection-pool-c212.sh scripts/push-connection-pool-c212.sh
makefile_blob=$(git hash-object -w "$base_makefile")
git update-index --add --cacheinfo "100644,$makefile_blob,Makefile"
git diff --cached --check
git diff --cached --name-only
