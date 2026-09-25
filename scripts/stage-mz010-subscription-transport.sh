#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_root"

makefile_tmp=$(mktemp)
trap 'rm -f "$makefile_tmp"' EXIT

git show HEAD:Makefile > "$makefile_tmp"
printf '%s\n' \
  '.PHONY: test-mz010-subscription-transport benchmark-mz010-subscription-transport format-mz010-subscription-transport race-mz010-subscription-transport vet-mz010-subscription-transport stage-mz010-subscription-transport commit-mz010-subscription-transport push-mz010-subscription-transport' \
  'test-mz010-subscription-transport:' \
  $'\tbash ./scripts/test-mz010-subscription-transport.sh' \
  'benchmark-mz010-subscription-transport:' \
  $'\tbash ./scripts/benchmark-mz010-subscription-transport.sh' \
  'format-mz010-subscription-transport:' \
  $'\tbash ./scripts/format-mz010-subscription-transport.sh' \
  'race-mz010-subscription-transport:' \
  $'\tbash ./scripts/race-mz010-subscription-transport.sh' \
  'vet-mz010-subscription-transport:' \
  $'\tbash ./scripts/vet-mz010-subscription-transport.sh' \
  'stage-mz010-subscription-transport:' \
  $'\tbash ./scripts/stage-mz010-subscription-transport.sh' \
  'commit-mz010-subscription-transport:' \
  $'\tbash ./scripts/commit-mz010-subscription-transport.sh' \
  'push-mz010-subscription-transport:' \
  $'\tbash ./scripts/push-mz010-subscription-transport.sh' >> "$makefile_tmp"

makefile_blob=$(git hash-object -w "$makefile_tmp")
git update-index --add --cacheinfo "100644,$makefile_blob,Makefile"

git add -- \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md \
  ENGINE_IDEAS.md \
  MZ010_CROSS_PROCESS_TRANSPORT.md \
  hat/hatSql/mz010_subscription_transport.go \
  hat/hatSql/mz010_subscription_transport_test.go \
  hat/hatSql/mz010_subscription_transport_benchmark_test.go \
  scripts/test-mz010-subscription-transport.sh \
  scripts/benchmark-mz010-subscription-transport.sh \
  scripts/format-mz010-subscription-transport.sh \
  scripts/race-mz010-subscription-transport.sh \
  scripts/vet-mz010-subscription-transport.sh \
  scripts/stage-mz010-subscription-transport.sh \
  scripts/commit-mz010-subscription-transport.sh \
  scripts/push-mz010-subscription-transport.sh

printf '%s\n' 'Staged MZ-010 cross-process subscription transport paths.'
