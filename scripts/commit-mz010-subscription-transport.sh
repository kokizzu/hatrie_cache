#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_root"

expected=(
  Makefile
  ADOPTED_QUERY_ENGINE_IDEAS.md
  BENCHMARK.md
  CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md
  ENGINE_IDEAS.md
  MZ010_CROSS_PROCESS_TRANSPORT.md
  hat/hatSql/mz010_subscription_transport.go
  hat/hatSql/mz010_subscription_transport_test.go
  hat/hatSql/mz010_subscription_transport_benchmark_test.go
  scripts/test-mz010-subscription-transport.sh
  scripts/benchmark-mz010-subscription-transport.sh
  scripts/format-mz010-subscription-transport.sh
  scripts/race-mz010-subscription-transport.sh
  scripts/vet-mz010-subscription-transport.sh
  scripts/stage-mz010-subscription-transport.sh
  scripts/commit-mz010-subscription-transport.sh
  scripts/push-mz010-subscription-transport.sh
)

git diff --cached --check
mapfile -t staged < <(git diff --cached --name-only)
if ((${#staged[@]} != ${#expected[@]})); then
  printf 'Unexpected staged path count: got %d, want %d\n' "${#staged[@]}" "${#expected[@]}" >&2
  printf 'Staged paths:\n%s\n' "$(git diff --cached --name-only)" >&2
  exit 1
fi
for path in "${expected[@]}"; do
  found=0
  for staged_path in "${staged[@]}"; do
    if [[ "$path" == "$staged_path" ]]; then
      found=1
      break
    fi
  done
  if ((found == 0)); then
    printf 'Missing expected staged path: %s\n' "$path" >&2
    exit 1
  fi
done

git commit -m 'feat: add framed subscription transport [skip ci]'
