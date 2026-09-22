#!/usr/bin/env bash
set -euo pipefail

expected=(
  Makefile
  INSPIRATION_ROUND2.md
  ADOPTED_QUERY_ENGINE_IDEAS.md
  BENCHMARK.md
  M225_PERSISTED_SHARD_LEASES.md
  hat/hatReplication/m225_shard_lease.go
  hat/hatReplication/m225_shard_lease_test.go
  hat/hatReplication/m225_shard_lease_benchmark_test.go
  scripts/m225-shard-leases.sh
  scripts/stage-m225-shard-leases.sh
  scripts/commit-m225-shard-leases.sh
  scripts/push-m225-shard-leases.sh
)

for path in $(git diff --cached --name-only); do
  allowed=0
  for expected_path in "${expected[@]}"; do
    if [[ "$path" == "$expected_path" ]]; then
      allowed=1
      break
    fi
  done
  if [[ "$allowed" != "1" ]]; then
    printf 'refusing to commit unexpected staged path: %s\n' "$path" >&2
    exit 1
  fi
done

git diff --cached --check
git commit -m "feat: add persisted shard leases"
