#!/usr/bin/env bash
set -euo pipefail

if [[ -n "$(git diff --cached --name-only)" ]]; then
  printf '%s\n' 'refusing to stage M225: index already contains changes' >&2
  git diff --cached --name-only >&2
  exit 1
fi

paths=(
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

git add -- "${paths[@]}"
git diff --cached --check
git diff --cached --name-only
