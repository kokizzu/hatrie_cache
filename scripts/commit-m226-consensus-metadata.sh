#!/usr/bin/env bash
set -euo pipefail

expected=(
  Makefile
  INSPIRATION_ROUND2.md
  ADOPTED_QUERY_ENGINE_IDEAS.md
  BENCHMARK.md
  M226_DURABLE_CONSENSUS_METADATA.md
  hat/hatReplication/m226_consensus_metadata.go
  hat/hatReplication/m226_consensus_metadata_test.go
  hat/hatReplication/m226_consensus_metadata_benchmark_test.go
  scripts/m226-consensus-metadata.sh
  scripts/stage-m226-consensus-metadata.sh
  scripts/commit-m226-consensus-metadata.sh
  scripts/push-m226-consensus-metadata.sh
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
git commit -m "feat: add durable consensus metadata"
