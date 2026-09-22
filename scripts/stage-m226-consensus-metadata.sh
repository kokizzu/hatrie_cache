#!/usr/bin/env bash
set -euo pipefail

if [[ -n "$(git diff --cached --name-only)" ]]; then
  printf '%s\n' 'refusing to stage M226: index already contains changes' >&2
  git diff --cached --name-only >&2
  exit 1
fi

paths=(
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

git add -- "${paths[@]}"
git diff --cached --check
git diff --cached --name-only
