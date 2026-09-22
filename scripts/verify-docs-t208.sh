#!/usr/bin/env bash
set -euo pipefail

for file in \
  README.md \
  INSPIRATION_ROUND2.md \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  T208_ANONYMOUS_REPLICAS.md; do
  test -s "$file"
done
rg -q 'T208_ANONYMOUS_REPLICAS.md' README.md
rg -q 'T208 Anonymous replicas that do not participate in quorum decisions' INSPIRATION_ROUND2.md
rg -q 'Anonymous replicas excluded from quorum decisions' ADOPTED_QUERY_ENGINE_IDEAS.md
rg -q 't208-anonymous-replicas-and-voter-only-quorum' BENCHMARK.md
rg -q 'ExecuteVoterWriteQuorum' T208_ANONYMOUS_REPLICAS.md
