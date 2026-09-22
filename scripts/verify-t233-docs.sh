#!/usr/bin/env bash
set -euo pipefail

for file in TT033_MVCC_COOPERATIVE_YIELD.md README.md BENCHMARK.md ADOPTED_QUERY_ENGINE_IDEAS.md INSPIRATION_ROUND2.md; do
	test -s "$file"
done
rg -q 'SQLTransaction\.Yield' TT033_MVCC_COOPERATIVE_YIELD.md README.md BENCHMARK.md
rg -q 'T033: MVCC Cooperative Yield' BENCHMARK.md
rg -q '\| T233 \| Tarantool \| MVCC cooperative yields' ADOPTED_QUERY_ENGINE_IDEAS.md
rg -q '\[x\] T233 MVCC transactions' INSPIRATION_ROUND2.md
printf '%s\n' 'T233 documentation references verified.'
