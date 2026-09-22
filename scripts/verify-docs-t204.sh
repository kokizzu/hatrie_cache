#!/usr/bin/env bash
set -euo pipefail

for path in \
	T204_SUPERVISED_FAILOVER.md \
	BENCHMARK.md \
	README.md \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	INSPIRATION_ROUND2.md; do
	test -s "$path"
done

rg -q 'T204_SUPERVISED_FAILOVER.md' README.md
rg -q 'T204 Supervised Failover Lifecycle' BENCHMARK.md
rg -q 'T204_SUPERVISED_FAILOVER.md' ADOPTED_QUERY_ENGINE_IDEAS.md
rg -q '\[x\] T204' INSPIRATION_ROUND2.md
