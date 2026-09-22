#!/usr/bin/env bash
set -euo pipefail

for path in \
	T203_LEADER_WRITE_FENCE.md \
	BENCHMARK.md \
	README.md \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	INSPIRATION_ROUND2.md; do
	test -s "$path"
done

rg -q 'T203_LEADER_WRITE_FENCE.md' README.md
rg -q 'T203 Strict Leader Write Fencing' BENCHMARK.md
rg -q 'T203_LEADER_WRITE_FENCE.md' ADOPTED_QUERY_ENGINE_IDEAS.md
rg -q '\[x\] T203' INSPIRATION_ROUND2.md
