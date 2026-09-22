#!/usr/bin/env bash
set -euo pipefail

for path in \
	T202_REPLICA_SET_LEADER_ELECTION.md \
	BENCHMARK.md \
	README.md \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	INSPIRATION_ROUND2.md; do
	test -s "$path"
done

rg -q 'T202_REPLICA_SET_LEADER_ELECTION.md' README.md
rg -q 'T202 Replica-Set Leader Election' BENCHMARK.md
rg -q 'T202_REPLICA_SET_LEADER_ELECTION.md' ADOPTED_QUERY_ENGINE_IDEAS.md
rg -q '\[x\] T202' INSPIRATION_ROUND2.md
