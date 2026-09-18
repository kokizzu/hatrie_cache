#!/usr/bin/env bash
set -euo pipefail

test -s CH045_GLOBAL_JOIN_BROADCAST_PLANNING.md
rg -n 'CH045_GLOBAL_JOIN_BROADCAST_PLANNING.md|ch-045-global-in-global-join-broadcast-planning|GlobalJoinBroadcastPlanner' README.md INSPIRATION_BACKLOG.md ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md CH045_GLOBAL_JOIN_BROADCAST_PLANNING.md
