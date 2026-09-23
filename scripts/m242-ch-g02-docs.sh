#!/usr/bin/env bash
set -euo pipefail
test -s CH002_GRACE_HASH_JOIN.md
rg -n 'CH002_GRACE_HASH_JOIN.md|CH-G02: Bounded Grace-Hash Join|MaxJoinBytes|BenchmarkCH002GraceHashJoin' README.md BENCHMARK.md CH002_GRACE_HASH_JOIN.md IDEA_GAP_CATALOG.md
