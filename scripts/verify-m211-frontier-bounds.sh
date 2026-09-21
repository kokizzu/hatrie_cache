#!/usr/bin/env bash
set -euo pipefail

test -f M211_SQL_FRONTIER_BOUNDS.md
rg -q 'M211 SQL Frontier Bounds' M211_SQL_FRONTIER_BOUNDS.md
rg -q 'M211: SQL Frontier Bounds' ADOPTED_QUERY_ENGINE_IDEAS.md
rg -q 'M211 SQL Frontier Bounds' BENCHMARK.md
rg -q '\[x\] M211 ' INSPIRATION_ROUND2.md
