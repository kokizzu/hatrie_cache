#!/usr/bin/env bash
set -euo pipefail

rg -n 'SQL_PLANNER_STATISTICS.md|TT-050|benchmark-tt050-sql-planner-statistics' README.md ENGINE_IDEAS.md ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md SQL_PLANNER_STATISTICS.md
