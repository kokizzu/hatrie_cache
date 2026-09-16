#!/bin/sh
set -eu
test -s C227_GROUP_MERGE_BUDGET.md
rg -n 'MaxGroupMergeBytes|C227_GROUP_MERGE_BUDGET.md|c227-external-group-merge-memory-budget' README.md INSPIRATION_ROUND2.md ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md hat/hatSql/query.go hat/hatSql/sql_result_cache.go
