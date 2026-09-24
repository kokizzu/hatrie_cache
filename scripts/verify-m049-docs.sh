#!/usr/bin/env bash
set -euo pipefail

test -f M049_COMPILED_PLAN_SINGLEFLIGHT.md
rg -n 'M049: Concurrent Compiled Plan Miss Coalescing' ADOPTED_QUERY_ENGINE_IDEAS.md
rg -n 'M049: Concurrent compiled-plan miss coalescing' CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md
rg -n 'M049 Concurrent Compiled-Plan Miss Coalescing' BENCHMARK.md
rg -n 'M049_COMPILED_PLAN_SINGLEFLIGHT.md' ADOPTED_QUERY_ENGINE_IDEAS.md CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md
rg -n 'Coalesced' hat/hatSql/c213_compiled_plan_cache.go hat/hatSql/m049_compiled_query_cache_singleflight_test.go

printf '%s\n' 'M049 documentation checks passed'
