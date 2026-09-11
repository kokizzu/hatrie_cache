#!/usr/bin/env bash
set -euo pipefail

test -f SQL_JSON_INDEX_READINESS.md
rg -q 'MZ-022 SQL JSON Index Readiness' BENCHMARK.md
rg -q 'WaitSQLJSONIndexReady' SQL_JSON_INDEX_READINESS.md
rg -q 'Index hydration readiness' ENGINE_IDEAS.md
rg -q 'Index hydration readiness' ADOPTED_QUERY_ENGINE_IDEAS.md
rg -q 'SQL_JSON_INDEX_READINESS.md' README.md
