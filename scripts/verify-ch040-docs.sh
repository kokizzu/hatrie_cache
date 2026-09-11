#!/usr/bin/env bash
set -euo pipefail

rg -n -F -- 'SQL_ARG_EXTREME.md' README.md
rg -n -F -- 'ARGMAX(payload, ordering_value)' SQL_ARG_EXTREME.md
rg -n -F -- '1,028,248 ns' SQL_ARG_EXTREME.md
rg -n -F -- '| CH-040 | `argMax`/`argMin` aggregates | Implemented' ENGINE_IDEAS.md
rg -n -F -- '| ClickHouse | `argMax`/`argMin` aggregates | Adopted automatically |' ADOPTED_QUERY_ENGINE_IDEAS.md
rg -n -F -- '## SQL ARGMAX and ARGMIN' BENCHMARK.md
rg -n -F -- '1135 1032845 ns/op 5816 B/op 25 allocs/op' BENCHMARK.md
rg -n -F -- '2.21x' BENCHMARK.md
