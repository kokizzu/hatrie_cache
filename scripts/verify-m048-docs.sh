#!/usr/bin/env bash
set -euo pipefail

rg -q '^# M052 Dataflow Plan Codec$' M052_DATAFLOW_PLAN_CODEC.md
rg -q 'EncodeSQLDataflowPlanJSON' M052_DATAFLOW_PLAN_CODEC.md
rg -q '^## M052ab Compact SQL Dataflow Plan Codec$' BENCHMARK.md
rg -q '3\.79x faster' BENCHMARK.md
rg -q '^### M052ab: Compact reusable dataflow-plan transfer$' CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md
rg -q '^## M052ab: Compact Dataflow Plan Transfer$' ADOPTED_QUERY_ENGINE_IDEAS.md
printf '%s\n' 'M048 documentation verified'
