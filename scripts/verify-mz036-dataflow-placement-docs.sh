#!/usr/bin/env bash
set -euo pipefail

test -f MZ036_DATAFLOW_OPERATOR_PLACEMENT.md
rg -q 'MZ-036 Dataflow Operator Placement' MZ036_DATAFLOW_OPERATOR_PLACEMENT.md
rg -q 'MZ036_DATAFLOW_OPERATOR_PLACEMENT.md' README.md
rg -q 'MZ-36.*\[x\]' INSPIRATION_BACKLOG.md
rg -q 'mz-036-dataflow-operator-placement' BENCHMARK.md
rg -q 'PlanDataflowOperatorPlacement' ADOPTED_QUERY_ENGINE_IDEAS.md
