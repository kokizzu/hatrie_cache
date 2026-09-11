#!/usr/bin/env bash
set -euo pipefail

test -f SQL_NAMESPACE_COMPUTE_POOLS.md
rg -q 'MZ-019 Named SQL Compute Pools' BENCHMARK.md
rg -q 'ComputeWorkers: 0' SQL_NAMESPACE_COMPUTE_POOLS.md
rg -q 'Per-cluster resource isolation' ENGINE_IDEAS.md
rg -q 'Per-cluster resource isolation' ADOPTED_QUERY_ENGINE_IDEAS.md
rg -q 'SQL_NAMESPACE_COMPUTE_POOLS.md' README.md
