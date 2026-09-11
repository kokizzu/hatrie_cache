#!/usr/bin/env bash
set -euo pipefail

test -f SQL_COMPUTE_STORAGE_SEPARATION.md
rg -q 'MZ-018 Optional SQL Compute Pool' BENCHMARK.md
rg -q 'ComputeWorkers: 0' SQL_COMPUTE_STORAGE_SEPARATION.md
rg -q 'MZ-018.*Compute/storage separation' ENGINE_IDEAS.md
rg -q 'Compute/storage separation' ADOPTED_QUERY_ENGINE_IDEAS.md
rg -q 'SQL_COMPUTE_STORAGE_SEPARATION.md' README.md
printf '%s\n' 'MZ-018 documentation verified'
