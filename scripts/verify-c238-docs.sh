#!/usr/bin/env bash
set -euo pipefail

rg -q 'C238|MutationSnapshot|EstimatedRemaining' C238_MUTATION_PROGRESS.md
rg -q 'C238.*Mutation queue progress' INSPIRATION_ROUND2.md
rg -q 'ClickHouse C238: Mutation Queue Progress' ADOPTED_QUERY_ENGINE_IDEAS.md
rg -q 'c238-mutation-progress' BENCHMARK.md
printf '%s\n' 'C238 documentation references verified'
