#!/usr/bin/env bash
set -euo pipefail

test -f T210_MASTER_MASTER_CONFLICT_HOOKS.md
rg -q 'T209 Relay/applier backpressure' INSPIRATION_ROUND2.md
rg -q 'T210 Master-master conflict hooks' INSPIRATION_ROUND2.md
rg -q 'Master-master conflict hooks with source and sequence context' ADOPTED_QUERY_ENGINE_IDEAS.md
rg -q '^## T210 Master-Master Conflict Hooks$' BENCHMARK.md
rg -q 'T210_MASTER_MASTER_CONFLICT_HOOKS.md' README.md
rg -q 'make benchmark-t210-baseline' BENCHMARK.md
rg -q 'make benchmark-t210' BENCHMARK.md
printf '%s\n' 'T210 documentation verified'
