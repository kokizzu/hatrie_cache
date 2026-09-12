#!/bin/sh
set -eu

printf '%s\n' 'checking manual'
test -f CH027_COMPACTION_SCHEDULER_OBSERVABILITY.md
printf '%s\n' 'checking README link'
rg -q 'CH027_COMPACTION_SCHEDULER_OBSERVABILITY.md' README.md
printf '%s\n' 'checking ledger'
rg -q 'CH-027' ENGINE_IDEAS.md
printf '%s\n' 'checking benchmark section'
rg -q 'CH-027 Compaction Scheduler Observability' BENCHMARK.md
printf '%s\n' 'checking API name'
rg -q 'scheduler\.Ages\(\)' CH027_COMPACTION_SCHEDULER_OBSERVABILITY.md
