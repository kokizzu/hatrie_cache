#!/usr/bin/env bash
set -euo pipefail

test -f COMPACTION_SCHEDULER_STATS.md
rg -q 'COMPACTION_SCHEDULER_STATS.md' README.md
rg -q 'CH-027.*Background-task observability' ENGINE_IDEAS.md
rg -q 'TT-036.*Storage `box.stat` equivalent' ENGINE_IDEAS.md
rg -q 'Background-task and storage statistics' ADOPTED_QUERY_ENGINE_IDEAS.md
rg -q '^## TT-036 CH-027 Compaction Scheduler Statistics$' BENCHMARK.md
rg -q 'BenchmarkCompactionSchedulerStats' COMPACTION_SCHEDULER_STATS.md
