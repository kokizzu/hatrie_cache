#!/usr/bin/env bash
set -euo pipefail

test -f M244_COMPACTION_DEBT.md
rg -q 'CompactionDebt|SourceSequence|CompactedThrough' M244_COMPACTION_DEBT.md
rg -q 'M244_COMPACTION_DEBT.md' README.md INSPIRATION_ROUND2.md ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md
rg -q 'M244 Compaction Debt At The Logical Frontier' BENCHMARK.md
