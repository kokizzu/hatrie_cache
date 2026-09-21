#!/usr/bin/env bash
set -euo pipefail
test -f M212_LOGICAL_COMPACTION.md
rg -q '^# M212 Logical MVCC Compaction$' M212_LOGICAL_COMPACTION.md
rg -q 'AdvanceMVCCCompactionThrough' M212_LOGICAL_COMPACTION.md
rg -q 'M212: Logical MVCC Compaction' ADOPTED_QUERY_ENGINE_IDEAS.md
rg -q 'M212 Logical MVCC Compaction' BENCHMARK.md
rg -q '^\- \[x\] M212 ' INSPIRATION_ROUND2.md
rg -q 'func \(table \*TypedTable\) AdvanceMVCCCompactionThrough' hat/hatSql/typed_table_mvcc.go
rg -q 'TestM212LogicalCompactionAdvancesFrontierWithoutRewritingChains' hat/hatSql/m212_logical_compaction_test.go
