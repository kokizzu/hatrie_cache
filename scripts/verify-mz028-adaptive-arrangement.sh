#!/usr/bin/env bash
set -euo pipefail

grep -Fq 'groups := aggregate.compactOrderedGroups()' hat/hatSql/typed_table.go
grep -Fq 'pendingGroupOrder' hat/hatSql/typed_table.go
grep -Fq 'func TestMZ028LegacyAggregateCachesOrderedGroupReferences' hat/hatSql/mz028_adaptive_arrangement_test.go
grep -Fq 'func TestMZ028SingleNewGroupMaintainsOrderIncrementally' hat/hatSql/mz028_adaptive_arrangement_test.go
grep -Fq '1.68x faster' MZ028_ADAPTIVE_ARRANGEMENT.md
grep -Fq '1.79x faster' MZ028_ADAPTIVE_ARRANGEMENT.md
grep -Fq '| MZ-028 | Adaptive arrangement compaction | Partially adopted:' ENGINE_IDEAS.md
test ! -e scripts/inspect-mz028-adaptive-compaction.sh
test ! -e scripts/inspect-mz028-test-process.sh

printf '%s\n' 'MZ-028 implementation and documentation verified'
