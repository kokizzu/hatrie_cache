#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure -run '^$' -bench '^BenchmarkTT021(PackedRTreeQueryInto|ExistingRTreeUpdateQuery|PackedRebuildUpdateQuery|LinearUpdateQuery|MutablePackedRTreeUpdateQuery|MutablePackedRTreeReadOnlyQuery|MutablePackedRTreeUpdateCompact)$' -benchmem -count=5
