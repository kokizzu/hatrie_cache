#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkTypedTableSortedArrangement(LegacyBuild|DictionaryBuild|SingleOrderBuild|CompositeOrderBuild)$' -benchmem -count=5
