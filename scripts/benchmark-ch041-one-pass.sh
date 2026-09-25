#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkCH041GroupingSetQuery(OnePass|ExpandedUnionAll)$' -benchmem -count=5
