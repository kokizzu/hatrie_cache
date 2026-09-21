#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure -run '^$' -bench '^BenchmarkTT021(ExistingRTreeUpdateQuery|PackedRebuildUpdateQuery|LinearUpdateQuery)$' -benchmem -count=5
