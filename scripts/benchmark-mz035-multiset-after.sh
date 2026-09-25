#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkMZ035(IncrementalMultisetApply|RebuildMultisetBaseline)$' -benchmem -count=5
