#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkMZ035RebuildMultisetBaseline$' -benchmem -count=5
