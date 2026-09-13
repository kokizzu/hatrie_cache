#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkMZ018SourceFrontier' -benchmem -count=5
