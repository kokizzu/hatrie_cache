#!/usr/bin/env bash
set -euo pipefail
go test ./hat/hatSql -run '^$' -bench '^BenchmarkMU015Catalog(SourceStatus|SourcesBaseline)$' -benchmem -count=5
