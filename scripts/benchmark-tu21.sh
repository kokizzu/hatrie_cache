#!/usr/bin/env bash
set -euo pipefail

pattern=${1:-'^BenchmarkTU21'}
go test ./hat/hatSchema -run '^$' -bench "$pattern" -benchmem -count=3 -timeout=120s
