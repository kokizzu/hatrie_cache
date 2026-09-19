#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench 'BenchmarkStoredProcedure' -benchmem -count=5
