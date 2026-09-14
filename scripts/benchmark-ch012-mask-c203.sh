#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkCH012DeleteMaskBacking$' -benchmem -count=5
