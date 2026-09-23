#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkCHG04HashIndexNumeric(MissHeavy|HitHeavy)' -benchmem -count=5
