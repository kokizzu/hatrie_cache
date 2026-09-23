#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^Benchmark(C212JoinIndexNumeric|C213SQLHashJoin)$' -benchmem -count=5
