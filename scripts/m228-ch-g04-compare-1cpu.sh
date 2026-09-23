#!/usr/bin/env bash
set -euo pipefail

GOMAXPROCS=1 go test -cpu=1 ./hat/hatSql -run '^$' -bench '^Benchmark(C212JoinIndexNumeric|C213SQLHashJoin)$' -benchmem -count=5
