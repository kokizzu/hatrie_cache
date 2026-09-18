#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkM032(IndependentFrontierBaseline|DistributedFrontier|DistributedFrontierConstruction)$' -benchmem -benchtime=1s -count=5
