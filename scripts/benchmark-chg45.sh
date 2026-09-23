#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^Benchmark(CostedExplainMZ044(Regular|Costed)|CH045Explain(Cost|Estimate))$' -benchmem -benchtime=100ms -count=5
