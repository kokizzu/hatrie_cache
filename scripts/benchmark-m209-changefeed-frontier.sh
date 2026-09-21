#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatReplication -run '^$' -bench '^BenchmarkM209ChangefeedFrontierAdvance(Baseline)?$' -benchmem -benchtime=2s -count=5
