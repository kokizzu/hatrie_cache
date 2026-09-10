#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatReplication -run '^$' -bench '^(BenchmarkTimestampOracleNext|BenchmarkGlobalTimestampOracle)$' -benchmem -benchtime=250ms -count=5
