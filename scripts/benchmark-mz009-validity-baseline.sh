#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench 'BenchmarkSQLTemporalValidity(Baseline|Function)$' -benchtime=200ms -count=5
