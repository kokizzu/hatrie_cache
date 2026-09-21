#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCommand ./hat/hatCache -run '^$' -bench '^BenchmarkTT035WithRequestTimeout' -benchmem -benchtime=1s -count=5
go test ./hat/hatCache -run '^$' -bench '^BenchmarkMonitoringCommandHTTP$' -benchmem -benchtime=300ms -count=3
