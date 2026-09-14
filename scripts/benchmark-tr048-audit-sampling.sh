#!/usr/bin/env bash
set -euo pipefail

GOMAXPROCS=1 go test -count=5 ./hat/hatAudit -run '^$' -bench '^BenchmarkAuditLogger(DefaultLog|SampledLog|SinkLog)$' -benchmem
