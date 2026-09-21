#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatAudit -run '^$' -bench '^(BenchmarkAuditLoggerDefaultLog|BenchmarkTT037AuditLoggerRedactedLog)$' -benchmem -benchtime=500ms -count=5
