#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench 'BenchmarkQueryTraceRecorder(Events|OpenTelemetrySpans|ExportOpenTelemetry)$' -benchmem -count=5
