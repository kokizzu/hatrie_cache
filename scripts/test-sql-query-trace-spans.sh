#!/bin/sh
set -eu

go test ./hat/hatSql -run '^TestQueryTraceRecorderExportsOpenTelemetrySpans$' -count=1
