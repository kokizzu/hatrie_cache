#!/bin/sh
set -eu

go test -race ./hat/hatSql -run '^TestQueryTraceRecorderExportsOpenTelemetrySpans$' -count=1
