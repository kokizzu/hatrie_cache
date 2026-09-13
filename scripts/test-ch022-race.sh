#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run '^TestCH022ExplainAnalyzeIncludesStructuredPruningTelemetry$' -count=1
