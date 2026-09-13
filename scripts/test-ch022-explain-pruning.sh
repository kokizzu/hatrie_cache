#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestCH022ExplainAnalyzeIncludesStructuredPruningTelemetry$' -count=1
