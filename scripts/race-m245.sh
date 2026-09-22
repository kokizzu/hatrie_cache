#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run '^(TestM245|TestSQLTelemetry)' -count=1
