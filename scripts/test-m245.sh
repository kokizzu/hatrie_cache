#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^(TestM245|TestSQLTelemetry)' -count=1
