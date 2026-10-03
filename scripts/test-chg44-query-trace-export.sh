#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestQueryTraceRecorder' -count=1
