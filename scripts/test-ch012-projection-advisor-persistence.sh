#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestSQLProjectionAdvisor' -count=1
