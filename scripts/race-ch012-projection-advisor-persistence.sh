#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run '^TestSQLProjectionAdvisor' -count=1
