#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestSQLProjectionAdvisorSnapshotSize$' -v -count=1
