#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestExecuteSQLQuery|^TestExecuteSQLQueryRowsRejectsStaleSourceFrontier' -count=1
