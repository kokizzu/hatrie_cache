#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestSQLTableSample' -count=1
