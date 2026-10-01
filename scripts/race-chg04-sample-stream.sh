#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run '^TestSQLTableSample' -count=1
