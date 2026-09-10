#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestSQLSourceFrontierBarrier' -count=1
