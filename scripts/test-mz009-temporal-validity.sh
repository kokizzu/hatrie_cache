#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestSQLValidAt' -count=1
