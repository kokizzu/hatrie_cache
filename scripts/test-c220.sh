#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestSQLQualify' -count=1
