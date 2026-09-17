#!/usr/bin/env bash
set -euo pipefail

exec go test -race ./hat/hatSql -run '^TestSQLClusterAdmission' -count=1
