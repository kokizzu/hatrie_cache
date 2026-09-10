#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run '^TestBeginSQLFrontierSnapshot' -count=10
