#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run '^TestCompiledSQLQuery' -count=1
