#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestSQLRowBinaryParallelDecode' -count=1
