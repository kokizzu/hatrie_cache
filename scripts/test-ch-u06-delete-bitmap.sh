#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestSQLColumnarDeleteBitmap' -count=1 "$@"
