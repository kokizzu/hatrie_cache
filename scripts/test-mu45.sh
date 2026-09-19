#!/usr/bin/env bash
set -euo pipefail

go test -tags=mu45 ./hat/hatSql -run '^TestSelectTypedTableJoinArrangement'
