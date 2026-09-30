#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestRound16ColumnarLimitWithTies$' -count=1
