#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run 'TestTypedTableJoinArrangementsAcquireBest|TestTypedTableJoinArrangement' -count=1
go test ./hat/hatSql -run 'TestTypedTableJoin' -count=1
