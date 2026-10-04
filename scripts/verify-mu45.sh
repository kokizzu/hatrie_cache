#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run 'TestTypedTableJoinArrangementsAcquireBest|TestTypedTableJoinArrangement' -count=1
go vet ./hat/hatSql
