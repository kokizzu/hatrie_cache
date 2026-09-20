#!/usr/bin/env bash
set -euo pipefail
go test ./hat/hatSql -run '^TestMutableIncrementalRangeNthValueWindow'
go test -race ./hat/hatSql -run '^TestMutableIncrementalRangeNthValueWindow'
go vet ./hat/hatSql
