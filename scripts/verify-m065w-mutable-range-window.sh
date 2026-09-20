#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestMutableIncrementalRangeWindow'
go test -race ./hat/hatSql -run '^TestMutableIncrementalRangeWindow'
go vet ./hat/hatSql
