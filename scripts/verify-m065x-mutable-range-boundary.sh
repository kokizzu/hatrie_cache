#!/usr/bin/env bash
set -euo pipefail
go test ./hat/hatSql -run '^TestMutableIncrementalRangeBoundaryWindow'
go test -race ./hat/hatSql -run '^TestMutableIncrementalRangeBoundaryWindow'
go vet ./hat/hatSql
