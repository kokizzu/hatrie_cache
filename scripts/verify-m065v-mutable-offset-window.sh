#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestMutableIncrementalOffsetWindow'
go test -race ./hat/hatSql -run '^TestMutableIncrementalOffsetWindow'
go vet ./hat/hatSql
