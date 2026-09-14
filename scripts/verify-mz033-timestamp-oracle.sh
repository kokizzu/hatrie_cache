#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatReplication
go test -race ./hat/hatReplication
go vet ./hat/hatReplication
test -f hat/hatReplication/global_timestamp_oracle.go
test -f MZ033_TIMESTAMP_ORACLE.md
git diff --check
