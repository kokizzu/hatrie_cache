#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql
go test -race ./hat/hatSql -run '^TestM052AH'
go vet ./hat/hatSql
