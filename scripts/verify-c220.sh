#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -count=1
go test ./hat/hatSql -race -count=1
go vet ./hat/hatSql
