#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql
go test -race ./hat/hatSql
go vet ./hat/hatSql
