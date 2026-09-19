#!/usr/bin/env bash
set -euo pipefail

go test -tags=mu45 ./hat/hatSql
go test -race -tags=mu45 ./hat/hatSql
go vet -tags=mu45 ./hat/hatSql
