#!/usr/bin/env bash
set -euo pipefail

go test -tags mu12 ./hat/hatSql -count=1
go test -race -tags mu12 ./hat/hatSql -count=1
go vet -tags mu12 ./hat/hatSql
