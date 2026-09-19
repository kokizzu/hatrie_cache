#!/usr/bin/env bash
set -euo pipefail

go test -tags mu11 ./hat/hatSql -count=1
go test -race -tags mu11 ./hat/hatSql -count=1
go vet -tags mu11 ./hat/hatSql
