#!/usr/bin/env bash
set -euo pipefail

go test -tags mu41 ./hat/hatSql
go test -race -tags mu41 ./hat/hatSql
go vet -tags mu41 ./hat/hatSql
