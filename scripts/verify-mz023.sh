#!/bin/sh
set -eu
go test ./hat/hatSql -count=1
go test -race ./hat/hatSql -count=1
go vet ./hat/hatSql
