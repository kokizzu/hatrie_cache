#!/bin/sh
set -eu

sh ./scripts/test-sql-typed-minmax.sh
go test -race ./hat/hatSql
