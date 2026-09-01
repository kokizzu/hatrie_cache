#!/bin/sh
set -eu

sh ./scripts/test-sql-borrowed-indexed-join.sh
go test -race ./hat/hatSql ./hat/hatCache
