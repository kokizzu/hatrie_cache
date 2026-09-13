#!/bin/sh
set -eu

go test ./hat/hatSql -run '^TestSQLRowBinaryEnum'
go test ./hat/hatSchema -run '^TestEnumSchema'
