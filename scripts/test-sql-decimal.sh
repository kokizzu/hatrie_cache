#!/bin/sh
set -eu

go test ./hat/hatSql -run '^TestSQLDecimal'
go test ./hat/hatSchema -run '^TestSQLDecimalSchema'
