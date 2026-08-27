#!/bin/sh
set -eu

go test ./hat/hatSql -run '^TestSQLTimeZone'
