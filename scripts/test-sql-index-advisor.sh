#!/bin/sh
set -eu

go test ./hat/hatSql -run '^TestSQLIndexAdvisor' -count=1
