#!/bin/sh
set -eu
go test -race ./hat/hatSql -run 'TestSQLProjectionAdvisor' -count=1 -v
