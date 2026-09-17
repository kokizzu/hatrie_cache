#!/bin/sh
set -eu
go test ./hat/hatSql -run 'TestSQLProjectionAdvisor' -count=1
