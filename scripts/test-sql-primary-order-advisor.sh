#!/bin/sh
set -eu
go test ./hat/hatSql -run '^TestSQLIndexAdvisorPrimaryOrderRecommendations$' -count=1
