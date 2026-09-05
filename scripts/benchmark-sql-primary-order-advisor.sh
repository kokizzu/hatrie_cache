#!/bin/sh
set -eu
go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLIndexAdvisor(PrimaryOrderRecommendations|PerFieldRecommendations)$' -benchmem -count=5
