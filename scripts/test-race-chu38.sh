#!/bin/sh
set -eu

go test -race ./hat/hatSql -run 'TestSQLQueryLog(ProbabilisticSampling|SamplingDefaults)' -count=1
