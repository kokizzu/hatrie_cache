#!/bin/sh
set -eu

go test ./hat/hatSql -run 'TestSQLQueryLog(ProbabilisticSampling|SamplingDefaults)' -count=1
