#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run 'TestGroupAverageInt64DifferentialRows|ExampleGroupAverageInt64DifferentialRows' -count=1
go test ./hat/hatSql -run 'Test(GroupCount|GroupSum|GroupMinMax|GroupAverage).*Differential' -count=1
go test ./hat/hatSql -count=1
go test -race ./hat/hatSql -run 'TestGroupAverageInt64DifferentialRows' -count=1
go vet ./hat/hatSql
