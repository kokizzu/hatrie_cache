#!/bin/sh
set -eu

go test ./hat/hatSql -run '^TestTypedTableAggregate(MaintainsExactMinMaxAcrossUpdatesAndDeletes|ArrangementsDoNotShareDifferentMinMaxDefinitions)$' -count=1
