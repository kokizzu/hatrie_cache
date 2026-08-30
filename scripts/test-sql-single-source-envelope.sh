#!/bin/sh
set -eu

go test ./hat/hatSql -run '^TestMaterializedSingleSourceEnvelopeRetainsFieldsMetricsAndJoinData$' -count=1
