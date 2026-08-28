#!/bin/sh
set -eu

go test ./hat/hatCache -run '^(TestQuerySQLTimeSeriesPrunesConfiguredTimePartitions|TestConfigureSQLTimePartitionsRejectsOverlappingRanges)$' -count=1
