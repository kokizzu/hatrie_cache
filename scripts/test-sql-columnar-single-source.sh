#!/bin/sh
set -eu

go test ./hat/hatSql -run '^TestSQLSingleSourceExecRow$'
go test ./hat/hatCache -run '^TestSQLColumnarScanFiltersBeforeProjectionMaterialization$'
