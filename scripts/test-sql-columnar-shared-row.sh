#!/bin/sh
set -eu

go test ./hat/hatSql -run '^TestSQLColumnarSourceExecRow$'
go test ./hat/hatCache -run '^TestSQLColumnarScanFiltersBeforeProjectionMaterialization$'
