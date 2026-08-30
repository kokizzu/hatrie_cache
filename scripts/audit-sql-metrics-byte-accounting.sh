#!/bin/sh
set -eu

rg -n -C 3 'recordBytes\(|sqlExecRowsBytes\(|sqlGroupedRowsBytes\(' hat/hatSql/query.go
rg -n 'func \(.*\) recordBytes|func \(.*\) recordScanRows|type sqlExecutionMetrics' hat/hatSql/query.go
sed -n '6595,6675p' hat/hatSql/query.go
rg -n '^func ExecuteSQL|^func ParseSQL' hat/hatSql/query.go
