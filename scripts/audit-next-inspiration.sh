#!/usr/bin/env bash
set -u

printf '%s\n' 'Candidate backlog rows:'
rg -n '^\| (CH-32|CH-35|MZ-37|MZ-39|TR-14|TR-17|TR-24|TR-30|TR-43) \|' INSPIRATION_BACKLOG.md
printf '%s\n' 'Exact enum/decimal implementations:'
rg -l -i 'enum8|enum16|decimal128|decimal256' hat/hatSchema hat/hatSql hat/hatCache --glob '*.go'
printf '%s\n' 'Existing index-statistics/covering implementations:'
rg -l -i 'selectivity|frequency.?histogram|covering.?index|index.?stats' hat/hatSql hat/hatCache --glob '*.go'
printf '%s\n' 'Existing batching/yield implementations:'
rg -l -i 'worker.?local|exchange|yield|fuel|operator.?budget' hat/hatSql --glob '*.go'
printf '%s\n' 'Existing run-filter implementations:'
rg -l -i 'run.?filter|bloom' hat/hatStorage hat/hatCache --glob '*.go'
printf '%s\n' 'Enum codec call sites:'
rg -l 'appendSQLRowBinaryValue|validateSQLRowBinaryColumns|SQLRowBinaryColumn' hat/hatSql --glob '*.go'
printf '%s\n' 'Schema metadata consumers:'
rg -l 'Column\{|\.Columns|SchemaFingerprint|json:"columns"' hat/hatSchema hat/hatCache --glob '*.go'
printf '%s\n' 'Adaptive/fixed-width codec boundaries:'
rg -n 'EncodeSQLRowBinary|Delta|DoubleDelta|validSQLRowBinaryType' hat/hatSql/row_binary_adaptive_codec.go hat/hatSql/row_binary.go hat/hatSql/row_binary_delta_codec.go
printf '%s\n' 'RowBinary type and value boundaries:'
rg -n 'type SQLRowBinaryColumn|SQLRowBinary[A-Za-z0-9]+ =|func .*SQLRowBinary|validSQLRowBinaryType|appendSQLRowBinaryValue|decodeSQLRowBinaryValue' hat/hatSql --glob '*.go'
printf '%s\n' 'Schema type and generation boundaries:'
rg -n 'type Column|SQLType|SchemaFingerprint|Generate.*Model|ColumnType' hat/hatSchema --glob '*.go'
exit 0
