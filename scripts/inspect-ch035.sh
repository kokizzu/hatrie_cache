#!/bin/sh
set -eu

section=${SECTION:-summary}
case "$section" in
summary)
	rg -n 'type SQLRowBinaryColumn|SQLRowBinary[A-Za-z0-9]+ SQLRowBinaryType|func (Encode|Decode)SQLRowBinary|validSQLRowBinaryType|appendSQLRowBinaryValue|decodeSQLRowBinaryValue' hat/hatSql/row_binary.go
	rg -n 'func (Encode|Decode)SQLRowBinary|decodeSQLRowBinaryBitmapValue|sqlRowBinaryNullableBitmapLayout' hat/hatSql/row_binary_nullable_bitmap.go
	rg -n 'func (Encode|Decode)SQLRowBinary|readSQLRowBinaryStreamValue|inferSQLRowBinaryStream|appendSQLRowBinaryStreamRow|normalizeSQLRowBinaryStreamValue|sqlRowBinaryStreamFixedWidth' hat/hatSql/row_binary_stream.go
	rg -n 'func (Encode|Decode)SQLRowBinary|sqlRowBinaryDeltaType|sqlRowBinaryDeltaValue|sqlRowBinaryDeltaDecodedValue|appendSQLRowBinaryDeltaValue|decodeSQLRowBinaryDeltaValue' hat/hatSql/row_binary_delta_codec.go
	rg -n 'type Column|type Schema|type Source|GenerateGoModels|SQLRowBinary|SchemaFingerprint' hat/hatSchema --glob '*.go'
	;;
row)
	sed -n '1,560p' hat/hatSql/row_binary.go
	;;
nullable)
	sed -n '1,250p' hat/hatSql/row_binary_nullable_bitmap.go
	;;
stream)
	sed -n '340,540p' hat/hatSql/row_binary_stream.go
	;;
stream-normalize)
	sed -n '515,680p' hat/hatSql/row_binary_stream.go
	;;
stream-header)
	sed -n '1,340p' hat/hatSql/row_binary_stream.go
	;;
delta)
	sed -n '1,390p' hat/hatSql/row_binary_delta_codec.go
	;;
stats)
	sed -n '1,540p' hat/hatSql/row_binary_stats.go
	;;
read-stats)
	sed -n '1,220p' hat/hatSql/row_binary_read_stats.go
	;;
stats-pruning)
	sed -n '1,130p' hat/hatSql/row_binary_stats_pruning.go
	;;
schema)
	sed -n '1,220p' hat/hatSchema/schema.go
	sed -n '1,260p' hat/hatSchema/generate.go
	sed -n '1,180p' hat/hatSchema/compatibility.go
	;;
schema-validation)
	sed -n '220,520p' hat/hatSchema/schema.go
	;;
fingerprint)
	sed -n '1,220p' hat/hatSchema/fingerprint.go
	;;
docs)
	rg -n 'SQL_IP_TYPES|RowBinary|BENCHMARK|INSPIRATION' README.md BENCHMARK.md INSPIRATION_BACKLOG.md
	;;
*)
	printf 'unknown SECTION=%s\n' "$section" >&2
	exit 2
	;;
esac
