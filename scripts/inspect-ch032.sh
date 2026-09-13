#!/bin/sh
set -eu

section=${SECTION:-summary}
case "$section" in
summary)
	rg -n 'type (SQLDecimal|sqlDecimal)|SQLDecimal|sqlDecimal|Decimal128|Decimal256' hat/hatSql hat/hatSchema --glob '*.go'
	rg -n 'SQLRowBinaryString|appendSQLRowBinaryValue|decodeSQLRowBinaryValue|inferSQLRowBinaryStream|modelGoType' hat/hatSql/row_binary.go hat/hatSql/row_binary_stream.go hat/hatSchema/generate.go
	;;
decimal)
	rg -n 'type (SQLDecimal|sqlDecimal)|SQLDecimal|sqlDecimal|ParseSQLDecimal|Decimal' hat/hatSql --glob '*.go'
	;;
decimal-code)
	sed -n '380,535p' hat/hatSql/query.go
	sed -n '17110,17160p' hat/hatSql/query.go
	;;
row)
	sed -n '1,560p' hat/hatSql/row_binary.go
	;;
stream)
	sed -n '430,735p' hat/hatSql/row_binary_stream.go
	;;
schema)
	sed -n '1,180p' hat/hatSchema/schema.go
	sed -n '75,150p' hat/hatSchema/generate.go
	;;
*)
	printf 'unknown SECTION=%s\n' "$section" >&2
	exit 2
	;;
esac
