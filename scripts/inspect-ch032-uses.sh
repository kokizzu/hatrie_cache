#!/bin/sh
set -eu

rg -n 'SQLRowBinary(Type|Column|Int64|Uint64|Float64|Bool|String|Bytes|Date|DateTime|Duration|UUID|JSON|IPv4|IPv6|Enum8|Enum16|Decimal128|Decimal256)' hat/hatSql hat/hatSchema
