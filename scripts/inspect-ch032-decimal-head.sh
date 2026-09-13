#!/bin/sh
set -eu

sed -n '1,120p' hat/hatSql/decimal_types.go
