#!/bin/sh
set -eu

sed -n '150,210p' hat/hatSql/decimal_types_test.go
