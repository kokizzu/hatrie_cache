#!/bin/sh
set -eu

sed -n '1,220p' hat/hatSql/row_binary_dictionary.go
