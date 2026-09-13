#!/bin/sh
set -eu

sed -n '1,380p' hat/hatSql/row_binary_stream.go
