#!/usr/bin/env sh
set -eu

sed -n '1640,1815p' hat/hatCache/sql_query.go
