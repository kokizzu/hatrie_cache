#!/usr/bin/env bash
set -eu

go test ./hat/hatSql -run '^TestSQL(IP|RowBinaryIP)'
