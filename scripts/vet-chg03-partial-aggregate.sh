#!/usr/bin/env sh
set -eu

printf '%s\n' 'running CH-G03 partial aggregate state vet'
go vet ./hat/hatSql
