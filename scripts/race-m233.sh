#!/bin/sh
set -eu

cd /tmp/hatrie-cache-m233
go test -race -run '^TestM233' -count=1 ./hat/hatSql
