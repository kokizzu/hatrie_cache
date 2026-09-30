#!/bin/sh
set -eu

cd /tmp/hatrie-cache-m233
go test ./hat/hatSql
