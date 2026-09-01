#!/bin/sh
set -eu

go test ./hat/hatSql -run 'Temporal' -count=1
go test -race ./hat/hatSql
