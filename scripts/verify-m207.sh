#!/bin/sh
set -eu

go test ./hat/hatSql -count=1
go test ./hat/hatSql -race -run '^TestDebeziumChangefeed' -count=1
go vet ./hat/hatSql
