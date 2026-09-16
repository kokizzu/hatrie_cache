#!/bin/sh
set -eu
go test ./hat/hatSql -run '^TestCHG02' -count=1
