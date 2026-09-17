#!/bin/sh
set -eu
go test ./hat/hatSql -run '^TestMZ030' -count=1
