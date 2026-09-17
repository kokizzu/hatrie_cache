#!/bin/sh
set -eu

go test -race ./hat/hatSql -run '^TestCHU40' -count=1
