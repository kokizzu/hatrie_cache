#!/bin/sh
set -eu

go test ./hat/hatSql -run '^TestCHU40' -count=1
