#!/bin/sh
set -eu

go test ./hat/hatSql -run '^TestCHG01' -count=1
