#!/usr/bin/env sh
set -eu

go test ./hat/hatSql -run 'Approx' -count=1
