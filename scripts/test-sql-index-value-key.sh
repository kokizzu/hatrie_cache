#!/usr/bin/env sh
set -eu

go test ./hat/hatCache -run '^TestSQLIndex(Float|Integer)ValueKeyMatchesJSONEncoding$' -count=1
