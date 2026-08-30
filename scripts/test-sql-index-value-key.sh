#!/usr/bin/env sh
set -eu

go test ./hat/hatCache -run '^TestSQLIndexFloatValueKeyMatchesJSONEncoding$' -count=1
