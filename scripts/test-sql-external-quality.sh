#!/usr/bin/env sh
set -eu

go test ./hat/hatSql -run '^TestCopyCSVValidatesQuarantinesAndAddsProvenance$' -count=1
