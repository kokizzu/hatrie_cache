#!/usr/bin/env sh
set -eu

go test ./hat/hatSql -run '^TestGeoIndex' -count=1
