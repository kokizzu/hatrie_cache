#!/bin/sh
set -eu

go test ./hat/hatCache -run '^TestSQLJSONPartialIndexRestrictsAndRefreshes$' -count=1
