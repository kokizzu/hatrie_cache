#!/usr/bin/env sh
set -eu

go test ./hat/hatCache -run '^TestSQLDirectStringSourcesRefreshAfterReplacement$' -count=1
