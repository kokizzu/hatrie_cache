#!/bin/sh
set -eu

go test ./hat/hatCache -run '^TestSQLJSON(Multikey|Array)' -count=1
