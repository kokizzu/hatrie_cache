#!/usr/bin/env sh
set -eu

go test ./hat/hatCache -run '^TestSQLTypedInt64IndexAcceleratesEqualityRangeAndOrder$' -count=1
