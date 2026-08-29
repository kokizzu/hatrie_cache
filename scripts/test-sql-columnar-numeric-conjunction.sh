#!/usr/bin/env sh
set -eu

go test ./hat/hatCache -run '^TestSQLColumnarScanUsesNumericVectorConjunction$' -count=1
