#!/bin/sh
set -eu

go test ./hat/hatCache -run '^TestSQLColumnarDictionaryLiteralINUsesNumericConjunction$' -count=1
