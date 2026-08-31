#!/bin/sh
set -eu

go test ./hat/hatCache -run '^TestSQLColumnarScanUsesDictionaryLiteralIN$' -count=1
