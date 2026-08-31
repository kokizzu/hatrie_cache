#!/bin/sh
set -eu

go test ./hat/hatCache -run '^TestSQLColumnarTopNUsesDictionaryLiteralIN$' -count=1
