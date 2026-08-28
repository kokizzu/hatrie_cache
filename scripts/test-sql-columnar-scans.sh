#!/bin/sh
set -eu

go test ./hat/hatCache -run '^TestSQLColumnarScan' -count=1
