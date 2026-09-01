#!/bin/sh
set -eu

go test ./hat/hatCache -run '^TestSQLJSONLowerIndex' -count=1
