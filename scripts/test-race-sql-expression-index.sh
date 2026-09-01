#!/bin/sh
set -eu

go test -race ./hat/hatCache -run '^TestSQLJSONLowerIndex' -count=1
