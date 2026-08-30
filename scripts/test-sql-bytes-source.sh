#!/bin/sh
set -eu

go test ./hat/hatCache -run '^TestSQLJSONByteSource' -count=1
