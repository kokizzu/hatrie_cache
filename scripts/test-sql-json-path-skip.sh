#!/bin/sh
set -eu

go test ./hat/hatCache -run '^TestSQLJSONPathSkip' -count=1
