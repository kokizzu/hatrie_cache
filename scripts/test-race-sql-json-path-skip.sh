#!/bin/sh
set -eu

go test -race ./hat/hatCache -run '^TestSQLJSONPathSkip' -count=1
