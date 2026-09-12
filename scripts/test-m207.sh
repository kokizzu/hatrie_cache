#!/bin/sh
set -eu

go test ./hat/hatSql -run '^TestDebeziumChangefeed' -count=1
