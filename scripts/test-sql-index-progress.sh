#!/bin/sh
set -eu

go test ./hat/hatCache -run '^TestSQLJSONIndexRebuildProgress' -count=1
