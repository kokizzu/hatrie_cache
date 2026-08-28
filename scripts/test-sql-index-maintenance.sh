#!/bin/sh
set -eu

go test ./hat/hatCache -run '^TestSQLJSONIndexMaintenance' -count=1
