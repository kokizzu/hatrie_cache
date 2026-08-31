#!/bin/sh
set -eu

go test ./hat/hatCache -run '^TestSQLProjectionRetentionFrontier' -count=1
