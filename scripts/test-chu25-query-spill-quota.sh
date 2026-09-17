#!/bin/sh
set -eu

go test ./hat/hatSql -run 'Test(CHU25|SQLSpillQuota)' -count=1
