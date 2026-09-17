#!/bin/sh
set -eu

go test -race ./hat/hatSql -run 'Test(CHU25|SQLSpillQuota)' -count=1
