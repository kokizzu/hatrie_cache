#!/bin/sh
set -eu

go test ./hat/hatCache -run '^TestSQLGroupSkewLimitRejectsDominant' -count=1
