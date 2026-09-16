#!/bin/sh
set -eu

go test ./hat/hatCache -run '^TestMonitoringAsyncCommandWaitForAsyncInsert' -count=1
