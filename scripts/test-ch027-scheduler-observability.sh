#!/bin/sh
set -eu

go test ./hat/hatStorage -run 'TestCompactionSchedulerStatsReportsQueueAndRunningAge' -count=1
